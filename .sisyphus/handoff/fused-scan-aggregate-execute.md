# FusedScanAggregate.execute() Analysis

## Method Signature (line ~310)
```java
public static List<Page> execute(
    AggregationNode aggNode, IndexShard shard, Query query, Map<String, Type> columnTypeMap)
```

## Execution Flow for Q02 (SUM + COUNT + AVG, MatchAll, no deletes)

1. Parses agg functions into `List<AggSpec>` (SUM/AdvEngineID, COUNT/*, AVG/ResolutionWidth)
2. Creates `DirectAccumulator` per agg (SumDirectAccumulator, CountStarDirectAccumulator, AvgDirectAccumulator)
3. Skips COUNT(*)-only fast path (has SUM+AVG too)
4. Skips MIN/MAX PointValues fast path (has SUM+AVG)
5. **Enters `tryFlatArrayPath()`** — the critical path for Q02

## tryFlatArrayPath() — The Hot Path (line ~430)

### Eligibility check:
- No DISTINCT, no DoubleType, no VarcharType columns → passes for Q02
- No deleted docs → passes

### Column-major iteration strategy:
- Collects unique column names: ["AdvEngineID", "ResolutionWidth"] (2 columns)
- Allocates flat arrays: `colSum[2]`, `colCount[2]`, `colMin[2]`, `colMax[2]`

### Parallelism (line ~490):
```
numWorkers = min(availableProcessors / dqe.numLocalShards(default=4), numSegments)
```
- Uses `FusedGroupByAggregate.getParallelPool()` — a shared ForkJoinPool sized to availableProcessors
- Segments partitioned via largest-first greedy assignment

### Inner loop (parallel worker, line ~520):
```java
for (LeafReaderContext leafCtx : mySegments) {
    for (int c = 0; c < nc; c++) {                    // column-major
        SortedNumericDocValues dv = reader.getSortedNumericDocValues(colArray[c]);
        int doc = dv.nextDoc();
        while (doc != NO_MORE_DOCS) {                 // sequential scan
            localSum += dv.nextValue();
            localCount++;
            doc = dv.nextDoc();
        }
        wColSum[c] += localSum; wColCount[c] += localCount;
    }
}
```

### Key observations:
- **Column-major**: reads ALL docs for col0, then ALL docs for col1 (not interleaved)
- **nextDoc()** sequential scan (not advanceExact random access) — faster for dense columns
- **No MIN/MAX branch** taken for Q02 (needMin=false, needMax=false) — tighter loop
- Each column scanned independently = 2 full passes over 100M docs per segment

### Result merge (line ~560):
Worker results packed into `long[1 + nc*4]`, merged into global colSum/colCount arrays, then mapped back to accumulators.

## Thread Pool
```java
// FusedGroupByAggregate.java line ~124
private static final ForkJoinPool PARALLEL_POOL = new ForkJoinPool(
    Runtime.getRuntime().availableProcessors(), ..., asyncMode=true);
```

## Performance Implication for Q02
- 2 columns × 100M rows = 200M docvalue reads per shard
- Column-major means each column's docvalue stream is read contiguously (good cache locality)
- But 2 separate full passes over the segment (could be 1 pass if row-major)
- Parallelism across segments helps, but numWorkers = cpus/4 may be low
