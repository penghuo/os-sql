# Scalar AVG Analysis: Q04 (SELECT AVG(UserID) FROM hits) — 16.1x slower than ClickHouse

## 1. Does Q04 hit FusedScanAggregate? What does canFuse() check?

**YES, Q04 hits FusedScanAggregate.execute().**

`canFuse()` (line 78-93) checks:
1. No GROUP BY keys (empty groupByKeys) → Q04 passes ✓
2. Child is `TableScanNode` (not EvalNode etc.) → Q04 passes ✓
3. All aggregate functions match regex `^(COUNT|SUM|MIN|MAX|AVG)\((DISTINCT\s+)?(.+?)\)$` → `AVG(UserID)` passes ✓

Dispatch in `TransportShardExecuteAction.java:238`:
```java
if (scanFactory == null && effectivePlan instanceof AggregationNode aggNode
    && FusedScanAggregate.canFuse(aggNode)) {
    List<Page> pages = executeFusedScanAggregate(aggNode, req);
```

## 2. Does Q04 use tryFlatArrayPath?

**YES, Q04 should use tryFlatArrayPath.**

`tryFlatArrayPath` eligibility (line 766+):
- Not DISTINCT → ✓
- argType is BigintType (not DoubleType/VarcharType) → ✓
- No deleted docs (ClickBench data is append-only) → ✓ (likely)

The flat array path uses **column-major sequential iteration** with `nextDoc()` instead of `advanceExact(doc)` random access. For Q04 with a single column (UserID), it reads one column's SortedNumericDocValues sequentially.

**Inner loop (no MIN/MAX needed, so the else branch):**
```java
int doc = dv.nextDoc();
while (doc != DocIdSetIterator.NO_MORE_DOCS) {
    localSum += dv.nextValue();
    localCount++;
    doc = dv.nextDoc();
}
```

**Parallelization:** Segments are distributed across workers. `numWorkers = min(availableProcessors / numLocalShards(default=4), numSegments)`. Uses `ForkJoinPool` with `CompletableFuture.supplyAsync()`.

## 3. Execution mechanism for scalar AVG

1. Parse `AVG(UserID)` → `AggSpec("AVG", false, "UserID", BigintType.BIGINT)`
2. Create `AvgDirectAccumulator` (line 1342+) — tracks `doubleSum` (double) and `count` (long)
3. `tryFlatArrayPath` accumulates `colSum[0]` (long) and `colCount[0]` (long) across all segments
4. Results mapped back: `AvgDirectAccumulator.addLongSumCount(colSum[0], colCount[0])` → converts long sum to double
5. `writeTo()` outputs `doubleSum / count`

## 4. Bottlenecks & Missing Optimizations

### Bottleneck 1: SortedNumericDocValues overhead (MAJOR)
The inner loop uses `SortedNumericDocValues.nextDoc()` + `nextValue()`. This is Lucene's multi-valued numeric doc values API, which has overhead for handling multi-valued fields even when all docs have exactly one value. For a simple BIGINT column, this involves:
- Variable-length decoding per doc
- Multi-value count check per doc
- Two virtual method calls per doc (`nextDoc()` + `nextValue()`)

**ClickHouse comparison:** ClickHouse-Parquet reads a contiguous `long[]` array from a Parquet column chunk and sums it with SIMD vectorization. No per-value decoding overhead.

### Bottleneck 2: No SIMD / vectorized accumulation
The tight loop `localSum += dv.nextValue()` processes one value at a time. There's no batch reading of doc values into a `long[]` buffer followed by vectorized summation. Java's auto-vectorization cannot optimize this because `nextValue()` is a virtual call with side effects.

**Potential fix:** If Lucene's doc values could be read in bulk (e.g., via a bulk-read API or direct access to the underlying compressed blocks), the sum could be computed with `Vector API` or at least with a tight `long[]` loop that JIT can auto-vectorize.

### Bottleneck 3: Lucene doc values encoding vs Parquet encoding
Lucene's `SortedNumericDocValues` uses a compressed format optimized for random access and updates. Parquet uses columnar encoding (RLE, dictionary, delta) optimized for sequential scans. For a full-table scan of 100M rows, Parquet's encoding is fundamentally more efficient to decode sequentially.

### Bottleneck 4: Limited intra-shard parallelism
`numWorkers = availableProcessors / 4` (default). On a typical machine with 8-16 cores, that's 2-4 workers per shard. ClickHouse uses all cores for a single query. If there are few segments (e.g., 1 large segment after force-merge), parallelism drops to 1 worker.

### Bottleneck 5: CompletableFuture overhead for simple aggregation
For a single-column AVG, the parallel infrastructure (CompletableFuture, ForkJoinPool, long[] packing/unpacking) adds overhead that may not be justified. A single-threaded sequential scan of one column might be faster for small segment counts.

## Summary

The code path is well-optimized relative to what Lucene offers:
- ✅ Fused scan-aggregate (no intermediate Pages)
- ✅ Sequential `nextDoc()` iteration (not random access)
- ✅ Column-major processing
- ✅ Parallel segment processing
- ✅ Long accumulation (no per-doc double conversion)

**The fundamental gap is Lucene's doc values format vs Parquet's columnar format.** Lucene's `SortedNumericDocValues` requires per-doc decoding with virtual dispatch, while Parquet provides bulk-decodable columnar data that enables SIMD summation. This is an inherent storage-layer limitation, not an algorithmic one.

**Actionable recommendations:**
1. **Bulk doc values reading**: Investigate if Lucene's internal doc values format can be accessed in bulk (bypassing the iterator API) for dense columns
2. **Direct codec access**: For known-dense BIGINT columns, read the underlying compressed blocks directly and decode in batch
3. **Segment-level parallelism**: Ensure the index has multiple segments (not force-merged to 1) to enable parallel processing
4. **Consider alternative storage**: For analytics workloads, a Parquet-backed or columnar storage format would close the gap fundamentally
