# 3-Key GROUP BY Analysis: FusedGroupByAggregate.java

## executeThreeKeyFlat Method

**Location**: Line 8402, `FusedGroupByAggregate.java` (12,778 lines total)

**Signature** (lines 8402-8424):
```java
private static List<Page> executeThreeKeyFlat(
    Engine.Searcher engineSearcher, Query query,
    List<KeyInfo> keyInfos, List<AggSpec> specs,
    int numAggs, boolean[] isCountStar, int[] accType,
    String[] truncUnits, String[] arithUnits,
    List<String> groupByKeys, int sortAggIndex,
    boolean sortAscending, long topN,
    Map<String, Type> columnTypeMap,
    int bucket, int numBuckets) throws Exception
```

**Scan loop**: Sequential single-segment doc iteration (lines 8467-8595). Iterates `for (int doc = 0; doc < maxDoc; doc++)` reading 3 key DocValues per doc, inserting into FlatThreeKeyMap. Has MatchAllDocsQuery bypass and liveDocs handling. Supports hash-partition bucketing via `bucket`/`numBuckets` params.

**No parallel execution**: The method processes a single segment sequentially. No ForkJoinPool, no CompletableFuture, no worker threads.

## FlatThreeKeyMap Inner Class

**Location**: Line 11925

**Fields**: `long[] keys0, keys1, keys2; boolean[] occupied; long[] accData; int size`
**MAX_CAPACITY**: 8,000,000 (line 11928)
**Methods**: `hash3()`, `findOrInsert()`, `findExisting()`, `resize()`

**⚠ mergeFrom is ABSENT** — FlatThreeKeyMap does NOT have a `mergeFrom()` method.
- FlatTwoKeyMap HAS `mergeFrom()` at line 11906
- FlatSingleKeyMap HAS `mergeFrom()` at line 12130
- FlatThreeKeyMap is MISSING it — must be added for parallelization.

## 3-Key Dispatch Logic

**Location**: `executeNKeyVarcharPath()` around line 8986-9080

**Dispatch condition** (line 8986-8991):
```java
if (singleSegment) {
    if (keyInfos.size() == 3) {
        // checks: no varchar/double agg args, only COUNT/SUM/AVG, no DISTINCT
        if (canUseFlatThreeKey) { ... }
    }
}
```

**Guard**: `singleSegment = (leaves.size() == 1)` — only fires for single-segment indices.

**Bucketing** (lines 9020-9066): If `totalDocs > MAX_CAPACITY`, splits into hash-partitioned buckets and calls `executeThreeKeyFlat` sequentially per bucket. Results merged via `mergePartitionedPages`.

**Multi-segment fallback**: When `singleSegment` is false, 3-key queries fall through to `executeNKeyVarcharParallelDocRange` (line 9901) — the generic N-key parallel path, which does NOT use FlatThreeKeyMap.

## Q18 Routing

Q18 in this codebase = ClickBench Q18: `GROUP BY UserID, SearchPhrase` — a **2-key** query, not 3-key.
- Routes to `executeWithVarcharKeys` → flat two-key varchar path (line 6922)
- Uses `FlatTwoKeyMap` with parallel `executeMultiSegGlobalOrdFlatTwoKey` for multi-segment

If Q18 were hypothetically a 3-key query, it would route:
1. `executeInternal` → `executeWithVarcharKeys` (has varchar keys)
2. → `executeNKeyVarcharPath` (line 7808)
3. If single-segment + 3 keys + compatible aggs → `executeThreeKeyFlat` (line 9025)
4. If multi-segment → falls through to `executeNKeyVarcharParallelDocRange` (generic, slower)

## TransportShardExecuteAction Routing

**File**: `TransportShardExecuteAction.java` (line 57 import, lines 281-524 dispatch)
- No 3-key-specific routing — all GROUP BY queries go through `executeFusedGroupByAggregate` (line 1530)
- Which calls `FusedGroupByAggregate.execute()` → `executeInternal()` → dispatch by key types

## What Needs to Change for 3-Key Parallelization

1. **Add `mergeFrom(FlatThreeKeyMap other)` to FlatThreeKeyMap** (line ~12000) — pattern from FlatTwoKeyMap:11906
2. **Add `scanSegmentFlatThreeKey()` helper** — extract per-segment scan from executeThreeKeyFlat
3. **Add parallel multi-segment path in executeNKeyVarcharPath** — before the generic fallback at line 9901, add a 3-key parallel path using ForkJoinPool + CompletableFuture (pattern from executeTwoKeyNumericFlat:5350)
4. **Remove `singleSegment` guard** for 3-key — currently line 8986 restricts to single-segment only

### Parallel Pattern Template (from 2-key path, lines 5355-5392):
```java
CompletableFuture<FlatTwoKeyMap>[] futures = new CompletableFuture[numWorkers];
for (int w = 0; w < numWorkers; w++) {
    futures[w] = CompletableFuture.supplyAsync(() -> {
        FlatTwoKeyMap localMap = new FlatTwoKeyMap(slotsPerGroup);
        for (LeafReaderContext leafCtx : mySegments)
            scanSegmentFlatTwoKey(leafCtx, weight, isMatchAll, localMap, ...);
        return localMap;
    }, PARALLEL_POOL);
}
CompletableFuture.allOf(futures).join();
for (var future : futures) {
    FlatTwoKeyMap workerMap = future.join();
    if (workerMap.size > 0) flatMap.mergeFrom(workerMap);
}
```
Replicate this pattern for FlatThreeKeyMap.
