# Q15 Parallel Analysis: executeSingleKeyNumericFlat

## 1. executeSingleKeyNumericFlat — Signature & Key Logic

**Location**: `dqe/src/main/java/org/opensearch/sql/dqe/shard/source/FusedGroupByAggregate.java:5129-5440`

**Signature** (lines 5129-5143):
```java
private static List<Page> executeSingleKeyNumericFlat(
    IndexShard shard, Query query, List<KeyInfo> keyInfos, List<AggSpec> specs,
    Map<String, Type> columnTypeMap, List<String> groupByKeys, int numAggs,
    boolean[] isCountStar, int[] accType, int sortAggIndex, boolean sortAscending,
    long topN, int bucket, int numBuckets) throws Exception
```

**Data structure**: `FlatSingleKeyMap` (inner class at line 13448) — open-addressing hash map with:
- `long[] keys` — group key values (EMPTY_KEY = Long.MIN_VALUE)
- `long[] accData` — contiguous accumulator slots, `slot * slotsPerGroup` indexing
- `mergeFrom()` at line 13542 — element-wise addition of accData

**Processing flow** (3 paths based on `canParallelize`):

### Path A: Doc-range parallel (lines 5176-5241)
- **Condition**: `canParallelize && allCountStar && isMatchAll && numBuckets <= 1`
- Loads key columns into `long[]` arrays per segment via `loadNumericColumn()`
- Splits total docs across `THREADS_PER_SHARD` workers into `(segIndex, startDoc, endDoc)` work units
- Each worker creates pre-sized `FlatSingleKeyMap(slotsPerGroup, initCap)` capped at 4M
- Calls `scanDocRangeFlatSingleKeyCountStar()` (line 14195) — tight loop: `findOrInsert(key) → accData[slot]++`
- Merges via `flatMap.mergeFrom(workerMap)` on main thread

### Path B: Segment-parallel (lines 5244-5307)
- **Condition**: `canParallelize && !(allCountStar && isMatchAll && numBuckets <= 1)`
- Greedy largest-first segment assignment to `numWorkers = min(THREADS_PER_SHARD, leaves.size())`
- Each worker creates pre-sized `FlatSingleKeyMap(slotsPerGroup, initCap)` capped at 4M
- Calls `scanSegmentFlatSingleKey()` (line 5561) per assigned segment
- Merges via `flatMap.mergeFrom(workerMap)` on main thread

### Path C: Sequential (lines 5308-5320)
- Single `FlatSingleKeyMap` shared across all segments
- Iterates leaves sequentially calling `scanSegmentFlatSingleKey()`

### Top-N / ORDER BY (lines 5330-5440)
- If `sortAggIndex >= 0 && topN > 0 && topN < flatMap.size`: uses a min/max heap of size `topN`
- Iterates all slots in flatMap, maintains heap for top-N selection
- Extracts sorted results from heap

## 2. Parallelism Configuration

**Location**: lines 117-128
```java
private static final String PARALLELISM_MODE = System.getProperty("dqe.parallelism", "docrange"); // line 118
private static final int THREADS_PER_SHARD = Math.max(1,
    Runtime.getRuntime().availableProcessors() / Integer.getInteger("dqe.numLocalShards", 4)); // line 119-121
private static final ForkJoinPool PARALLEL_POOL = new ForkJoinPool(
    Runtime.getRuntime().availableProcessors(), ..., true); // asyncMode=true, line 124-128
```

## 3. Key Helper Methods

| Method | Line | Purpose |
|--------|------|---------|
| `scanSegmentFlatSingleKey` | 5561 | Per-segment processing: reads DocValues, handles matchAll/filtered, COUNT/SUM/AVG |
| `scanDocRangeFlatSingleKeyCountStar` | 14195 | Doc-range COUNT(*) only: tight `findOrInsert → accData++` loop |
| `loadNumericColumn` | 14213 | Loads SortedNumericDocValues into contiguous `long[]` for cache-friendly access |
| `FlatSingleKeyMap.findOrInsert` | 13489 | Open-addressing probe: hash → linear probe → insert |
| `FlatSingleKeyMap.mergeFrom` | 13542 | Iterate other's slots, findOrInsert into this, add accData element-wise |

## 4. executeSingleKeyNumericFlatMultiBucket (line 4699-4912)

- Same structure but allocates `FlatSingleKeyMap[]` per bucket
- Segment-parallel only (no doc-range path)
- Each worker gets its own `FlatSingleKeyMap[numBuckets]`
- Merge: per-bucket `bucketMaps[b].mergeFrom(workerMaps[b])`

## 5. Answer: Does executeSingleKeyNumericFlat Already Have Parallelism?

**YES — it already has TWO parallel paths from iter19:**

1. **Doc-range parallel** (Path A): Only for `COUNT(*) + MatchAll + single bucket`. Splits doc ranges across threads. This is the narrowest fast path.

2. **Segment-parallel** (Path B): For all other parallelizable cases (non-COUNT(*) aggs, filtered queries, multi-bucket). Assigns whole segments to workers.

**For Q15** (`GROUP BY UserID ORDER BY COUNT(*) LIMIT 10`):
- Q15 uses COUNT(*) with MatchAll and single bucket → **hits Path A (doc-range parallel)**
- The doc-range path is already the fastest available path
- Top-N heap selection (LIMIT 10) runs sequentially after parallel aggregation

## 6. Key Line Numbers for Intercept Points

| What | Line |
|------|------|
| Method entry | 5129 |
| canParallelize check | 5173 |
| Doc-range parallel entry (Path A) | 5176 |
| Work unit construction | 5199-5210 |
| CompletableFuture submit | 5224 |
| Doc-range merge | 5238-5241 |
| Segment-parallel entry (Path B) | 5244 |
| Segment-parallel merge | 5302-5305 |
| Sequential fallback (Path C) | 5308 |
| Top-N heap start | 5330 |
| FlatSingleKeyMap class | 13448 |
| mergeFrom | 13542 |
