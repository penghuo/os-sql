# Parallel Execution Patterns in FusedGroupByAggregate.java

## 1. executeSingleKeyNumericFlat (line 4350)

**Signature:**
```java
private static List<Page> executeSingleKeyNumericFlat(
    IndexShard shard, Query query, List<KeyInfo> keyInfos, List<AggSpec> specs,
    Map<String, Type> columnTypeMap, List<String> groupByKeys, int numAggs,
    boolean[] isCountStar, int[] accType, int sortAggIndex, boolean sortAscending, long topN)
```

**Approach:**
- Uses `FlatSingleKeyMap` (line 11970) — open-addressing hash map with `long[] keys`, `boolean[] occupied`, `long[] accData` (contiguous flat arrays)
- Eliminates per-group object allocation (AccumulatorGroup + MergeableAccumulator[] + CountStarAccum)
- Supports COUNT(*), SUM(long), AVG(long) only (accTypes 0, 1, 2)
- Called from `executeSingleKeyNumeric` (line 3242) when `canUseFlatAccumulators == true`

**Already parallelized (segment-level):**
- Lines 4395-4449: Checks `canParallelize` (PARALLELISM_MODE != "off" && THREADS_PER_SHARD > 1 && leaves.size() > 1)
- Creates per-worker `FlatSingleKeyMap`, assigns segments via largest-first greedy balancing
- Uses `CompletableFuture.supplyAsync(..., PARALLEL_POOL)` per worker
- Merges via `flatMap.mergeFrom(workerMap)` after `CompletableFuture.allOf(futures).join()`

**Why it can be slow (Q15):**
- For high-cardinality keys, `FlatSingleKeyMap.findOrInsert()` uses linear probing with Murmur3 hash
- With many groups, cache misses on the flat arrays dominate (keys[], occupied[], accData[] are separate arrays)
- Single-threaded segment scan when only 1 segment exists (canParallelize requires leaves.size() > 1)

## 2. executeWithEvalKeys (line 5813) — Parallel Pattern

**Approach:**
- Handles GROUP BY with eval expressions (CASE WHEN, date_trunc, etc.)
- Uses `LinkedHashMap<Object, AccumulatorGroup>` for grouping (line 6172)
- Collects doc IDs per segment into `int[]`, then processes with DocValues
- Currently **sequential** — iterates segments in a single loop (line 6183+)
- No CompletableFuture usage within this method itself

**Note:** The commit 80f906c13 parallelism may refer to a different method or a different level of parallelism.

## 3. executeNKeyVarcharParallelDocRange (line 10241)

**Signature:**
```java
private static List<Page> executeNKeyVarcharParallelDocRange(
    Engine.Searcher engineSearcher, Query query, List<KeyInfo> keyInfos, List<AggSpec> specs,
    int numAggs, boolean[] isCountStar, boolean[] isDoubleArg, boolean[] isVarcharArg,
    int[] accType, String[] truncUnits, String[] arithUnits, List<String> groupByKeys,
    int sortAggIndex, boolean sortAscending, long topN, Map<String, Type> columnTypeMap)
```

**Parallel pattern (doc-range splitting):**
1. For each segment: collect all matching doc IDs into `int[]` via Scorer
2. Split doc IDs into chunks: `chunkSize = (matchCount + numWorkers - 1) / numWorkers`
3. Each worker gets its own `Map<MergedGroupKey, AccumulatorGroup>` (per-worker map, line 10283)
4. Each worker opens its own DocValues readers (required — DocValues are not thread-safe)
5. Workers run via `CompletableFuture.runAsync(..., PARALLEL_POOL)` (line 10305+)
6. After all segments: merge per-worker maps into a global map

**Key difference from segment-level parallelism:** Splits docs *within* a single segment across workers, enabling parallelism even with 1 segment.

## 4. Thread Pool Configuration (lines 115-133)

```java
// System property: -Ddqe.parallelism=docrange (default) | off
private static final String PARALLELISM_MODE = System.getProperty("dqe.parallelism", "docrange");

// Threads = availableProcessors / numLocalShards (default 4)
private static final int THREADS_PER_SHARD = Math.max(1,
    Runtime.getRuntime().availableProcessors() / Integer.getInteger("dqe.numLocalShards", 4));

// Shared ForkJoinPool with asyncMode=true (work-stealing)
private static final ForkJoinPool PARALLEL_POOL = new ForkJoinPool(
    Runtime.getRuntime().availableProcessors(),
    ForkJoinPool.defaultForkJoinWorkerThreadFactory, null, true);
```

- **Pool type:** `java.util.concurrent.ForkJoinPool` (not ExecutorService)
- **Exposed via:** `getParallelPool()` (line 133) for use by other fast paths
- **Submission:** Always `CompletableFuture.supplyAsync/runAsync(..., PARALLEL_POOL)`
- **Join:** Always `CompletableFuture.allOf(futures).join()`

## 5. Hash-Partitioned Aggregation (line 4960+)

For two-key numeric paths when cardinality exceeds `FlatTwoKeyMap.MAX_CAPACITY`:
- Splits into `numBuckets` based on hash of keys
- Each bucket runs `executeTwoKeyNumericFlat(shard, ..., bkt, numBuckets)` independently
- Parallelized across buckets via `CompletableFuture.supplyAsync(..., PARALLEL_POOL)`
- Results concatenated after `allOf().join()`

## Summary of Parallel Patterns

| Pattern | Method | Parallelism Type | Data Structure |
|---------|--------|-----------------|----------------|
| Segment-level | executeSingleKeyNumericFlat:4395 | Segments → workers | FlatSingleKeyMap per worker |
| Doc-range | executeNKeyVarcharParallelDocRange:10241 | Docs within segment → workers | Map<MergedGroupKey, AccumulatorGroup> per worker |
| Hash-partition | executeTwoKeyNumeric:4987 | Buckets → workers | FlatTwoKeyMap per bucket |
| Segment-level | executeSingleVarcharCountStar:1670 | Segments → workers | HashMap<BytesRefKey, long[]> per worker |
| Segment-level | executeSingleVarcharGeneric:2560 | Segments → workers | HashMap<BytesRefKey, AccumulatorGroup> per worker |
