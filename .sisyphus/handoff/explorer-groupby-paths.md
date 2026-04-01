# FusedGroupByAggregate GROUP BY Execution Paths

**File**: `dqe/src/main/java/org/opensearch/sql/dqe/shard/source/FusedGroupByAggregate.java` (14,493 lines)

## 1. Parallelism Infrastructure (L118-135)

- `PARALLELISM_MODE` = `System.getProperty("dqe.parallelism", "docrange")` — default "docrange"
- `THREADS_PER_SHARD` = `availableProcessors / dqe.numLocalShards(default 4)`
- `PARALLEL_POOL` = shared `ForkJoinPool` with `asyncMode=true` (work-stealing), sized to `availableProcessors`
- Guard: `canParallelize = !"off".equals(PARALLELISM_MODE) && THREADS_PER_SHARD > 1 && leaves.size() > 1`

## 2. executeSingleKeyNumericFlat (L5166-5600)

**Signature** (L5166):
```java
private static List<Page> executeSingleKeyNumericFlat(
    IndexShard shard, Query query, List<KeyInfo> keyInfos, List<AggSpec> specs,
    Map<String,Type> columnTypeMap, List<String> groupByKeys, int numAggs,
    boolean[] isCountStar, int[] accType, int sortAggIndex, boolean sortAscending,
    long topN, int bucket, int numBuckets)
```

**Data structure**: `FlatSingleKeyMap` (L13662) — open-addressing hash map with `long[] keys` + `long[] accData` (contiguous slots per group). Max 32M groups.

**Three execution branches** (all inside a single `acquireSearcher` block):

### Branch A: Parallel doc-range for COUNT(*) + MatchAll (L5218-5287)
- Condition: `allCountStar && isMatchAll && numBuckets <= 1`
- Loads columnar key arrays per segment via `loadNumericColumn()`
- **High cardinality (>10M docs)**: Falls back to **sequential** scan into single map (L5237) — comment says "parallel workers each build large maps that thrash L3 cache"
- **Low cardinality (≤10M docs)**: Splits docs across `THREADS_PER_SHARD` workers via `CompletableFuture.supplyAsync` on `PARALLEL_POOL`. Each worker builds local `FlatSingleKeyMap`, then merged via `flatMap.mergeFrom(workerMap)` (L5286)

### Branch B: Segment-parallel for general aggs (L5290-5355)
- Condition: `canParallelize` but not COUNT(*)-only
- Greedy largest-first segment assignment to workers for load balancing (L5305-5315)
- Each worker calls `scanSegmentFlatSingleKey()` (L5612) per assigned segment
- Workers build local `FlatSingleKeyMap`, merged into main map via `mergeFrom()` (L5354)

### Branch C: Sequential fallback (L5358-5375)
- Condition: `!canParallelize` (single segment or parallelism off)
- Single `FlatSingleKeyMap`, iterates all segments sequentially

**Key insight for Q15**: The >10M doc threshold at L5237 forces sequential execution. This is the bottleneck for 100M row scans.

## 3. executeWithTopN (L1249-1258)

**Signature**:
```java
public static List<Page> executeWithTopN(
    AggregationNode aggNode, IndexShard shard, Query query,
    Map<String,Type> columnTypeMap, int sortAggIndex, boolean sortAscending, long topN)
```

Simply delegates to `executeInternal()` (L1280) with sort parameters. TopN is handled **post-aggregation** inside each execution path:

**TopN selection** (L5393-5440 in executeSingleKeyNumericFlat):
- Uses a **min/max heap** of size N over the flat map slots
- Heap compares `accData[slot * slotsPerGroup + sortAccOff]` values
- After heap selection, sorts the N winners and builds output page
- Same pattern replicated in other paths (executeTwoKeyNumericFlat, etc.)

**Early termination** in `executeNKeyVarcharParallelDocRange` (L11943):
- `earlyTerminate = (sortAggIndex < 0 && topN > 0)` — LIMIT without ORDER BY
- Uses `AtomicBoolean stopFlag` checked every 4096 docs
- Workers stop once total groups across all workers ≥ topN

## 4. Hash-Partitioned Aggregation (Multi-Bucket)

**Trigger** (L3590-3615 in `executeSingleKeyNumeric`):
- First tries single-bucket `executeSingleKeyNumericFlat` with `bucket=0, numBuckets=1`
- On overflow (`"GROUP BY exceeded memory limit"` exception), computes `numBuckets = ceil(totalDocs / MAX_CAPACITY)`
- Falls back to `executeSingleKeyNumericFlatMultiBucket` (L4734)

**executeSingleKeyNumericFlatMultiBucket** (L4734-4860):
- Allocates `FlatSingleKeyMap[numBuckets]` — one map per hash bucket
- Hash partitioning: `bkt = (SingleKeyHashMap.hash1(key) & 0x7FFFFFFF) % numBuckets` (L5011)
- Parallel: each worker gets own `FlatSingleKeyMap[numBuckets]`, merged per-bucket via `mergeFrom()`
- Final merge: `bucketMaps[1..N]` merged into `bucketMaps[0]` (L4843)
- Dedicated scan method: `scanSegmentFlatSingleKeyMultiBucket` (L4952) — avoids per-doc hash-partition filter skip

**Important**: This pattern exists ONLY for single numeric key. No multi-bucket path for varchar or multi-key.

## 5. executeNKeyVarcharParallelDocRange (L11912-12240)

**Signature**:
```java
private static List<Page> executeNKeyVarcharParallelDocRange(
    Engine.Searcher engineSearcher, Query query, List<KeyInfo> keyInfos,
    List<AggSpec> specs, int numAggs, boolean[] isCountStar, boolean[] isDoubleArg,
    boolean[] isVarcharArg, int[] accType, String[] truncUnits, String[] arithUnits,
    List<String> groupByKeys, int sortAggIndex, boolean sortAscending, long topN,
    Map<String,Type> columnTypeMap)
```

**Architecture**: Per-segment doc-range parallelism with ordinal resolution
1. For each segment: collect all matching doc IDs into `int[] matchedDocs`
2. Split doc IDs into chunks across `numWorkers`
3. Each worker opens its own DocValues readers (L12001-12015)
4. Workers build local `Map<SegmentGroupKey, AccumulatorGroup>` using `NumericProbeKey` for hashing
5. After scanning, workers resolve segment-local ordinals to strings via `dv.lookupOrd()` (L12185)
6. Workers merge resolved keys into per-worker global `Map<MergedGroupKey, AccumulatorGroup>` (L12196)
7. After all segments: merge all worker maps via `mergeMergedGroupMaps()` (L12219)

**Merge** (`mergeMergedGroupMaps` at L12788): Simple iterate-and-merge into target map using `AccumulatorGroup.merge()`.

**Dispatch** (L11489 in `executeNKeyVarcharPath`):
- Condition: `"docrange".equals(PARALLELISM_MODE) && THREADS_PER_SHARD > 1`
- Falls through from `executeMultiSegGlobalOrdFlatTwoKey` (tried first for 2-key numeric+varchar)

## 6. executeMultiSegGlobalOrdFlatTwoKey (L12247+)

- Specialized for 2-key (numeric + varchar) using global ordinals + `FlatTwoKeyMap`
- Avoids per-group object allocation AND per-segment ordinal resolution
- Only resolves ordinals to strings for final top-N output groups
- Returns `null` if not applicable (too many global ordinals), falling through to doc-range path

## 7. Dispatch Flow Summary

```
executeWithTopN / execute
  └─ executeInternal (L1280)
       ├─ hasVarchar?
       │   ├─ 1 varchar key + COUNT(*) → executeSingleVarcharCountStar (L1616)
       │   ├─ 1 varchar key + general → executeSingleVarcharGeneric (L2293)
       │   ├─ has eval key → executeWithEvalKeys (L7301)
       │   └─ multi-key/varchar → executeWithVarcharKeys (L8349)
       │       └─ executeNKeyVarcharPath (L10549)
       │           ├─ 2-key global ord → executeMultiSegGlobalOrdFlatTwoKey (L12247)
       │           ├─ 3-key flat → executeThreeKeyFlat (L10069)
       │           └─ parallel doc-range → executeNKeyVarcharParallelDocRange (L11912)
       └─ numeric only → executeNumericOnly (L3159)
           ├─ 1 key → executeSingleKeyNumeric (L3519)
           │   ├─ flat → executeSingleKeyNumericFlat (L5166)
           │   └─ overflow → executeSingleKeyNumericFlatMultiBucket (L4734)
           ├─ 2 keys → executeTwoKeyNumeric (L6018)
           │   └─ flat → executeTwoKeyNumericFlat (L6349)
           └─ derived → executeDerivedSingleKeyNumeric (L4073)
```

## 8. Key Patterns for Extension

| Pattern | Where | How |
|---------|-------|-----|
| Segment-parallel with greedy assignment | L5290-5355 | Largest-first segment→worker, local maps, mergeFrom |
| Doc-range parallel | L5246-5287, L11912+ | Split docs across workers, each builds local map |
| Hash-partitioned buckets | L4734-4860 | `hash(key) % numBuckets`, per-bucket FlatMap |
| FlatMap mergeFrom | L13776 | Element-wise `accData[j] += other.accData[j]` |
| AccumulatorGroup merge | L14119 | Per-accumulator polymorphic merge |
| Early termination | L11943 | AtomicBoolean stopFlag for LIMIT without ORDER BY |
| TopN heap selection | L5393-5440 | Min/max heap over flat map slots post-aggregation |
