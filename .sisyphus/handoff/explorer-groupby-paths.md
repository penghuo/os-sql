# FusedGroupByAggregate.java — Execution Path Analysis

**File**: `dqe/src/main/java/org/opensearch/sql/dqe/shard/source/FusedGroupByAggregate.java` (13,200 lines)

## 1. Parallelism Infrastructure (lines 116-134)

```java
PARALLELISM_MODE = System.getProperty("dqe.parallelism", "docrange");  // "off" disables
THREADS_PER_SHARD = availableProcessors / numLocalShards(default=4);
PARALLEL_POOL = new ForkJoinPool(availableProcessors, ..., asyncMode=true);
```

Guard pattern used everywhere: `!"off".equals(PARALLELISM_MODE) && THREADS_PER_SHARD > 1 && leaves.size() > 1`

## 2. executeInternal Dispatch Tree (line 938)

```
executeInternal(line 938)
├── hasVarchar?
│   ├── 1 VARCHAR key + COUNT(*) only → executeSingleVarcharCountStar (line 1274)
│   ├── 1 VARCHAR key + general aggs → executeSingleVarcharGeneric (line 1947)
│   ├── has eval key → executeWithEvalKeys (line 6232)
│   └── else → executeWithVarcharKeys (line 7223)
│       ├── single-segment 2-key → tryOrdinalIndexedTwoKeyCountStar / FlatTwoKeyMap
│       ├── 3 keys → executeThreeKeyFlat (line 8893)
│       ├── multi-seg global-ord 2-key → executeMultiSegGlobalOrdFlatTwoKey (line 11068)
│       └── else → executeNKeyVarcharParallelDocRange (line 10733)
└── numeric only → executeNumericOnly (line 2812)
    ├── 1 key, no expr → executeSingleKeyNumeric (line 3172)
    │   ├── canUseFlatAccumulators → executeSingleKeyNumericFlat (line 4438)
    │   └── else → HashMap<SingleKeyHashMap, AccumulatorGroup> path
    ├── 2 keys, no expr → executeTwoKeyNumeric (line 5071)
    │   ├── canUseFlatAccumulators → executeTwoKeyNumericFlat (line 5435)
    │   └── else → HashMap path
    ├── all keys from same col (derived) → executeDerivedSingleKeyNumeric (line 3799)
    └── else (N keys with expressions) → Collector/Weight+Scorer HashMap path (line 2920)
```

## 3. executeSingleKeyNumericFlat (line 4438) — ALREADY HAS PARALLELISM

**Signature**: `executeSingleKeyNumericFlat(shard, query, keyInfos, specs, ..., bucket, numBuckets)`

**Data structure**: `FlatSingleKeyMap` (line 12462) — open-addressing hash map with:
- `long[] keys` — group key values
- `boolean[] occupied` — slot occupancy
- `long[] accData` — contiguous accumulator array, `slotsPerGroup` longs per slot
- `mergeFrom(other)` — element-wise addition of accData for matching keys

**Parallel pattern (lines 4481-4543)**:
1. Check `canParallelize` guard
2. Partition segments via largest-first greedy assignment to `numWorkers` buckets
3. Each worker gets own `FlatSingleKeyMap localMap`
4. Workers scan their segments via `scanSegmentFlatSingleKey()` into localMap
5. `CompletableFuture.supplyAsync(..., PARALLEL_POOL)` for each worker
6. `CompletableFuture.allOf(futures).join()` to wait
7. Sequential merge: `flatMap.mergeFrom(workerMap)` for each worker result

**Sequential fallback (lines 4548-4558)**: Single FlatSingleKeyMap, iterate all leaves.

**Multi-bucket partitioning** (in `executeSingleKeyNumeric`, lines 3247-3330):
When estimated groups > `MAX_CAPACITY` (8M), partitions key space into buckets.
Each bucket call to `executeSingleKeyNumericFlat` passes `bucket` and `numBuckets`.
Buckets themselves can be parallelized via CompletableFuture when `parallelBuckets > 1`.

## 4. Parallel Patterns Summary (all use same template)

| Method | Line | Data Structure | Parallel Strategy |
|--------|------|---------------|-------------------|
| executeSingleVarcharCountStar | 1634 | `HashMap<BytesRefKey, long[]>` | Segment-partitioned workers, merge maps |
| executeSingleVarcharGeneric | 2528 | `HashMap<BytesRefKey, AccumulatorGroup>` | Segment-partitioned workers, merge maps |
| executeSingleKeyNumericFlat | 4481 | `FlatSingleKeyMap` | Segment-partitioned workers, `mergeFrom()` |
| executeDerivedSingleKeyNumeric | 3952 | `FlatSingleKeyMap` | Segment-partitioned workers, `mergeFrom()` |
| executeTwoKeyNumericFlat | 5516 | `FlatTwoKeyMap` | Segment-partitioned workers, `mergeFrom()` |
| executeNKeyVarcharParallelDocRange | 10733 | `Map<MergedGroupKey, AccumulatorGroup>[]` | Doc-range split per segment |

**Common parallel template** (segment-partitioned):
```java
int numWorkers = Math.min(THREADS_PER_SHARD, leaves.size());
List<LeafReaderContext>[] workerSegments = new List[numWorkers];
// Largest-first greedy assignment for balanced load
sortedLeaves.sort((a, b) -> Integer.compare(b.reader().maxDoc(), a.reader().maxDoc()));
for (leaf : sortedLeaves) { assign to lightest worker }
// Submit to PARALLEL_POOL
CompletableFuture<LocalMapType>[] futures = ...;
for (w : workers) { futures[w] = CompletableFuture.supplyAsync(() -> { scan segments }, PARALLEL_POOL); }
CompletableFuture.allOf(futures).join();
// Sequential merge
for (future : futures) { mainMap.mergeFrom(future.join()); }
```

**Doc-range pattern** (executeNKeyVarcharParallelDocRange, line 10733):
- Collects matching doc IDs per segment
- Splits doc ID array among workers (chunk-based)
- Each worker opens own DocValues readers for the same segment
- Uses `Map<MergedGroupKey, AccumulatorGroup>[]` per worker

## 5. Data Structure Paths

| Path | Data Structure | When Used |
|------|---------------|-----------|
| Flat array | `FlatSingleKeyMap` (long[] keys + long[] accData) | 1 numeric key, COUNT/SUM/AVG of longs |
| Flat array | `FlatTwoKeyMap` (long[] key1, long[] key2 + long[] accData) | 2 numeric keys, COUNT/SUM/AVG of longs |
| Flat array | `FlatThreeKeyMap` (line 12334) | 3 keys in varchar path |
| HashMap | `HashMap<BytesRefKey, long[]>` | Single VARCHAR key + COUNT(*) |
| HashMap | `HashMap<BytesRefKey, AccumulatorGroup>` | Single VARCHAR key + general aggs |
| HashMap | `HashMap<SegmentGroupKey, AccumulatorGroup>` | N numeric keys with expressions |
| HashMap | `Map<MergedGroupKey, AccumulatorGroup>` | N-key varchar path |

## 6. Key Inner Loop: scanSegmentFlatSingleKey (line 4798)

Handles both MatchAll and filtered paths. For MatchAll + COUNT(*) only:
```java
// Ultra-fast: nextDoc() on key column, no agg DV reads
while (doc != NO_MORE_DOCS) {
    long key0 = dv0.nextValue();
    int slot = flatMap.findOrInsert(key0);
    flatMap.accData[slot * slotsPerGroup + accOffset[0]]++;
    doc = dv0.nextDoc();
}
```

For filtered queries: uses `Weight.scorer(leafCtx)` → `DocIdSetIterator` loop.

## 7. Key Finding: executeSingleKeyNumericFlat IS ALREADY PARALLEL

The task description says "executeSingleKeyNumericFlat (currently sequential)" — but the code at lines 4481-4543 shows it **already has a parallel path**. The parallel path:
- Partitions segments across workers using greedy load balancing
- Each worker scans into a local `FlatSingleKeyMap`
- Results merged via `flatMap.mergeFrom(workerMap)`

The sequential path is only used as a fallback when `canParallelize` is false (parallelism off, single thread, or single segment).
