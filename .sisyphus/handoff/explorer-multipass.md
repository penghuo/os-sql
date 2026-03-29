# Multi-Pass Optimization in FusedGroupByAggregate.java

## 1. Location and Mechanism

**File:** `dqe/src/main/java/org/opensearch/sql/dqe/shard/source/FusedGroupByAggregate.java`

### Multi-Pass (Hash-Partitioned Bucket) Optimization

The "multi-pass" optimization is a **hash-partitioned bucket aggregation** that kicks in when the estimated number of groups exceeds `MAX_CAPACITY` (8M). It works identically for both single-key and two-key paths:

**Single-key path** (line 3241-3340):
- Estimates `totalDocs` across all segments
- Computes `numBuckets = ceil(totalDocs / FlatSingleKeyMap.MAX_CAPACITY)` (line 3252)
- If `numBuckets > 1`: executes multiple passes of `executeSingleKeyNumericFlat()`, each pass only aggregating docs whose `hash(key) % numBuckets == bucket`
- Passes can run in parallel via `CompletableFuture` + `PARALLEL_POOL` (line 3275-3305)
- Results merged via `mergePartitionedPages()` (line 3303)

**Two-key path** (line 5073-5172):
- Same pattern: `numBuckets = ceil(totalDocs / FlatTwoKeyMap.MAX_CAPACITY)` (line 5084)
- Each pass calls `executeTwoKeyNumericFlat()` with `bucket` and `numBuckets` params
- Bucket filtering in `collectFlatTwoKeyDoc()` (line 5822-5824):
  ```java
  if (numBuckets > 1) {
    int docBucket = (TwoKeyHashMap.hash2(key0, key1) & 0x7FFFFFFF) % numBuckets;
    if (docBucket != bucket) return;
  }
  ```
- Also parallelized via `CompletableFuture` + `PARALLEL_POOL` (line 5107-5140)

## 2. Is It Limited to Single-Key GROUP BY?

**No — it already supports both single-key AND two-key GROUP BY.**

| Path | Multi-Pass | Parallel Buckets | Parallel Segments |
|------|-----------|-----------------|-------------------|
| Single-key flat (`executeSingleKeyNumericFlat`) | ✅ line 3252 | ✅ line 3275 | ✅ line 4484 |
| Two-key flat (`executeTwoKeyNumericFlat`) | ✅ line 5084 | ✅ line 5107 | ✅ line 5447 |
| Non-flat single-key (`SingleKeyHashMap`) | ❌ | ❌ | ❌ |
| Non-flat two-key (`TwoKeyHashMap`) | ❌ | ❌ | ❌ |

### Constraint: Flat path only
Multi-pass only applies when `canUseFlatAccumulators == true`, which requires:
- All aggregates are COUNT(*), SUM(long), or AVG(long)
- No double args, no VARCHAR args, no COUNT(DISTINCT), no MIN/MAX

## 3. What Would Be Needed to Extend to 2-Key GROUP BY

**The multi-pass optimization already exists for 2-key GROUP BY.** Both paths are structurally identical. The key data structures are:

- `FlatSingleKeyMap` (line 12093): `long[] keys` — single key array, MAX_CAPACITY=8M
- `FlatTwoKeyMap` (line 11815): `long[] keys0, long[] keys1` — parallel key arrays, MAX_CAPACITY=8M
- `FlatThreeKeyMap` also exists (line ~12070, has `mergeFrom` at line 12070)

### If the question is about extending to non-flat 2-key paths:
The non-flat `TwoKeyHashMap` path (line 5176) does NOT have multi-pass. To add it:
1. Estimate `totalDocs` and compute `numBuckets` (same formula)
2. Add `bucket`/`numBuckets` params to the collector loop
3. Filter docs with `(TwoKeyHashMap.hash2(key0, key1) & 0x7FFFFFFF) % numBuckets != bucket`
4. Run passes sequentially or in parallel
5. Merge results

This would benefit queries with double/VARCHAR aggs or COUNT(DISTINCT) that currently can't use the flat path.

## 4. Existing Parallel Segment Scanning in GROUP BY Path

### ForkJoinPool Configuration (line 115-133)
```java
PARALLELISM_MODE = System.getProperty("dqe.parallelism", "docrange"); // default: on
THREADS_PER_SHARD = availableProcessors / numLocalShards(default=4);
PARALLEL_POOL = new ForkJoinPool(availableProcessors, ..., asyncMode=true);
```

### Parallel Segment Scanning Locations

| Method | Line | Mechanism |
|--------|------|-----------|
| `executeSingleKeyNumericFlat` | 4484 | Segments partitioned across workers, each with own `FlatSingleKeyMap`, merged via `mergeFrom()` |
| `executeTwoKeyNumericFlat` | 5447 | Same pattern with `FlatTwoKeyMap`, largest-first greedy assignment |
| Single VARCHAR COUNT(*) | 1633 | Parallel multi-segment path for ordinal-based counting |

### Segment Partitioning Strategy (same in both paths)
1. Sort segments largest-first by `maxDoc()`
2. Greedy assignment to `numWorkers = min(THREADS_PER_SHARD, numSegments)` workers
3. Each worker gets own `FlatMap`, scans assigned segments
4. Main thread merges via `flatMap.mergeFrom(workerMap)`

### Two Levels of Parallelism
The code has **two orthogonal parallelism axes**:
1. **Bucket-level parallelism** (multi-pass): Different buckets run in parallel via `CompletableFuture`
2. **Segment-level parallelism** (within each bucket/pass): Segments partitioned across workers

Both can be active simultaneously for very large datasets.
