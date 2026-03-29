# DQE Parallel Execution Analysis

## 1. Parallelism Infrastructure

### Thread Pool Configuration (FusedGroupByAggregate.java:116-128)
- `PARALLELISM_MODE`: System property `dqe.parallelism`, default `"docrange"`. Set to `"off"` to disable.
- `THREADS_PER_SHARD`: `availableProcessors / dqe.numLocalShards` (default 4 shards), minimum 1.
- `PARALLEL_POOL`: Shared `ForkJoinPool` with `availableProcessors` threads, `asyncMode=true` (work-stealing). Exposed via `getParallelPool()` static method.

### Common Parallelism Pattern (used everywhere)
1. Check `canParallelize`: `PARALLELISM_MODE != "off" && THREADS_PER_SHARD > 1 && leaves.size() > 1`
2. Partition segments via **largest-first greedy assignment** to balance doc counts across workers
3. Each worker gets a `CompletableFuture.supplyAsync(task, PARALLEL_POOL)`
4. `CompletableFuture.allOf(futures).join()` to wait
5. Merge worker-local results into global result

---

## 2. Parallel Paths (6 total)

### A. FusedScanAggregate.executeWithEval (line 154) — PARALLEL ✅
- **What**: Scalar SUM/COUNT/AVG with EvalNode (e.g., `SUM(col + constant)`)
- **How**: Segments partitioned across workers. Each worker accumulates `long[] pack` (sum+count per column). Merged by simple addition.
- **Condition**: `MatchAllDocsQuery && noDeletes && numWorkers > 1 && leaves > 1`

### B. FusedScanAggregate.execute (line 473, tryFlatArrayPath ~line 830) — PARALLEL ✅
- **What**: Scalar aggregates (SUM/COUNT/AVG/MIN/MAX) without EvalNode
- **How**: Same segment-partitioned pattern. Workers accumulate `long[]` packs.
- **Condition**: Same as above

### C. FusedGroupByAggregate — executeSingleVarcharCountStar (~line 1635) — PARALLEL ✅
- **What**: `SELECT col, COUNT(*) FROM t GROUP BY col` where col is VARCHAR
- **How**: Workers build per-worker `HashMap<BytesRefKey, long[]>` using ordinal arrays. Merged by iterating worker maps.
- **Condition**: `canParallelize && ordinalCount <= 500K` (high-cardinality VARCHAR disabled to avoid BytesRefKey copy overhead)

### D. FusedGroupByAggregate — executeSingleVarcharGeneric (~line 2530) — PARALLEL ✅
- **What**: Single VARCHAR key with general aggregates (SUM, AVG, etc.)
- **How**: Workers build per-worker `HashMap<BytesRefKey, AccumulatorGroup>`. Merged by iterating.
- **Condition**: Same 500K ordinal cap

### E. FusedGroupByAggregate — executeSingleKeyNumericFlat (line 4443) — PARALLEL ✅
- **What**: Single numeric key GROUP BY with COUNT/SUM/AVG (Q15 path)
- **How**: **Two levels of parallelism**:
  1. **Segment-level**: Workers each get a `FlatSingleKeyMap`, scan assigned segments via `scanSegmentFlatSingleKey`, then `flatMap.mergeFrom(workerMap)`.
  2. **Bucket-level** (line 3281): When estimated groups exceed `FlatSingleKeyMap.MAX_CAPACITY` (32M), data is hash-partitioned into buckets. Buckets run in parallel via `CompletableFuture.supplyAsync`.
- **FlatSingleKeyMap**: Open-addressing hash map with `long[] keys`, `boolean[] occupied`, `long[] accData` (contiguous slots). Initial capacity 4096, load factor 0.7, max 32M.

### F. TransportShardExecuteAction — executeCountDistinctWithHashSets (~line 1630) — PARALLEL ✅
- **What**: `COUNT(DISTINCT col1) GROUP BY col0` with numeric keys
- **How**: Workers build per-worker open-addressing hash maps with `LongOpenHashSet` per group. Merged by iterating worker maps and calling `set.addAll()`.

---

## 3. Sequential Paths (NOT parallelized)

### A. executeTwoKeyNumeric (FusedGroupByAggregate ~line 5313)
- Uses `TwoKeyHashMap` (open-addressing with two long keys). **Sequential** across segments.
- No parallel dispatch found.

### B. executeWithVarcharKeys (multi-key VARCHAR)
- Sequential HashMap-based grouping when multiple VARCHAR keys present.

### C. executeWithEvalKeys
- Sequential — handles CASE WHEN expressions as GROUP BY keys.

### D. FusedScanAggregate COUNT(DISTINCT) path (~line 1514)
- **Explicitly sequential**: Comment says "parallel with merge was 4.3x SLOWER due to LongOpenHashSet merge cost."

---

## 4. Hash Map Implementations

| Class | Location | Key Type | Usage |
|-------|----------|----------|-------|
| `FlatSingleKeyMap` | FusedGroupByAggregate:12529 | `long` | Single numeric key GROUP BY (open-addressing, parallel arrays) |
| `TwoKeyHashMap` | FusedGroupByAggregate:~5313 | `long, long` | Two numeric key GROUP BY |
| `SingleKeyHashMap` | Referenced for hash function | `long` | Hash function provider |
| `LongOpenHashSet` | dqe/operator/LongOpenHashSet.java:20 | `long` | COUNT(DISTINCT) per-group sets |
| `HashMap<BytesRefKey, long[]>` | FusedGroupByAggregate:~1671 | `BytesRefKey` | VARCHAR COUNT(*) grouping |
| `HashMap<BytesRefKey, AccumulatorGroup>` | FusedGroupByAggregate:~2561 | `BytesRefKey` | VARCHAR generic agg grouping |

---

## 5. Extension Opportunities

### executeSingleKeyNumericFlat (Q15) — Already parallel
This path **already has** both segment-level and bucket-level parallelism (lines 3957-4040 and 3281-3310). The segment-level parallel path partitions segments across workers, each with its own `FlatSingleKeyMap`, then merges via `flatMap.mergeFrom()`.

### executeTwoKeyNumeric — Candidate for parallelism
Currently sequential. Could follow the same pattern as `executeSingleKeyNumericFlat`: per-worker `TwoKeyHashMap` instances, segment partitioning, then merge.

### executeWithVarcharKeys (multi-key) — Candidate for parallelism
Currently sequential. Could parallelize with per-worker `HashMap<MergedGroupKey, AccumulatorGroup>` and merge, subject to the same 500K ordinal cap used in single-key VARCHAR paths.

### Hash-partitioned aggregation pattern
Already exists in `executeSingleKeyNumeric` (line 3258-3320): when estimated groups exceed `MAX_CAPACITY`, data is split into hash buckets processed in parallel. This pattern could be generalized to two-key and multi-key paths.
