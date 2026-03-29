# FusedGroupByAggregate Parallel Execution Patterns

**File**: `dqe/src/main/java/org/opensearch/sql/dqe/shard/source/FusedGroupByAggregate.java`

## 1. Infrastructure (lines 116-133)

- `PARALLELISM_MODE`: System property `dqe.parallelism`, default `"docrange"`
- `THREADS_PER_SHARD`: `availableProcessors / dqe.numLocalShards(default 4)`
- `PARALLEL_POOL`: Shared `ForkJoinPool` with `asyncMode=true` (work-stealing), sized to `availableProcessors`

## 2. executeInternal Dispatch (line 937)

Routing logic:
1. Parses `AggSpec` list (funcName, isDistinct, arg, argType, applyLength)
2. Classifies keys into `KeyInfo` (plain column, date_trunc, extract, arith, eval)
3. Sets `hasVarchar` flag based on key types
4. **If hasVarchar**:
   - 1 VARCHAR key + COUNT(*) only → `executeSingleVarcharCountStar`
   - 1 VARCHAR key + general aggs → `executeSingleVarcharGeneric`
   - Any eval key → `executeWithEvalKeys`
   - Otherwise → `executeWithVarcharKeys` (line 6666)
5. **If all numeric** → `executeNumericOnly` (line 2811)

### executeNumericOnly sub-dispatch (line 2811):
- 1 key, no expressions → `executeSingleKeyNumeric` (line 3171)
- 2 keys, no expressions → `executeTwoKeyNumeric` (line 4799)
- N keys from same column (arith) → `executeDerivedSingleKeyNumeric`
- Fallback → generic N-key numeric path

### executeWithVarcharKeys sub-dispatch (line 6666):
- Single segment, 2 keys → tries ordinal-indexed path, then flat two-key varchar path
- Multi-segment, 2 keys with 1 varchar → tries `executeMultiSegGlobalOrdFlatTwoKey`
- **Parallel fallback** (line ~9763): if `PARALLELISM_MODE=="docrange"` && `THREADS_PER_SHARD > 1` → `executeNKeyVarcharParallelDocRange`

## 3. executeNKeyVarcharParallelDocRange (line 10186) — THE PARALLEL PATTERN

**Architecture**: Per-segment doc-range partitioning with per-worker hash maps.

**Steps**:
1. **Setup**: `numWorkers = min(THREADS_PER_SHARD, availableProcessors)`. Creates `workerMaps[numWorkers]` — each a `LinkedHashMap<MergedGroupKey, AccumulatorGroup>`.
2. **Per-segment loop**: For each `LeafReaderContext`:
   a. Collect all matching doc IDs into `int[] matchedDocs` via `Weight.scorer()`
   b. Split docs into chunks: `chunkSize = ceil(matchCount / numWorkers)`
   c. Each worker gets `[startIdx, endIdx)` range of the doc array
   d. Workers run via `CompletableFuture.runAsync(..., PARALLEL_POOL)`
3. **Per-worker processing**: Each worker:
   - Opens its own DocValues readers (each worker needs separate instances)
   - Uses `NumericProbeKey` for hash lookups into a local `segmentGroups` HashMap
   - Accumulates into `AccumulatorGroup` per group
   - After segment scan: resolves ordinals → `MergedGroupKey`, merges into `workerMaps[workerIdx]`
4. **Segment barrier**: `CompletableFuture.allOf(segFutures).join()` — waits before next segment
5. **Early termination**: If `topN > 0 && sortAggIndex < 0`, sets `AtomicBoolean stopFlag` when total groups across workers >= topN. Workers check every 4096 docs.
6. **Final merge**: Iterates all `workerMaps`, calls `mergeMergedGroupMaps(globalGroups, wMap)` to combine
7. **Output**: `buildMergedGroupOutput(globalGroups, ...)` with optional top-N heap selection

**Key design decisions**:
- Doc-range partitioning (not hash-partitioning) within each segment
- Each worker has its own DocValues readers (Lucene requirement)
- Segment-local ordinals resolved to global keys before merging
- No synchronization needed — workers write to separate maps

## 4. executeSingleKeyNumericFlat (line 4269) — ALREADY PARALLELIZED ✅

**Parallel path** (lines ~4315-4380):
- Condition: `PARALLELISM_MODE != "off" && THREADS_PER_SHARD > 1 && leaves.size() > 1`
- Strategy: **Segment-level partitioning** (not doc-range)
  - Largest-first greedy assignment of segments to workers for load balancing
  - Each worker gets a `FlatSingleKeyMap localMap`
  - Workers process their assigned segments via `CompletableFuture.supplyAsync(..., PARALLEL_POOL)`
  - After all workers complete: `flatMap.mergeFrom(workerMap)` for each worker
- Uses `scanSegmentFlatSingleKey()` helper for per-segment processing
- Falls back to sequential single-map path when parallelism conditions not met

## 5. executeTwoKeyNumericFlat (line 5163) — SEQUENTIAL INTERNALLY, BUCKET-PARALLEL EXTERNALLY

**The method itself is sequential** — it processes all segments in a single thread with one `FlatTwoKeyMap`.

**But the caller (executeTwoKeyNumeric, line ~4884) adds bucket-level parallelism**:
- Estimates doc count; if groups could exceed `FlatTwoKeyMap.MAX_CAPACITY` (8M), partitions into buckets
- `numBuckets = ceil(totalDocs / MAX_CAPACITY)`
- If `numBuckets > 1 && PARALLELISM_MODE != "off"`:
  - Runs each bucket as a `CompletableFuture.supplyAsync` on `PARALLEL_POOL`
  - Each bucket call passes `(bucket, numBuckets)` — the method filters docs by `hash(key0, key1) % numBuckets == bucket`
  - Results merged via `mergePartitionedPages()`
- **When numBuckets == 1** (common case): fully sequential, no parallelism

**To parallelize like executeSingleKeyNumericFlat**: Add segment-level partitioning inside `executeTwoKeyNumericFlat` itself:
1. Check `canParallelize` (same conditions as single-key flat)
2. Greedy-assign segments to workers
3. Each worker gets own `FlatTwoKeyMap localMap`
4. Workers call `scanSegmentFlatTwoKey()` (new helper, analogous to `scanSegmentFlatSingleKey`)
5. Merge via `flatMap.mergeFrom(workerMap)`

## 6. REGEXP_REPLACE / Regex Handling

- `java.util.regex.Matcher` and `java.util.regex.Pattern` imported (lines 30-31)
- **Ordinal-cached expression evaluation** (line 306, method `canFuseWithExpressionKey`):
  - For queries like `GROUP BY regexp_replace(Referer, '^pattern$', '$1')`
  - Key insight: evaluate expression N times (once per unique ordinal) instead of M times (once per doc)
  - Example: Q29 with REGEXP_REPLACE on Referer: ~16K ordinals vs ~921K docs = ~58x reduction
- Expression compiled via `ExpressionCompiler` (line ~490) using `FunctionRegistry` + `BuiltinFunctions`
- Executed in `executeWithExpressionKeyImpl` (line ~570) — sequential, uses Lucene Collector
- The compiled `BlockExpression` is applied to ordinal values, results cached in ordinal→group mapping

## Summary Table

| Method | Parallel? | Strategy | Data Structure |
|--------|-----------|----------|----------------|
| `executeNKeyVarcharParallelDocRange` | ✅ Yes | Doc-range split per segment, per-worker maps | `HashMap<MergedGroupKey, AccumulatorGroup>` |
| `executeSingleKeyNumericFlat` | ✅ Yes | Segment-level greedy assignment | `FlatSingleKeyMap` per worker |
| `executeTwoKeyNumericFlat` | ❌ No (internally) | Sequential scan, bucket-parallel at caller level only | Single `FlatTwoKeyMap` |
| `executeWithExpressionKeyImpl` | ❌ No | Sequential Collector | `HashMap<String, AccumulatorGroup>` |
