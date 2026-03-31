# COUNT(DISTINCT) Execution Method Analysis

## Shared Infrastructure
- **Thread Pool**: `FusedGroupByAggregate.PARALLEL_POOL` — `ForkJoinPool(availableProcessors, asyncMode=true)`
- **Workers per shard**: `availableProcessors / dqe.numLocalShards(default=4)` → e.g., 4 workers on 16-core
- **Hash Set**: Custom `LongOpenHashSet` — open-addressing, long[] keys, EMPTY sentinel, 0.65 load factor

---

## Method 1: executeCountDistinctWithHashSets (line 1000) — Q08 path
- **Parallelism**: YES — CountDownLatch + ForkJoinPool, one task per segment
- **Pattern**: Dispatch N-1 segments to pool, run last on caller thread, await latch
- **Hot loop** (scanSegmentForCountDistinct, line 1133):
  - MatchAll + no deletes: `FusedGroupByAggregate.loadNumericColumn()` loads DocValues into flat `long[]`, then iterates `for(doc=0; doc<maxDoc; doc++)` with inline open-addressing hash map
  - MatchAll + deletes: per-doc `nextDoc()` on SortedNumericDocValues
  - Filtered: Scorer-based iteration
- **Data structures**: Per-segment open-addressing map (`long[] grpKeys, LongOpenHashSet[] grpSets, boolean[] grpOcc`), 0.7 load factor, power-of-2 resize
- **Merge**: Sequential union of per-segment maps — smaller set merged into larger
- **Complexity**: O(N) scan + O(D) merge per segment where D=distinct count
- **BOTTLENECK**: `loadNumericColumn()` does `nextDoc()` loop to fill long[] — this is I/O bound on DocValues decompression. The inline hash map probing in the hot loop is cache-friendly but the column load is not parallelized within a segment.

## Method 2: executeVarcharCountDistinctWithHashSets (line 2410) — Q08 varchar variant
- **Parallelism**: YES — CompletableFuture + ForkJoinPool, greedy segment-to-worker assignment (largest-first)
- **Pattern**: numWorkers = min(availableProcessors/numLocalShards, leaves.size()), segments sorted by maxDoc desc, assigned to lightest worker
- **Hot loop**: Per-worker scans segments using global ordinals (`OrdinalMap`). MatchAll path uses `SortedDocValues.nextDoc()` loop; filtered uses Scorer
- **Data structures**: `LongOpenHashSet[]` indexed by global ordinal (one set per group-by value)
- **Merge**: Sequential merge of worker-local `LongOpenHashSet[]` arrays into global array via `addAll()`
- **Complexity**: O(N) scan + O(globalOrdCount * D_avg) merge
- **BOTTLENECK**: Global ordinal resolution (`segToGlobal.get()`) per doc. Merge phase is sequential over all ordinals.

## Method 3: executeNKeyCountDistinctWithHashSets (line 1329)
- **Parallelism**: YES — same CountDownLatch + ForkJoinPool pattern as Method 1
- **Hot loop** (scanSegmentForNKeyCountDistinct): Same as Method 1 but with composite `LongArrayKey` for N group-by keys
- **Data structures**: `HashMap<LongArrayKey, LongOpenHashSet>` — boxing overhead from LongArrayKey object creation per doc
- **Merge**: Sequential HashMap merge with set union
- **BOTTLENECK**: LongArrayKey allocation per doc in hot loop (object creation + hashing overhead)

## Method 4: executeMixedTypeCountDistinctWithHashSets (line 1649)
- **Parallelism**: YES — same CountDownLatch + ForkJoinPool pattern
- **Hot loop** (scanSegmentForMixedTypeCountDistinct): Handles mixed varchar+numeric group keys via `ObjectArrayKey`
- **Data structures**: `HashMap<ObjectArrayKey, LongOpenHashSet>` — even more boxing than Method 3 (String + Long objects)
- **Merge**: Sequential HashMap merge
- **BOTTLENECK**: ObjectArrayKey allocation + String materialization per doc

## Method 5: executeMixedDedupWithHashSets (line 1931)
- **Parallelism**: YES — CompletableFuture + ForkJoinPool, greedy segment assignment (same as Method 2)
- **Hot loop**: Per-worker inline open-addressing map with per-group accumulators (`long[][] grpAccs`)
- **Data structures**: `long[] grpKeys, LongOpenHashSet[] grpSets, long[][] grpAccs, boolean[] grpOcc`
- **Merge**: Sequential merge of worker maps
- **Complexity**: O(N) scan + O(groups * D) merge
- **BOTTLENECK**: Per-doc DocValues `nextDoc()` + `advanceExact()` for multiple aggregate columns

## Method 6: executeDistinctValuesScanWithRawSet (line 3023) — Q04 scalar path
- **Parallelism**: PARTIAL — Phase 1 (DocValues→long[]) is parallel via CompletableFuture per segment. Phase 2 (hash insertion) is SEQUENTIAL on single thread.
- **Hot loop**: Phase 2: `for(i=0; i<count; i++) distinctSet.add(vals[i])` — single-threaded insertion into one pre-sized LongOpenHashSet
- **Data structures**: One global `LongOpenHashSet` pre-sized to `min(maxDoc, 32M)`
- **Complexity**: O(N) for load + O(N) for insertion, but insertion is single-threaded
- **BOTTLENECK**: **Phase 2 is the critical bottleneck for Q04.** ~100M values inserted sequentially into one hash set. The comment says "hash insertion is sequential, cache-bound" — this is the 2.4s wall time. Parallel load helps I/O but the hash set insertion dominates.

## Method 7: executeDistinctValuesScanVarcharWithRawSet (line 3053) — Q05 scalar path
- **Parallelism**: PARTIAL — Uses global ordinals. If no deletes + >100K ordinals: parallel String resolution via CompletableFuture workers. Otherwise sequential.
- **Hot loop**: `for(g=start; g<end; g++) { segOrd = ordinalMap.getFirstSegmentOrd(g); bytes = dv.lookupOrd(segOrd); chunk[i] = bytes.utf8ToString(); }`
- **Data structures**: `HashSet<String>` (standard Java, not custom) — boxing + String allocation overhead
- **Complexity**: O(globalOrdCount) for resolution + O(globalOrdCount) for HashSet insertion
- **BOTTLENECK**: **String allocation + java.util.HashSet overhead for Q05.** `utf8ToString()` allocates a new String per ordinal. HashSet<String> has boxing overhead. Final `Collections.addAll()` merge is sequential.

---

## Summary of Bottlenecks by Query

| Query | Method | Parallel? | Primary Bottleneck |
|-------|--------|-----------|-------------------|
| Q04 (scalar COUNT(DISTINCT UserID)) | #6 collectDistinctValuesRaw | Partial | **Sequential hash insertion** — Phase 2 single-threaded LongOpenHashSet.add() for ~100M values |
| Q05 (scalar COUNT(DISTINCT SearchPhrase)) | #7 collectDistinctStringsRaw | Partial | **String allocation + HashSet<String>** — utf8ToString() per ordinal, java.util.HashSet overhead |
| Q08 (GROUP BY + COUNT(DISTINCT)) | #1 scanSegmentForCountDistinct | Yes | **loadNumericColumn() DocValues decompression** — nextDoc() loop per segment, then inline hash map |

## Key Optimization Opportunities

1. **Q04**: Parallelize Phase 2 hash insertion — use per-worker LongOpenHashSets then merge, or use a concurrent/partitioned hash set
2. **Q04**: Consider HyperLogLog if approximate count is acceptable (eliminates hash set entirely)
3. **Q05**: Replace `HashSet<String>` with ordinal-based counting (just count globalOrdCount if MatchAll + no deletes — no need to materialize strings at all)
4. **Q05**: If exact strings needed, use `BytesRef` comparison instead of `utf8ToString()` to avoid String allocation
5. **Q08**: The parallel segment scan is good but `loadNumericColumn()` does redundant work — it loads ALL values then the hash loop re-reads them. Fusing the load+hash in one pass would halve memory bandwidth
6. **All methods**: Merge phase is always sequential — for high-cardinality groups this becomes significant
