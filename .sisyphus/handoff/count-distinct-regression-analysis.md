# COUNT(DISTINCT) Code Path Changes: Mar 26 Baseline → 7a341ca50 (wukong)

## Timeline of Changes to collectDistinctValuesRaw

### Mar 26 Baseline (084ad1e1e) — Q04 was 2.3s
**Simple sequential scan**: Single `LongOpenHashSet`, iterates DocValues per segment sequentially.
- No parallelism, no columnar loading, no hash partitioning
- Each segment scanned once via `dv.nextDoc()` loop, values inserted directly into one shared set

### Key Commits (chronological):

| Commit | Date | Summary |
|--------|------|---------|
| `a0b1e5c73` | Mar | Shard-level pre-dedup for scalar COUNT(DISTINCT) |
| `bdd43cd22` | Mar | Primitive LongOpenHashSet (replaces boxed HashSet) |
| `89e0ee80d` | Mar | Parallel per-segment scan + raw set merge |
| `1c6d2fed1` | Mar | Parallel hash insertion, ConcurrentHashMap for varchar |
| `9c4984065` | Apr | Group-partitioned CD + pre-sized hash sets |
| `cc0175dc0` | Apr | Fused DV scan for CD + prefetch-batched GROUP BY |
| `b4ed73b55` | Apr 1 | Run-length dedup + parallel single-segment CD (doc-range split) |
| `7a341ca50` | Apr 1 | **Hash-partitioned CD** (current — potential regression) |

### Commit 7a341ca50 — The Critical Change

**Before (b4ed73b55)**: Doc-range partitioning
- Workers split the array by doc ranges: worker 0 gets docs [0, N/W), worker 1 gets [N/W, 2N/W), etc.
- Each worker calls `addAllBatched(column, start, rangeLen)` on its range
- Merge: find largest set, `addAll` smaller sets into it
- **Each value scanned exactly once** across all workers

**After (7a341ca50)**: Hash partitioning
- Workers scan **ALL** docs but only insert keys where `partitionHash(key) & partMask == partId`
- **Each value scanned N times** (once per partition worker) — only inserted by one worker
- Merge: disjoint sets, simple concatenation

### ⚠️ THE REGRESSION MECHANISM

For Q04 (`SELECT COUNT(DISTINCT UserID) FROM hits`, 100M rows, BIGINT):

**Multi-segment path (leaves.size() > 1)**:
1. Phase 1: Parallel columnar load — reads all segment DocValues into `long[]` arrays (unchanged)
2. Phase 2 CHANGED:
   - **Old**: Per-segment hash insertion — each worker processes ONE segment's array via `addAllBatched`
   - **New**: Concatenates ALL segment arrays into one `allVals[totalVals]` array, then EACH partition worker scans the ENTIRE 100M-element array

**Single-segment path (maxDoc > 1M)**:
- Same pattern: each partition worker scans ALL docs, only inserts matching hash partition

**Work amplification**: With `numPartitions = N`, total work = N × 100M reads (vs 1 × 100M before).
If THREADS_PER_SHARD = 4 (on a 16-core / 4-shard machine), `numPartitions = 4`, so 4× the reads.
But the cache locality benefit only helps if partition sets fit in L3 — for 18M distinct UserIDs,
each partition set is ~4.5M entries × 8 bytes = 36MB, which may NOT fit in L3 cache.

### BIGINT vs VARCHAR Difference

| Aspect | BIGINT (collectDistinctValuesRaw) | VARCHAR (collectDistinctStringsRaw) |
|--------|-----------------------------------|-------------------------------------|
| Data structure | `LongOpenHashSet` (primitive) | `HashSet<String>` / `ConcurrentHashMap.newKeySet()` |
| Parallelism | Hash-partitioned (each worker scans ALL values) | Global ordinals + parallel ordinal resolution |
| Work per value | Scanned N times (once per partition) | Scanned once (ordinal-based dedup) |
| Merge cost | `addAll` into largest partition | Already deduplicated via ordinals |

**VARCHAR uses ordinal-based dedup** (FixedBitSet per segment, then global ordinal map) — each doc is visited exactly once. BIGINT has no ordinal equivalent, so it uses the hash-partitioned approach which scans all values N times.

## Root Cause Hypothesis

The hash-partitioned approach in 7a341ca50 trades **N× more memory reads** for **disjoint merge** (no duplicate insertions during merge). This is a net win when:
- Partition sets fit in L3 cache (small distinct count)
- The merge cost of overlapping sets dominates

But for Q04 (18M distinct UserIDs across 100M rows):
- Partition sets are ~36MB each — likely exceeds L3 cache
- The old per-segment approach scanned each value once and merged overlapping sets
- The new approach scans 100M values × 4 workers = 400M reads, all hitting main memory

**The 2.3s → 539s regression (234×) suggests the hash-partitioned scan is thrashing memory**, possibly combined with the `allVals` concatenation allocating a 100M × 8 = 800MB temporary array.
