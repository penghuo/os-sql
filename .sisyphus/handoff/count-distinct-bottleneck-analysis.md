# COUNT(DISTINCT) Bottleneck Analysis

## Summary Table

| Query | Method | Parallel? | Data Structure | Key Bottleneck |
|-------|--------|-----------|----------------|----------------|
| Q04 (2.15s) | `collectDistinctValuesRaw` in FusedScanAggregate:1511 | **NO** — sequential across all segments | Single shared `LongOpenHashSet` | Sequential scan of ~100M rows into one giant HashSet; comment says parallel was 4.3x SLOWER due to merge cost |
| Q05 (3.96s) | `collectDistinctStringsRaw` in FusedScanAggregate:1588 | **YES** — parallel ordinal iteration per segment | Per-worker `HashSet<String>` merged via `addAll()` | `lookupOrd().utf8ToString()` per ordinal + `HashSet<String>` boxing + String merge across workers |
| Q08 (2.40s) | `executeCountDistinctWithHashSets` in Transport:982 | **YES** — parallel per-segment via ForkJoinPool + CountDownLatch | Per-segment `Map<Long, LongOpenHashSet>`, merged | Per-group `LongOpenHashSet` allocation (new set per group per segment); merge iterates full backing arrays |
| Q09 (3.51s) | `executeMixedDedupWithHashSets` in Transport:1872 | **YES** — parallel via CompletableFuture + greedy segment assignment | Per-worker open-addressing map (long[] + LongOpenHashSet[] + long[][] accumulators) | Complex inner loop: regex-parsed agg functions, multiple DocValues advances per doc, per-worker map merge |
| Q13 (7.50s) | `executeVarcharCountDistinctWithHashSets` in Transport:2351 | **YES** — parallel via CompletableFuture + greedy segment assignment | Per-segment ordinal-indexed `LongOpenHashSet[]`, merged into `Map<String, LongOpenHashSet>` | `mergeOrdSetsIntoMap` does `lookupOrd().utf8ToString()` per ordinal + String HashMap merge; high cardinality SearchPhrase creates many small HashSets |

---

## Detailed Findings

### Q04: `collectDistinctValuesRaw` (FusedScanAggregate.java:1511)
- **Scan**: Sequential `nextDoc()` loop across ALL segments into a single `LongOpenHashSet`
- **Pre-sizing**: `Math.min(totalDocs, 32_000_000)` — good, avoids resize storms
- **Why not parallel**: Comment at line 1530 says "parallel with merge was 4.3x SLOWER due to LongOpenHashSet merge cost" — the `addAll()` method iterates the entire backing array (including empty slots at 65% load factor → 35% wasted iteration)
- **Optimization opportunity**: 
  - **COUNT-only shortcut**: For scalar `COUNT(DISTINCT col)`, we don't need the actual set — just the count. Could use HyperLogLog for approximate, or a **RoaringBitmap** if values fit in int range (UserID likely does). RoaringBitmap merge is O(compressed size), not O(capacity).
  - **Parallel with bitset merge**: If values are in a bounded range, use per-segment `FixedBitSet` or `RoaringBitmap`, then OR-merge (cache-line friendly, no hash probing). Merge cost is O(bitmap_size), not O(capacity/load_factor).
  - **Parallel with count-only merge**: Each segment builds its own LongOpenHashSet, but instead of merging sets, use the "largest + contains-check" strategy already used at coordinator level (TransportTrinoSqlAction:1988). Count = largest.size() + count of entries in others not in largest.

### Q05: `collectDistinctStringsRaw` (FusedScanAggregate.java:1588)
- **Scan**: Parallel ordinal iteration — each worker iterates `dv.lookupOrd(ord)` for all ordinals in its segments
- **Key cost**: `lookupOrd(ord).utf8ToString()` creates a new String object per ordinal per segment. Same string in multiple segments → duplicate String allocations.
- **Merge**: `distinctStrings.addAll(future.join())` — HashSet.addAll does hash + equals on every String
- **Optimization opportunity**:
  - **Ordinal-only counting**: For COUNT(DISTINCT), don't materialize strings at all within a shard. Use `FixedBitSet` of ordinals per segment, OR-merge across segments, then count set bits. Only resolve strings if needed for output (not needed for scalar COUNT).
  - **Global ordinal dedup**: If using SortedSetDocValues, ordinals are segment-local. But for a single shard, we could build a global ordinal mapping and use bitset operations instead of String hashing.
  - **Avoid utf8ToString()**: Keep BytesRef and use BytesRef-based HashSet to avoid UTF-8 decode + String allocation overhead.

### Q08: `executeCountDistinctWithHashSets` (Transport:982)
- **Scan**: Parallel per-segment via ForkJoinPool with CountDownLatch
- **Inner loop** (scanSegmentForCountDistinct at line 1115): Custom open-addressing map (long[] keys + LongOpenHashSet[] values + boolean[] occupied) with linear probing
- **Per-doc cost**: Two `nextDoc()` calls (key0, key1), hash probe for group lookup, `LongOpenHashSet.add(k1)`
- **Merge**: Iterates all segment maps, merges LongOpenHashSets per group key — uses "merge smaller into larger" heuristic (good)
- **Optimization opportunity**:
  - **Pre-sized LongOpenHashSet per group**: Currently `new LongOpenHashSet()` uses default capacity 8. For high-cardinality key1 (UserID), this causes many resizes. Could estimate cardinality from segment doc count.
  - **Bitset per group**: If key1 (UserID) values fit in a bounded int range, use RoaringBitmap per group instead of LongOpenHashSet. OR-merge is much cheaper.

### Q09: `executeMixedDedupWithHashSets` (Transport:1872)
- **Scan**: Parallel via CompletableFuture with greedy segment assignment
- **Inner loop**: Per-doc: read key0 DV, read key1 DV, read N aggregate argument DVs, hash-probe group map, add to LongOpenHashSet + accumulate SUM/COUNT values
- **Overhead**: Regex parsing of aggregate functions (`Pattern.compile` per agg) — done once before loop, not per-doc (OK)
- **Merge**: Complex — must merge both LongOpenHashSets AND accumulator arrays per group across workers
- **Optimization opportunity**:
  - Same as Q08: pre-size HashSets, consider bitsets for bounded key1 ranges
  - **Fuse accumulator merge**: Currently merges maps then separately handles accumulators. Could use a single pass.

### Q13: `executeVarcharCountDistinctWithHashSets` (Transport:2351) — **SLOWEST at 7.5s**
- **Scan**: Parallel via CompletableFuture. Uses ordinal-indexed `LongOpenHashSet[]` per segment (good — avoids String hashing during scan)
- **Inner loop**: `sdv.nextDoc()` → `sdv.ordValue()` → `ordSets[ord].add(numericValue)`. Uses `loadNumericColumn()` to pre-load numeric values into array for O(1) access (good optimization).
- **Critical bottleneck — `mergeOrdSetsIntoMap`** (line 2629): After each segment scan, converts ordinal-indexed sets to String-keyed map:
  - Calls `dv.lookupOrd(ord).utf8ToString()` for every non-null ordinal
  - Does `HashMap.get(key)` + `HashMap.put(key, set)` or merge
  - For high-cardinality SearchPhrase (potentially millions of unique values), this creates millions of String objects and HashMap entries
- **Why 7.5s**: SearchPhrase has very high cardinality → millions of ordinals per segment → millions of `lookupOrd` + `utf8ToString` + HashMap operations per worker, then cross-worker merge does it again
- **Optimization opportunities**:
  - **Defer string resolution**: Keep ordinal-indexed arrays through the merge phase. Build a global ordinal→string mapping only once at the end. Currently each worker resolves ordinals independently → duplicate String creation.
  - **Ordinal remapping**: Since ordinals are segment-local, build a segment→global ordinal mapping (using the SortedSetDocValues merge infrastructure), then merge using global ordinal-indexed arrays instead of String HashMap.
  - **Count-only shortcut**: For `GROUP BY SearchPhrase + COUNT(DISTINCT UserID)`, the final output needs (SearchPhrase, count). Could use ordinal-indexed `int[]` counts instead of full LongOpenHashSets if we can compute distinct counts without materializing the sets (e.g., using HyperLogLog per group for approximate counts).
  - **Reduce LongOpenHashSet overhead**: Many groups may have very few distinct UserIDs. For groups with ≤4 values, a simple sorted long[4] array with linear scan would be faster than hash set overhead.

---

## Top Optimization Recommendations (Ranked by Impact)

### 1. Q13 (7.5s → est. 2-3s): Defer String Resolution in mergeOrdSetsIntoMap
**File**: TransportShardExecuteAction.java:2629
**Problem**: Each worker calls `lookupOrd().utf8ToString()` for every ordinal, creating duplicate Strings across workers.
**Fix**: Keep ordinal-indexed `LongOpenHashSet[]` arrays per worker. After all workers complete, resolve ordinals to strings only once using the first segment's DocValues (or build a merged ordinal map). This eliminates N-1 redundant string resolutions where N = number of workers.

### 2. Q04 (2.15s → est. 0.8-1.2s): Parallel Scan with Count-Only Merge
**File**: FusedScanAggregate.java:1511
**Problem**: Sequential scan because LongOpenHashSet merge is expensive.
**Fix**: Use per-segment LongOpenHashSets (parallel scan), then use the "largest + contains-check" merge strategy (already implemented at coordinator in TransportTrinoSqlAction:1988) instead of `addAll()`. This avoids iterating empty slots. Alternatively, use RoaringBitmap if UserID fits in int range — bitmap OR-merge is O(compressed_size).

### 3. Q05 (3.96s → est. 1.5-2s): Ordinal-Only Counting (Skip String Materialization)
**File**: FusedScanAggregate.java:1588
**Problem**: Creates String objects for every ordinal just to count distinct values.
**Fix**: For scalar COUNT(DISTINCT varchar_col), use `FixedBitSet` of ordinals per segment, OR-merge across segments, then `bitSet.cardinality()`. No String allocation needed. Only works within a single shard (ordinals are segment-local), but FixedBitSet OR handles segment-local ordinals if we track per-segment ordinal spaces.

### 4. Q08/Q09 (2.4s/3.5s): Pre-size LongOpenHashSet per Group
**File**: TransportShardExecuteAction.java:1160 (scanSegmentForCountDistinct)
**Problem**: `new LongOpenHashSet()` defaults to capacity 8, causing many resizes for high-cardinality groups.
**Fix**: Estimate per-group cardinality as `segment_doc_count / estimated_group_count` and pre-size. Even a rough estimate (e.g., 256 or 1024) would eliminate most resize operations.

### 5. Cross-cutting: Small-Group Optimization for LongOpenHashSet
**File**: LongOpenHashSet.java
**Problem**: For groups with ≤4 distinct values, LongOpenHashSet overhead (8-slot array, hash computation, load factor checks) is excessive.
**Fix**: Add a "tiny mode" — store up to 4 values in a fixed long[4] with linear scan, upgrade to hash mode only when exceeding 4. This benefits Q13 heavily where many SearchPhrase groups have few distinct UserIDs.
