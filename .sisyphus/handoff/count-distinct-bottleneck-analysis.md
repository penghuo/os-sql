# COUNT(DISTINCT) Execution Path Bottleneck Analysis

## Query → Dispatch Path Mapping

| Query | Pattern | Dispatch Path | Key Method |
|-------|---------|---------------|------------|
| Q04 | `COUNT(DISTINCT UserID)` scalar numeric | `isBareSingleNumericColumnScan` → `executeDistinctValuesScanWithRawSet` | `FusedScanAggregate.collectDistinctValuesRaw()` :1505 |
| Q05 | `COUNT(DISTINCT SearchPhrase)` scalar varchar | `isBareSingleVarcharColumnScan` → `executeDistinctValuesScanVarcharWithRawSet` | `FusedScanAggregate.collectDistinctStringsRaw()` :1576 |
| Q08 | `GB RegionID + COUNT(DISTINCT UserID)` | 2-key numeric dedup → `executeCountDistinctWithHashSets` | `scanSegmentForCountDistinct()` :1089 |
| Q09 | `GB RegionID + SUM/COUNT/AVG/COUNT(DISTINCT)` | 2-key mixed dedup → `executeMixedDedupWithHashSets` | :1537 |
| Q11 | `GB MobilePhone,MobilePhoneModel + COUNT(DISTINCT UserID)` | 3-key with 2 VARCHAR → **falls through to generic FusedGroupBy** | `FusedGroupByAggregate` (no specialized dedup) |
| Q13 | `GB SearchPhrase + COUNT(DISTINCT UserID)` | VARCHAR key0 + numeric key1 → `executeVarcharCountDistinctWithHashSets` | :2011 |

## Bottleneck #1: LongOpenHashSet Resize Storm (Q04, Q08, Q09)

**File**: `operator/LongOpenHashSet.java`
- Initial capacity: **8** (default constructor used in `collectDistinctValuesRaw` at FusedScanAggregate:1507)
- Load factor: 0.65
- For Q04 (~18M distinct UserIDs): resizes **21 times** (8→16→32→...→33M)
- Each resize: allocates new array, fills with EMPTY sentinel, rehashes ALL existing entries
- Resize at 18M entries: allocates 33M×8 = **264MB** array, rehashes 18M entries with Murmur3

**Cost estimate**: For 18M distinct values, total rehash work ≈ 8+16+32+...+18M ≈ **36M hash operations** just from resizing, on top of the 18M insert hashes.

**Fix**: Pre-size the HashSet. The shard knows `reader.maxDoc()` before scanning — use `new LongOpenHashSet(maxDoc)` or a fraction thereof as initial capacity hint.

## Bottleneck #2: Per-Doc Hash Computation (Q04, Q08)

**File**: `operator/LongOpenHashSet.java:84-98`
- Every `add()` call computes full Murmur3 finalizer (3 multiplies, 3 XOR-shifts)
- For Q04: ~100M docs across all segments, each doing hash + probe
- For Q08: ~100M docs, each doing TWO hash lookups (group key + dedup key)

**The hash itself is not the bottleneck** — it's the **cache misses** during probing. At 18M entries with 0.65 load factor, the backing array is ~28M longs = **224MB**. This far exceeds L3 cache, making every probe a potential cache miss.

## Bottleneck #3: Sequential Single-Shard Scan (Q04)

**File**: `FusedScanAggregate.java:1515-1516`
```java
// Benchmarked: parallel with merge was 4.3x SLOWER due to LongOpenHashSet merge cost.
for (LeafReaderContext leafCtx : engineSearcher.getIndexReader().leaves()) {
```
- Q04 scans ALL segments sequentially into ONE HashSet
- Parallel was tried but merge cost dominated (merging two 18M-entry sets = scanning 28M-slot array)
- **This is the fundamental issue**: can't parallelize because merge is O(capacity), not O(size)

## Bottleneck #4: Q11 Falls to Generic Path (12x slowdown)

**File**: `TransportShardExecuteAction.java:325-335`
- Q11 has 3 keys: MobilePhone(VARCHAR), MobilePhoneModel(VARCHAR), UserID(numeric)
- The N-key dedup path requires `allNumeric = true` (line 330)
- Since 2 keys are VARCHAR, it falls through to generic `FusedGroupByAggregate`
- Generic path uses `CountDistinctAccum` with per-group `LongOpenHashSet(16)` — better initial size but still hash-based

## Bottleneck #5: No Bitmap/Ordinal Approach for Numeric Columns

- VARCHAR path uses `FixedBitSet` on ordinals (efficient — 1 bit per unique value)
- **Numeric columns have NO ordinal-based approach** — `SortedNumericDocValues` doesn't expose ordinals like `SortedSetDocValues`
- No RoaringBitmap or any compressed bitmap in the codebase
- The only dedup mechanism for numeric columns is `LongOpenHashSet`

## Bottleneck #6: Cross-Shard HashSet Transport (Q04)

**File**: `ShardExecuteResponse.java:40-58`
- Distinct sets are `transient` — only work for local execution
- For Q04: each shard builds ~18M-entry LongOpenHashSet, coordinator unions them
- Union cost: iterating the full backing array (28M slots) per shard, probing into target set

## Architecture Summary

```
Q04 path:
  Coordinator → N shards (parallel via dqe-shard-executor pool, size=cpus/2)
    Each shard: sequential segment scan → single LongOpenHashSet (18M entries, 224MB)
  Coordinator: union N sets → count

Q08 path:
  Coordinator → N shards (parallel)
    Each shard: parallel segment scan (ForkJoinPool) → per-segment Map<RegionID, LongOpenHashSet>
      → merge segment maps (union HashSets per group)
  Coordinator: union per-group HashSets across shards → count per group
```

## Potential Optimizations

1. **Pre-size LongOpenHashSet**: Use `new LongOpenHashSet(estimatedDistinct)` based on segment metadata. Eliminates 21 resize+rehash cycles for Q04.

2. **Two-pass approach for Q04**: First pass counts distinct values using HyperLogLog (~12 bytes), second pass pre-sizes the HashSet. Or use sorted DocValues: if the column is sorted, distinct count = iterate and count changes.

3. **Ordinal-based dedup for numeric columns**: If UserID values fit in a known range, use a `FixedBitSet(maxValue - minValue)` instead of HashSet. Check `PointValues.getMinPackedValue/getMaxPackedValue` for range.

4. **Q11 VARCHAR multi-key dedup**: Add a specialized path for VARCHAR+VARCHAR+numeric 3-key dedup using ordinal-indexed arrays (like the VARCHAR single-key path already does).

5. **Parallel-friendly dedup**: Instead of merging HashSets, use per-segment FixedBitSets (if value range allows) that can be OR'd together in O(words) instead of O(capacity) with rehashing.
