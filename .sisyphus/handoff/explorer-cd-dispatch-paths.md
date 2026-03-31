# COUNT(DISTINCT) Execution Paths Analysis

## Query Dispatch Summary

### Q04: COUNT(DISTINCT UserID) — Scalar Numeric
- **Shard dispatch**: `TransportShardExecuteAction.java:270` → `isBareSingleNumericColumnScan()` → `executeDistinctValuesScanWithRawSet()` (line 3025)
- **Shard execution**: `FusedScanAggregate.collectDistinctValuesRaw()` (line 1674)
  - Two-phase parallel: Phase 1 loads DocValues into flat `long[]` per segment (parallel), Phase 2 inserts into per-segment `LongOpenHashSet` with run-length dedup, then merges largest-first
  - Filtered queries fall back to Collector-based single-pass
- **Coordinator merge**: `TransportTrinoSqlAction.java:654` → `mergeCountDistinctValuesViaRawSets()` (line 2017)
  - Unions raw `LongOpenHashSet` attachments across shards (largest-first merge)
- **Bottleneck**: Hash set insertion in Phase 2. ~200K distinct UserIDs across ~100M rows. Run-length dedup on sorted segments helps skip consecutive duplicates.

### Q05: COUNT(DISTINCT SearchPhrase) — Scalar VARCHAR
- **Shard dispatch**: `TransportShardExecuteAction.java:278` → `isBareSingleVarcharColumnScan()` → `executeDistinctValuesScanVarcharWithRawSet()` (line 3055)
- **Shard execution**: `FusedScanAggregate.collectDistinctVarcharHashes()` (line 1862)
  - MatchAll + no deletes: iterates global ordinals (O(distinct_values)), hashes BytesRef via FNV-1a into `LongOpenHashSet`
  - Parallel: splits ordinal range across workers, each builds local `LongOpenHashSet`, then merges
  - With deletes: per-segment FixedBitSet ordinal collection, then hash unique ordinals
- **Coordinator merge**: `TransportTrinoSqlAction.java:658` → `mergeCountDistinctVarcharViaRawSets()` (line 2107)
  - Hash-based merge: unions `LongOpenHashSet` of hashes across shards
- **Key difference from Q04**: Uses hash-based approximation (FNV-1a on BytesRef) instead of exact values. Avoids String materialization entirely. Risk: hash collisions could undercount (64-bit FNV-1a collision probability is negligible for practical cardinalities).
- **Bottleneck**: `ordinalMap.getFirstSegmentNumber(g)` + `lookupOrd(segOrd)` per global ordinal. For high-cardinality SearchPhrase, this is O(distinct_values) BytesRef lookups.

### Q08: GROUP BY RegionID + COUNT(DISTINCT UserID) — 2-Key Numeric
- **Shard dispatch**: `TransportShardExecuteAction.java:292-330` → `executeCountDistinctWithHashSets()` (line 1001)
  - Condition: 2 keys, both non-VARCHAR, single COUNT(*) aggregate
- **Shard execution**: `scanSegmentForCountDistinct()` (line 1134)
  - Open-addressing hash map: `long[] grpKeys` + `LongOpenHashSet[] grpSets` + `boolean[] grpOcc`
  - MatchAll: loads both columns via `FusedGroupByAggregate.loadNumericColumn()` into flat arrays, then iterates with inline hash map probing
  - Parallel: each segment builds its own map, then merged across segments
- **Coordinator merge**: `TransportTrinoSqlAction.java:700` → `mergeDedupCountDistinctViaSets()` (line 2522)
  - Per-group `LongOpenHashSet` union with lazy top-K pruning when ORDER BY LIMIT present
  - `countMergedGroupSets()` (line 2661): finds largest set, collects extras from smaller sets
- **Bottleneck**: Per-doc hash map probe (group lookup) + per-doc `LongOpenHashSet.add()`. For ~400 RegionIDs × ~200K UserIDs, the per-group sets are the memory/compute cost.

### Q09: GROUP BY RegionID + mixed aggs + COUNT(DISTINCT) — Mixed Dedup
- **Shard dispatch**: `TransportShardExecuteAction.java:380` → `executeMixedDedupWithHashSets()` (line 1933)
  - Condition: 2 keys, both non-VARCHAR, all aggs are SUM/COUNT (decomposable), `isMixedDedup=true`
- **Shard execution**: Same open-addressing pattern as Q08 but with additional `long[][] grpAccs` for SUM/COUNT accumulators per group
  - Single pass: for each doc, probe group map, add to HashSet for dedup key, accumulate SUM/COUNT values
- **Coordinator merge**: `TransportTrinoSqlAction.java:718` → `mergeMixedDedupViaSets()`
  - Merges both HashSets (for COUNT DISTINCT) and accumulator values (SUM/COUNT) across shards
- **Bottleneck**: Same as Q08 plus additional accumulator updates per doc. The HashSet insertion dominates.

### Q11: GROUP BY MobilePhone,Model + COUNT(DISTINCT UserID) — Multi-Key Mixed-Type
- **Shard dispatch**: `TransportShardExecuteAction.java:357` → `executeMixedTypeCountDistinctWithHashSets()` (line 1651)
  - Condition: 3+ keys, last key (dedup) is numeric, some GROUP BY keys are VARCHAR
  - Uses `ObjectArrayKey` composite keys (String-based) for the group map
- **Shard execution**: `scanSegmentForMixedTypeCountDistinct()`
  - Per-segment: resolves VARCHAR keys via `SortedSetDocValues.lookupOrd()` to String, builds `HashMap<ObjectArrayKey, LongOpenHashSet>`
  - Cross-segment merge via String equality (not ordinals)
- **Coordinator merge**: Falls through to generic `mergeDedupCountDistinct()` (page-based merge, not HashSet-based)
  - Outputs full dedup tuples (all N keys + COUNT(*)=1) for coordinator's generic aggregation
- **Bottleneck**: String materialization per doc for VARCHAR group keys. `ObjectArrayKey` hashing/equality is more expensive than primitive long keys. No HashSet-based coordinator fast path — falls to generic merge.

### Q13: GROUP BY SearchPhrase + COUNT(DISTINCT UserID) — VARCHAR Key + Numeric Dedup
- **Shard dispatch**: `TransportShardExecuteAction.java:333` → `executeVarcharCountDistinctWithHashSets()` (line 2412)
  - Condition: 2 keys, key0 is VARCHAR, key1 is numeric
- **Shard execution**: Global ordinals path when available
  - `LongOpenHashSet[]` indexed by global ordinal (one set per unique SearchPhrase)
  - Parallel: workers scan assigned segments, map segment ordinals to global ordinals via `OrdinalMap`, insert numeric values into ordinal-indexed sets
  - String resolution deferred to output phase (only for non-empty groups)
- **Coordinator merge**: `TransportTrinoSqlAction.java:673` → `mergeDedupCountDistinctViaVarcharSets()`
  - String-keyed merge: `HashMap<String, List<LongOpenHashSet>>` across shards
- **Bottleneck**: Global ordinal mapping per doc (`segToGlobal.get(segOrd)`). For high-cardinality SearchPhrase, the `LongOpenHashSet[]` array can be very large (10M+ entries). Coordinator merge requires String-keyed HashMap.

## Data Structures

| Structure | File | Purpose |
|-----------|------|---------|
| `LongOpenHashSet` | `operator/LongOpenHashSet.java` | Open-addressing hash set for primitive longs. Murmur3 finalizer, 0.65 load factor, sentinel-based empty detection. |
| `SliceRangeHashSet` | `operator/SliceRangeHashSet.java` | Hash set operating on raw byte ranges from VariableWidthBlock. Used for VARCHAR scalar COUNT DISTINCT coordinator merge. |
| `DataFusionBridge.groupByCountDistinct` | `datafusion/DataFusionBridge.java:60` | Native (Rust) JNI for grouped COUNT DISTINCT. **Currently unused** — no callers found. |

## Architecture Summary

All 6 queries follow the same 3-stage pattern:
1. **Plan detection** in `TransportShardExecuteAction` (lines 265-400): pattern-matches the plan tree to select the optimal shard execution path
2. **Shard execution**: DocValues-based scan with per-group `LongOpenHashSet` accumulators, parallel across segments
3. **Coordinator merge** in `TransportTrinoSqlAction`: unions per-shard HashSets, with lazy top-K pruning for ORDER BY LIMIT queries

## Key Observations

1. **No TODO/FIXME/OPTIMIZE comments** found in any COUNT DISTINCT path — the code appears to be in a mature optimization state
2. **DataFusionBridge.groupByCountDistinct** is declared but has zero callers — potential unused native path
3. **Q05 uses hash approximation** (FNV-1a) while Q04 uses exact values — different accuracy guarantees
4. **Q11 is the slowest path** — falls to generic merge, uses String-based composite keys, no HashSet coordinator fast path
5. **Q13 uses global ordinals** to avoid per-doc String materialization — clever optimization but the ordinal-indexed `LongOpenHashSet[]` array can be huge for high-cardinality keys
6. **Run-length dedup** in Q04's Phase 2 (line 1781 of FusedScanAggregate) exploits index sort order to skip consecutive duplicate UserIDs before hash insertion
7. **Coordinator merge** uses "largest-first" strategy everywhere: find the biggest set, merge smaller sets into an extras set, return `largest.size() + extras.size()` — avoids mutating the largest set
