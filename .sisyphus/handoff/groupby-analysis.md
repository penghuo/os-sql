# GROUP BY High-Cardinality Analysis

## 1. Hash Map Implementations (FusedGroupByAggregate.java)

Four specialized open-addressing hash maps, all with `MAX_CAPACITY=32M`, `LOAD_FACTOR=0.7`, linear probing:

| Map | Keys | Storage | Used For |
|-----|------|---------|----------|
| `FlatSingleKeyMap` (:13891) | 1 long | `long[] accData` contiguous | Q15 (GROUP BY UserID) |
| `FlatTwoKeyMap` (:13549) | 2 longs | `long[] accData` contiguous | Q16 (GROUP BY UserID, SearchPhrase) |
| `FlatThreeKeyMap` (:13735) | 3 longs | `long[] accData` contiguous | Q18 (GROUP BY UserID, minute, SearchPhrase) |
| `TwoKeyHashMap` (:13321) | 2 longs | `AccumulatorGroup[]` objects | Fallback for MIN/MAX/DISTINCT |
| `SingleKeyHashMap` (:13438) | 1 long | `AccumulatorGroup[]` objects | Fallback for MIN/MAX/DISTINCT |

**Key bottleneck**: All Flat maps store accumulators as `long[]` at `slot * slotsPerGroup`. With 17M unique UserIDs, the hash map arrays alone consume:
- `FlatSingleKeyMap`: 17M/0.7 ≈ 24M slots → keys0(192MB) + accData(192MB+) = ~400MB+
- `FlatTwoKeyMap`: even larger with keys0 + keys1 + accData

This far exceeds L3 cache (typically 30-50MB), causing heavy cache misses during probing.

**Hash functions**: Murmur3-inspired mixing (`hash2`, `hash3` at :13422, :13762).

## 2. Coordinator Merge (ResultMerger.java + TransportTrinoSqlAction.java)

Three merge paths in `TransportTrinoSqlAction.java` (:414-465):

1. **Fused merge+sort** (`merger.mergeAggregationAndSort`) — when ORDER BY + LIMIT exists, no HAVING
2. **Capped merge** (`merger.mergeAggregationCapped`) — LIMIT without ORDER BY (any N groups valid)
3. **Fallback** (`merger.mergeAggregation` + `applyCoordinatorSort`) — full materialization

`ResultMerger.mergeAggregationAndSort` (:71-146) dispatches to:
- `mergeSingleKeyNumericCountWithSort` — 1 numeric key, 1 agg
- `mergeSingleKeyMultiAggNumericWithSort` — 1 numeric key, N aggs
- `mergeAggregationFastNumericWithSort` — N numeric keys
- `mergeAggregationFastVarcharWithSort` — varchar keys
- `mergeAggregationFastMixedWithSort` — mixed keys

**All use HashMap full materialization + PriorityQueue top-N selection** (:809):
```java
// Build full HashMap from ALL shard results first
// Then: O(n log k) top-N via bounded max-heap
PriorityQueue<Integer> heap = new PriorityQueue<>(k + 1, slotComparator.reversed());
for (int s = 0; s < fCapacity; s++) {
    if (fMapOccupied[s]) { heap.offer(s); if (heap.size() > k) heap.poll(); }
}
```
**No merge-sort streaming** — all groups from all shards are fully materialized into coordinator HashMap before top-N selection.

## 3. Shard-Level Top-N Pruning

**Yes, exists** for ORDER BY + LIMIT queries via `executeWithTopN()` (:1249):
- Shard executor passes `topN` parameter through to `FusedGroupByAggregate`
- For single-varchar COUNT(*), uses min/max heap directly on ordinal counts (:1725)
- For LIMIT-without-ORDER-BY, uses `findOrInsertCapped()` (:13617) to stop accepting new groups

**BUT**: Top-N pruning at shard level is **not exact** — each shard returns its local top-N, coordinator must re-merge. For Q15/Q16/Q18 without ORDER BY, the shard still builds the full hash map for all groups.

**No PriorityQueue-based streaming pruning during aggregation** — the heap is applied post-aggregation.

## 4. Q32 vs Q33/Q34 Differences

- **Q32** (2-key numeric GROUP BY with COUNT/SUM/AVG): Routes to `executeTwoKeyNumericFlat` (:6478) using `FlatTwoKeyMap` with contiguous `long[] accData`. Multiple aggs stored at `slot * slotsPerGroup` — e.g., COUNT=1 slot, SUM=1 slot, AVG=2 slots (sum+count). No per-group object allocation.

- **Q33/Q34** (single VARCHAR key GROUP BY with ORDER BY + LIMIT): Routes to `executeSingleVarcharCountStar` (:1633) or `executeSingleVarcharGeneric` (:2310). Uses ordinal-indexed arrays for single-segment, `HashMap<BytesRefKey, long[]>` for multi-segment. Has inline top-N heap selection (:1725) that avoids resolving ordinals for non-top groups.

**Key difference**: Q32 uses flat numeric maps (no ordinal resolution needed), Q33/Q34 use ordinal-based varchar paths with top-N pruning. Q32 cannot benefit from top-N pruning because it has no ORDER BY + LIMIT in the shard plan.

## Summary of Bottlenecks for Q15/Q16/Q18

1. **Hash map cache pressure**: 17M unique keys → ~400MB+ working set, far exceeding L3 cache
2. **Full materialization at coordinator**: All groups merged into HashMap before any filtering
3. **No shard-level cardinality reduction**: Without ORDER BY, shards return ALL groups
4. **Parallel merge overhead**: `FlatThreeKeyMap.mergeFrom()` (:13845) uses batched prefetch (BATCH=32) but still touches all entries
5. **Resize storms**: Starting at 8192 capacity, reaching 17M requires ~11 doublings with full rehash each time
