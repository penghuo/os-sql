# Q09 & Q11 Dispatch Analysis

## TL;DR
Both Q09 and Q11 DO hit the fused DocValues fast path. They are NOT missing fast paths.
The slowness comes from per-group `CountDistinctAccum` (HashSet) allocation overhead,
which prevents the flat-accumulator optimization used by non-DISTINCT queries.

## Q09 Dispatch Trace
**Query**: `GROUP BY RegionID` + SUM + COUNT(*) + AVG + COUNT(DISTINCT UserID)

1. **LogicalPlanner** → `AggregationNode(PARTIAL, groupBy=[RegionID], aggs=[SUM(AdvEngineID), COUNT(*), AVG(ResolutionWidth), COUNT(DISTINCT UserID)])`
2. **PlanFragmenter.buildShardPlan** → Step=PARTIAL, `buildShardPlanWithInflatedLimit` succeeds (sort key `COUNT(*)` is an agg column). Shard plan: `LimitNode(inflated) → SortNode → AggregationNode(PARTIAL, same aggs)`
   - **Key issue**: The mixed-dedup path (`buildMixedDedupShardPlan`) is ONLY for SINGLE step, never reached for PARTIAL. So COUNT(DISTINCT UserID) stays as-is in the shard plan — no dedup decomposition.
3. **TransportShardExecuteAction** → HashSet fast path (line 277) requires `effectivePlan instanceof AggregationNode` — skipped because plan root is LimitNode. Falls through to sorted-limit path (line 369).
4. **Sorted-limit path** → `extractAggFromSortedLimit` extracts inner agg, `canFuse` returns true, sort key resolves. Calls `executeFusedGroupByAggregateWithTopN`.
5. **FusedGroupByAggregate.executeInternal** → single numeric key, no VARCHAR → `executeNumericOnly` → `executeSingleKeyNumeric`.
6. **executeSingleKeyNumeric** → COUNT(DISTINCT) sets `accType[i]=5`, which sets `canUseFlatAccumulators=false`. Falls to `SingleKeyHashMap` path with per-group `AccumulatorGroup` + `CountDistinctAccum` objects.

**Performance bottleneck**: The flat accumulator path (`executeSingleKeyNumericFlat`) uses a `long[]` array per hash slot — zero per-group object allocation. But COUNT(DISTINCT) requires a `LongOpenHashSet` per group, forcing the object-based path. For ~230 RegionID groups × ~17M distinct UserIDs, each group's HashSet grows large.

## Q11 Dispatch Trace
**Query**: `GROUP BY MobilePhone, MobilePhoneModel` + COUNT(DISTINCT UserID) + WHERE filter

1. **LogicalPlanner** → `AggregationNode(PARTIAL, groupBy=[MobilePhone, MobilePhoneModel], aggs=[COUNT(DISTINCT UserID)])`
2. **PlanFragmenter** → `buildShardPlanWithInflatedLimit` succeeds (sort key `COUNT(DISTINCT UserID)` at index 2 >= groupByKeys.size()=2).
3. **TransportShardExecuteAction** → Same as Q09: HashSet fast path skipped (LimitNode root), sorted-limit path dispatches to `executeFusedGroupByAggregateWithTopN`.
4. **FusedGroupByAggregate.executeInternal** → 2 VARCHAR keys → `executeWithVarcharKeys`.
5. **executeWithVarcharKeys** → Uses ordinal-based per-segment aggregation with `AccumulatorGroup` containing `CountDistinctAccum` per group.

**Performance bottleneck**: Two VARCHAR keys means `MergedGroupKey` (BytesRef pairs) as hash keys + per-group `CountDistinctAccum`. The WHERE filter reduces rows but the GROUP BY cardinality (MobilePhone × MobilePhoneModel) can still be high.

## Root Cause Summary

| Aspect | Q09 | Q11 |
|--------|-----|-----|
| Fused path? | ✅ executeSingleKeyNumeric | ✅ executeWithVarcharKeys |
| Flat accumulators? | ❌ (COUNT(DISTINCT) blocks it) | ❌ (COUNT(DISTINCT) blocks it) |
| Per-group overhead | AccumulatorGroup + LongOpenHashSet | MergedGroupKey + AccumulatorGroup + LongOpenHashSet |
| Dedup at shard? | ❌ (PARTIAL step skips buildMixedDedupShardPlan) | ❌ (PARTIAL step skips dedup) |

The core issue: `PlanFragmenter.buildShardPlan` only applies dedup decomposition for SINGLE-step aggregations (line 148). Since `LogicalPlanner` always emits PARTIAL step for GROUP BY queries (line 569), the mixed-dedup and pure-dedup paths in PlanFragmenter are dead code for these queries. The shard plan retains `COUNT(DISTINCT UserID)` as a raw aggregate, forcing per-group HashSet allocation in the fused path.
