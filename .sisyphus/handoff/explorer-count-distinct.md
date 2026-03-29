# COUNT(DISTINCT) Execution Path in DQE Engine

## 1. Plan Compilation & Step Assignment

**PlanOptimizer** (`PlanOptimizer.java:371-378`): `hasNonDecomposableAgg()` detects `COUNT(DISTINCT` in aggregate functions and forces `AggregationNode.Step.SINGLE`. This means shards do NOT run partial aggregation — they scan raw data.

**PlanFragmenter** (`PlanFragmenter.java:144-157`): For SINGLE step with GROUP BY + all-COUNT(DISTINCT) aggregates, builds a **dedup shard plan**:
- Original: `GROUP BY (SearchPhrase) COUNT(DISTINCT UserID)` 
- Shard plan: `GROUP BY (SearchPhrase, UserID) COUNT(*)` with `Step.PARTIAL`
- Coordinator plan: `AggregationNode(SINGLE)` with original functions

This dedup strategy reduces cross-shard data by eliminating duplicate (group_key, distinct_value) pairs per shard.

## 2. Shard Execution Dispatch

**TransportShardExecuteAction** (`TransportShardExecuteAction.java:379-385`): The dedup shard plan is a `PARTIAL AggregationNode` with expanded GROUP BY keys. It hits the generic `FusedGroupByAggregate.canFuse()` check and routes to `executeFusedGroupByAggregate()`.

### Q13 Path (1 VARCHAR key: SearchPhrase + UserID)
- Dedup keys: `[SearchPhrase, UserID]` → 1 VARCHAR key + 1 numeric key
- Dispatch: `executeInternal()` → `hasVarchar=true` → `executeWithVarcharKeys()` (`FusedGroupByAggregate.java:1100`)
- Aggregation: `COUNT(*)` only (accType=0), so `canUseFlatAccumulators=true` for the dedup plan
- Data structure: flat `long[]` accumulators per ordinal group (no CountDistinctAccum at shard level)

### Q11 Path (2 keys: MobilePhone numeric + MobilePhoneModel varchar + UserID)
- Dedup keys: `[MobilePhone, MobilePhoneModel, UserID]` → has VARCHAR → `executeWithVarcharKeys()` (`FusedGroupByAggregate.java:1100`)
- Aggregation: `COUNT(*)` only, `canUseFlatAccumulators=true` for the dedup plan

**Key insight**: Because PlanFragmenter decomposes COUNT(DISTINCT) into dedup GROUP BY + COUNT(*), the shard-level execution NEVER creates `CountDistinctAccum` objects. The accType=5 / `canUseFlatAccumulators=false` path is only relevant if the aggregation node retains `COUNT(DISTINCT)` directly (which doesn't happen for GROUP BY queries due to the dedup rewrite).

## 3. Per-Group Data Structures (Shard Level)

Since the shard plan is `GROUP BY (original_keys + distinct_col) COUNT(*)`:
- **No HashSet/LongOpenHashSet per group** at shard level
- Uses flat `long[]` accumulators (count only) or `CountStarAccum` depending on path
- For Q13 with ~6M unique SearchPhrase values: the shard creates groups for unique `(SearchPhrase, UserID)` pairs, NOT 6M HashSets

### CountDistinctAccum (only used in non-dedup path)
Defined at `FusedGroupByAggregate.java:13094`:
- For numeric non-double columns: `LongOpenHashSet` (initial capacity 16)
- For VARCHAR/double columns: `HashSet<Object>`
- Allocated per group via `createAccumulator()` at line 12610

## 4. Top-N Optimization

**No shard-level top-N for COUNT(DISTINCT) queries.** The dedup shard plan produces `GROUP BY (expanded_keys) COUNT(*)` which is a PARTIAL aggregation. The `buildShardPlanWithInflatedLimit()` path (`PlanFragmenter.java:287`) could apply inflated limits, but the dedup plan's expanded key set means the sort column (COUNT(DISTINCT)) doesn't exist at the shard level — it's computed at the coordinator.

The top-N heap selection (`FusedGroupByAggregate.java:3555-3610`) uses `getSortValue()` on `MergeableAccumulator`, which works for `CountDistinctAccum.getSortValue()` returning the set size. But this path is only hit when the shard plan has a Sort+Limit wrapping the aggregation, which the dedup rewrite doesn't produce.

## 5. Coordinator Merge

**TransportTrinoSqlAction** (`TransportTrinoSqlAction.java:354-365`): Detects `isShardDedupCountDistinct()` and calls `mergeDedupCountDistinct()`.

**mergeDedupCountDistinct** (`TransportTrinoSqlAction.java:2109`):
- **Stage 1**: FINAL merge across shards — deduplicates `(original_keys + distinct_col)` tuples, summing COUNT(*) values
- **Stage 2**: Re-aggregate by original keys — counts distinct values per original group

Fast paths:
- All-numeric 2-key dedup: `mergeDedupCountDistinct2Key()` (line 2689) — flat `long[]` arrays, zero per-entry allocation
- All-numeric N-key: open-addressing hash map with `long[]` keys (line 2163)
- VARCHAR key: `mergeDedupCountDistinctVarcharKey()` (line 4182)

For Q13: coordinator receives `(SearchPhrase, UserID, COUNT(*))` tuples from all shards, deduplicates them, then counts distinct UserIDs per SearchPhrase.

## 6. canUseFlatAccumulators for COUNT(DISTINCT)

**Confirmed**: `accType[i] = 5` and `canUseFlatAccumulators = false` (`FusedGroupByAggregate.java:3200-3201`).

However, this is **moot for Q11/Q13** because the PlanFragmenter rewrites the shard plan to use `COUNT(*)` (accType=0), which CAN use flat accumulators. The accType=5 path only triggers if someone bypasses the PlanFragmenter dedup rewrite.

## 7. Memory Impact for Q13 (~6M unique SearchPhrase)

At shard level: groups are `(SearchPhrase, UserID)` pairs with COUNT(*) — no per-group HashSet.
At coordinator: builds a dedup hash map of all unique `(SearchPhrase, UserID)` pairs across shards, then re-aggregates. Memory is proportional to total unique pairs, not 6M × HashSet.

## Summary Table

| Aspect | Q11 (2 GROUP BY + COUNT(DISTINCT)) | Q13 (1 GROUP BY + COUNT(DISTINCT)) |
|--------|-------------------------------------|-------------------------------------|
| PlanOptimizer step | SINGLE | SINGLE |
| Shard plan | GROUP BY (MobilePhone, MobilePhoneModel, UserID) COUNT(*) | GROUP BY (SearchPhrase, UserID) COUNT(*) |
| Shard dispatch | executeWithVarcharKeys (has VARCHAR key) | executeWithVarcharKeys (has VARCHAR key) |
| Shard accType | 0 (COUNT(*)) | 0 (COUNT(*)) |
| canUseFlatAccumulators | true (COUNT(*) only) | true (COUNT(*) only) |
| Per-group data structure | flat long[] or CountStarAccum | flat long[] or CountStarAccum |
| Shard top-N | No | No |
| Coordinator merge | mergeDedupCountDistinct → re-aggregate | mergeDedupCountDistinct → re-aggregate |
| CountDistinctAccum used? | No (dedup rewrite) | No (dedup rewrite) |
