# COUNT(DISTINCT) Dispatch Analysis in DQE

## 1. How COUNT(DISTINCT) Queries Are Currently Dispatched

### PlanOptimizer (dqe/src/main/java/.../planner/optimizer/PlanOptimizer.java)
- `hasNonDecomposableAgg()` (line ~394) detects `COUNT(DISTINCT` in aggregate functions
- When found, forces `AggregationNode.Step.SINGLE` (line ~378) — meaning the aggregation is NOT decomposed into PARTIAL/FINAL
- This means shards send raw/deduped data and the coordinator runs the full aggregation

### PlanFragmenter (dqe/src/main/java/.../coordinator/fragment/PlanFragmenter.java)
- `buildShardPlan()` (line ~112) handles SINGLE step with GROUP BY:
  - Extracts distinct columns from `COUNT(DISTINCT col)` via `extractCountDistinctColumns()`
  - Builds a **dedup shard plan**: `AggregationNode(PARTIAL, groupByKeys=[original_keys + distinct_columns], aggs=[COUNT(*)])`
  - Example: `SELECT RegionID, COUNT(DISTINCT UserID)` → shard plan becomes `GROUP BY (RegionID, UserID) COUNT(*)`
  - For mixed aggregates (COUNT(DISTINCT) + SUM/COUNT), `buildMixedDedupShardPlan()` (line ~173) creates a similar dedup plan with partial aggregates for decomposable functions

### TransportShardExecuteAction.java Dispatch (line ~270-360)
The `executePlan()` method dispatches in priority order. The COUNT(DISTINCT) dedup fast path fires at **line 279**:

```
Condition: scanFactory == null
  && effectivePlan instanceof AggregationNode aggDedupNode
  && aggDedupNode.getStep() == PARTIAL
  && aggDedupNode.getGroupByKeys().size() >= 2
  && FusedGroupByAggregate.canFuse(aggDedupNode, columnTypeMap)
```

This matches the **dedup plan** created by PlanFragmenter (not the original query plan). The dedup plan has `groupByKeys = [original_keys + distinct_columns]` and `aggs = [COUNT(*)]`.

## 2. The Two-Level Calcite Plan Pattern

Calcite does NOT generate a two-level plan in this codebase. Instead, the DQE uses its own approach:

**Original query plan** (from LogicalPlanner):
```
AggregationNode(SINGLE, groupBy=[RegionID], aggs=[COUNT(DISTINCT UserID)])
  └── TableScanNode
```

**After PlanOptimizer** (forces SINGLE for COUNT(DISTINCT)):
```
AggregationNode(SINGLE, groupBy=[RegionID], aggs=[COUNT(DISTINCT UserID)])
  └── TableScanNode
```

**After PlanFragmenter** (creates dedup shard plan):
```
Shard plan: AggregationNode(PARTIAL, groupBy=[RegionID, UserID], aggs=[COUNT(*)])
                └── TableScanNode
Coordinator plan: AggregationNode(SINGLE, groupBy=[RegionID], aggs=[COUNT(DISTINCT UserID)])
```

The **"two-level" pattern** that the handover refers to is this dedup plan structure:
- **Outer Aggregate** (coordinator): `GROUP BY x, COUNT(DISTINCT y)` — the original query's aggregation
- **Inner Aggregate** (shard dedup plan): `GROUP BY (x, y), COUNT(*)` — the expanded dedup plan

The shard receives the inner aggregate. The coordinator merges with a two-stage process:
1. FINAL merge on dedup keys (remove cross-shard duplicates)
2. Re-aggregate with `GROUP BY original_keys, COUNT(*)` to get COUNT(DISTINCT)

## 3. Existing Fast Paths for COUNT(DISTINCT)

### Scalar COUNT(DISTINCT) — bare scan paths (lines 251-261)
- `isBareSingleNumericColumnScan()` (line 2094): bare `TableScanNode` with single numeric column
  - → `executeDistinctValuesScanWithRawSet()` (line 2180): collects into `LongOpenHashSet`, attaches raw set
- `isBareSingleVarcharColumnScan()` (line 2116): bare `TableScanNode` with single VARCHAR column
  - → `executeDistinctValuesScanVarcharWithRawSet()` (line 2210): ordinal-based dedup via `FixedBitSet`

### GROUP BY + COUNT(DISTINCT) — HashSet-based paths (lines 279-360)
All require: `PARTIAL` step, `groupByKeys.size() >= 2`, `canFuse()` passes.

| Method | Line | Pattern | Description |
|--------|------|---------|-------------|
| `executeCountDistinctWithHashSets` | 960 | 2 numeric keys, COUNT(*) only | GROUP BY key0 with `LongOpenHashSet` per group for key1. Compact output + attached HashSets. |
| `executeVarcharCountDistinctWithHashSets` | 1798 | VARCHAR key0 + numeric key1, COUNT(*) only | VARCHAR GROUP BY with numeric HashSet dedup |
| `executeNKeyCountDistinctWithHashSets` | 1248 | 3+ numeric keys, COUNT(*) only | GROUP BY (key0..keyN-2) with HashSet for keyN-1. Full dedup tuples output. |
| `executeMixedDedupWithHashSets` | 1537 | 2 numeric keys, mixed SUM/COUNT aggs | GROUP BY key0 with HashSet for key1 + accumulators for SUM/COUNT |

### Coordinator merge paths (TransportTrinoSqlAction.java)
- `isShardDedupCountDistinct()` (line 2071): detects shard dedup pattern
- `mergeDedupCountDistinct()` (line ~2100): two-stage merge (FINAL dedup + re-aggregate)
- `mergeCountDistinctValues()` (line 1973): scalar numeric COUNT(DISTINCT) via LongOpenHashSet union
- `mergeCountDistinctVarcharValues()` (line 2030): scalar VARCHAR COUNT(DISTINCT) via string set union

## 4. Interception Point for Fusion

The interception should happen at **TransportShardExecuteAction.java, line ~279** — the existing COUNT(DISTINCT) dedup fast path block.

Currently, the dispatch checks:
```java
if (scanFactory == null
    && effectivePlan instanceof AggregationNode aggDedupNode
    && aggDedupNode.getStep() == AggregationNode.Step.PARTIAL
    && aggDedupNode.getGroupByKeys().size() >= 2
    && FusedGroupByAggregate.canFuse(aggDedupNode, columnTypeMap))
```

**To add fusion**, you would:
1. Detect the pattern: outer `AggregationNode(GROUP BY x, COUNT(*))` + the knowledge that this is a dedup plan (keys include the distinct column)
2. Instead of routing to `executeCountDistinctWithHashSets` (which builds per-group HashSets), route to a **fused GROUP BY** path that:
   - Groups by only the original key(s) (not the expanded dedup keys)
   - Uses per-group `LongOpenHashSet` accumulators to track distinct values
   - Outputs `(group_key, count_of_distinct)` directly — no intermediate dedup tuples

The interception is **inside** the existing `if` block at line 279, before the sub-dispatch to `executeCountDistinctWithHashSets` etc. A new check could detect when the dedup plan's structure allows direct fusion (single-pass GROUP BY + HashSet accumulation) instead of the current two-phase approach.

## 5. What "AggDedupNode" Is

`aggDedupNode` is **NOT a separate class**. It's a **pattern variable** in Java 16+ pattern matching:

```java
effectivePlan instanceof AggregationNode aggDedupNode
```

This binds `effectivePlan` to the variable `aggDedupNode` of type `AggregationNode` when the instanceof check passes. The name "aggDedupNode" is just a convention indicating this is the dedup plan's aggregation node (created by PlanFragmenter).

The `AggregationNode` class itself (dqe/src/main/java/.../planner/plan/AggregationNode.java) has:
- `child: DqePlanNode` — the child plan node (TableScanNode or EvalNode)
- `groupByKeys: List<String>` — column names for GROUP BY
- `aggregateFunctions: List<String>` — string representations like "COUNT(*)", "SUM(col)"
- `step: Step` — enum: PARTIAL, FINAL, or SINGLE

In the dedup context, `aggDedupNode` has:
- `groupByKeys = [original_group_keys + distinct_columns]` (expanded)
- `aggregateFunctions = ["COUNT(*)"]`
- `step = PARTIAL`

## Key File References

| File | Purpose |
|------|---------|
| `dqe/src/main/java/.../shard/transport/TransportShardExecuteAction.java` | Shard-level dispatch, fast paths (lines 270-360 for COUNT(DISTINCT)) |
| `dqe/src/main/java/.../planner/optimizer/PlanOptimizer.java` | Forces SINGLE step for COUNT(DISTINCT) (line ~378) |
| `dqe/src/main/java/.../coordinator/fragment/PlanFragmenter.java` | Creates dedup shard plan (line ~144-157) |
| `dqe/src/main/java/.../coordinator/transport/TransportTrinoSqlAction.java` | Coordinator merge for dedup (line ~2071) |
| `dqe/src/main/java/.../shard/source/FusedGroupByAggregate.java` | canFuse() check (line 197), fused GROUP BY execution |
| `dqe/src/main/java/.../operator/LongOpenHashSet.java` | Custom open-addressing hash set for distinct value tracking |
| `dqe/src/main/java/.../planner/plan/AggregationNode.java` | Plan node with groupByKeys, aggregateFunctions, step |
