# PlanFragmenter Analysis: COUNT(DISTINCT) Decomposition

## 1. Plan Node Types

All defined in `dqe/src/main/java/org/opensearch/sql/dqe/planner/plan/`:

| Node | Purpose |
|------|---------|
| `AggregationNode` | Aggregation with `Step` enum: `PARTIAL`, `FINAL`, `SINGLE` |
| `LimitNode` | LIMIT/OFFSET (count + offset) |
| `SortNode` | ORDER BY (sortKeys, ascending, nullsFirst) |
| `FilterNode` | WHERE / HAVING |
| `ProjectNode` | Column projection |
| `EvalNode` | Expression evaluation |
| `TableScanNode` | Index scan |
| `DqePlanNode` | Abstract base |

**There is NO TopNNode.** TopN is expressed as `LimitNode(SortNode(...))`.

## 2. How buildShardPlan() Handles COUNT(DISTINCT)

Key file: `PlanFragmenter.java:97-155`

The logic branches on `AggregationNode.Step`:

### Path A: Step == PARTIAL (pre-decomposed aggregations like COUNT(*), SUM)
- Tries `buildShardPlanWithInflatedLimit()` — keeps Sort/Limit with inflated limit
- Tries `buildShardPlanWithLimitOnly()` — keeps Limit for capped GROUP BY
- Falls back to `stripAboveAggregation()` — strips Sort/Limit/Project/Filter above Agg
- Then `decomposeAvgInShardPlan()` — decomposes AVG → SUM + COUNT

### Path B: Step == SINGLE + GROUP BY + COUNT(DISTINCT) only (lines 139-149)
**This is the COUNT(DISTINCT) dedup path.**

1. `extractCountDistinctColumns()` checks ALL agg functions are `COUNT(DISTINCT col)`.
2. If yes, builds a **dedup shard plan**:
   ```
   AggregationNode(PARTIAL,
     groupBy = [original_keys + distinct_columns],
     aggs = [COUNT(*)],
     child = original_child)
   ```

### Path C: Step == SINGLE + mixed aggregates (COUNT(DISTINCT) + SUM/COUNT/etc.)
`buildMixedDedupShardPlan()` (lines 151-153):
- Groups by `(original_keys + distinct_columns)`
- Decomposes AVG → SUM + COUNT
- Passes through SUM, COUNT(*), MIN, MAX as-is
- Omits COUNT(DISTINCT) from shard aggs (handled by dedup GROUP BY)

### Path D: Step == SINGLE, no dedup eligible
Falls through to `aggNode.getChild()` — shards only scan+filter, coordinator does full aggregation.

## 3. Q08 Trace: GROUP BY RegionID, COUNT(DISTINCT UserID) ORDER BY u DESC LIMIT 10

Input plan (logical):
```
LimitNode(10)
  SortNode(u DESC)
    ProjectNode(...)
      AggregationNode(SINGLE, groupBy=[RegionID], aggs=[COUNT(DISTINCT UserID)])
        FilterNode(...)
          TableScanNode(hits)
```

### Shard Plan
`buildShardPlan()` finds `AggregationNode(SINGLE)` → enters Path B.

`extractCountDistinctColumns(["COUNT(DISTINCT UserID)"])` returns `["UserID"]`.

**Shard plan becomes:**
```
AggregationNode(PARTIAL,
  groupBy = [RegionID, UserID],
  aggs = [COUNT(*)],
  child = FilterNode → TableScanNode)
```

**Sort/Limit/Project are ALL STRIPPED.** The shard only does the dedup GROUP BY.

### Coordinator Plan
`buildCoordinatorPlan()` sees `Step == SINGLE` → returns:
```
AggregationNode(SINGLE, groupBy=[RegionID], aggs=[COUNT(DISTINCT UserID)])
```

### Coordinator Merge (TransportTrinoSqlAction.java:350-362)
The coordinator detects `isShardDedupCountDistinct()` == true (shard plan is PARTIAL with expanded keys + COUNT(*), coordinator is SINGLE with COUNT(DISTINCT)).

Calls `mergeDedupCountDistinct()` which does a **two-stage merge**:
1. **Stage 1 (FINAL merge):** Hash-merge dedup keys `(RegionID, UserID)` across shards → removes cross-shard duplicates
2. **Stage 2 (re-aggregate):** GROUP BY `RegionID` with `COUNT(*)` → produces `COUNT(DISTINCT UserID)` per region

Then `applyCoordinatorSort()` applies the original ORDER BY + LIMIT.

## 4. Does TopNNode Survive Into the Shard Plan?

**There is no TopNNode class.** TopN is `LimitNode(SortNode(...))`.

For COUNT(DISTINCT) queries (SINGLE step), **Sort and Limit are stripped** — the shard plan is just the dedup AggregationNode. The coordinator applies Sort/Limit after merge.

For PARTIAL step queries (non-COUNT(DISTINCT)), `buildShardPlanWithInflatedLimit()` CAN preserve `LimitNode(SortNode(AggNode))` with an inflated limit (`max(1000, limit * numShards * 2)`), but this only applies when:
- Primary sort key is an aggregate column (not group-by key)
- No HAVING clause
- Step is PARTIAL

## 5. Coordinator Merge Summary for COUNT(DISTINCT)

| Scenario | Coordinator Behavior |
|----------|---------------------|
| Scalar COUNT(DISTINCT long) | `mergeCountDistinctValues()` — direct set union |
| Scalar COUNT(DISTINCT varchar) | `mergeCountDistinctVarcharValues()` — varchar set union |
| GROUP BY + COUNT(DISTINCT) only | `mergeDedupCountDistinct()` — 2-stage: FINAL dedup merge → re-aggregate |
| GROUP BY + mixed (COUNT(DISTINCT) + SUM/etc.) | `mergeMixedDedup()` — merge partials + re-aggregate |
| GROUP BY + SINGLE (no dedup eligible) | `runCoordinatorAggregation()` — full aggregation on raw shard data |

## Key Files

| File | Purpose |
|------|---------|
| `dqe/.../fragment/PlanFragmenter.java` | Plan decomposition logic |
| `dqe/.../transport/TransportTrinoSqlAction.java:333-400` | Coordinator merge dispatch |
| `dqe/.../transport/TransportTrinoSqlAction.java:2057-2160` | `isShardDedupCountDistinct` + `mergeDedupCountDistinct` |
| `dqe/.../plan/AggregationNode.java` | Step enum (PARTIAL/FINAL/SINGLE), groupByKeys, aggregateFunctions |
| `dqe/.../merge/ResultMerger.java` | Hash-based merge for FINAL step |
