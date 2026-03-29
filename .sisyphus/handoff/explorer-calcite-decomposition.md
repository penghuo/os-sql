# COUNT(DISTINCT) Decomposition in DQE Planner

## Query: `SELECT RegionID, COUNT(DISTINCT UserID) FROM hits GROUP BY RegionID`

## Pipeline: `LogicalPlanner.plan() → PlanOptimizer.optimize() → PlanFragmenter.fragment()`

### Phase 1: PlanOptimizer — Mark as Non-Decomposable (SINGLE)

`AggregationSplitVisitor.visitAggregation()` calls `hasNonDecomposableAgg()` which
detects COUNT(DISTINCT) and forces the step to **SINGLE** (not PARTIAL).

```
AggregationNode(SINGLE, groupBy=[RegionID], agg=[COUNT(DISTINCT UserID)])
  └─ TableScanNode("hits", [RegionID, UserID])
```

### Phase 2: PlanFragmenter — Shard Dedup + Coordinator Full Agg

`buildShardPlan()` handles SINGLE + GROUP BY. `extractCountDistinctColumns()` confirms
all aggs are COUNT(DISTINCT), then builds a **dedup shard plan**: GROUP BY original
keys + distinct columns with COUNT(*).

#### Shard Plan (×N shards):
```
AggregationNode(PARTIAL, groupBy=[RegionID, UserID], agg=[COUNT(*)])
  └─ TableScanNode("hits", [RegionID, UserID], shard=i)
```
Each shard deduplicates UserID per RegionID, reducing data sent to coordinator.

#### Coordinator Plan:
```
AggregationNode(SINGLE, groupBy=[RegionID], agg=[COUNT(DISTINCT UserID)])
  └─ UNION of shard results (pre-deduped (RegionID, UserID) rows)
```

### Complete Plan Tree
```
COORDINATOR:
  ProjectNode([RegionID, COUNT(DISTINCT UserID)])
    └─ AggregationNode(SINGLE, groupBy=[RegionID], agg=[COUNT(DISTINCT UserID)])
        └─ UNION of shard results
EACH SHARD (×N):
  AggregationNode(PARTIAL, groupBy=[RegionID, UserID], agg=[COUNT(*)])
    └─ TableScanNode("hits", [RegionID, UserID], shard=i)
```

## Key Code Locations

| Component | File | Method |
|-----------|------|--------|
| Non-decomposable detection | `PlanOptimizer.java` | `hasNonDecomposableAgg()` |
| Force SINGLE step | `PlanOptimizer.java` | `AggregationSplitVisitor.visitAggregation()` |
| Shard dedup plan | `PlanFragmenter.java` | `buildShardPlan()` — SINGLE+GROUP BY branch |
| Extract distinct cols | `PlanFragmenter.java` | `extractCountDistinctColumns()` |
| Coordinator SINGLE agg | `PlanFragmenter.java` | `buildCoordinatorPlan()` |
| Mixed dedup (CD+SUM) | `PlanFragmenter.java` | `buildMixedDedupShardPlan()` |

## Why Two Levels?

COUNT(DISTINCT) can't be split PARTIAL/FINAL — the same value may appear on multiple
shards, so partial distinct counts can't be merged. Solution: **dedup at shard level
(GROUP BY keys+distinct_cols), full aggregation at coordinator** on the reduced dataset.
