# PlanFragmenter COUNT(DISTINCT) Rewrite Analysis

## Core Architecture

**File**: `dqe/src/main/java/org/opensearch/sql/dqe/coordinator/fragment/PlanFragmenter.java`

The `PlanFragmenter.fragment()` method splits a logical plan into per-shard fragments + a coordinator merge plan. The key method is `buildShardPlan()` which decides what transformation to apply based on the AggregationNode's Step and whether COUNT(DISTINCT) is present.

## AggregationNode.Step Enum

```java
public enum Step { PARTIAL, FINAL, SINGLE }
```

- **PARTIAL**: Shard-local aggregation (decomposable aggs like COUNT(*), SUM)
- **FINAL**: Coordinator merge of PARTIAL results
- **SINGLE**: Full aggregation at coordinator over raw shard data (non-decomposable aggs)

## Decision Tree in `buildShardPlan()` (PlanFragmenter.java:~L95-L140)

```
if (aggNode == null) → return full plan (no aggregation)
if (aggNode.step == PARTIAL) → strip Sort/Limit/Project above agg, decompose AVG
if (GROUP BY + all-COUNT(DISTINCT)) → DEDUP path
if (GROUP BY + mixed COUNT(DISTINCT) + decomposable) → MIXED DEDUP path
else (SINGLE) → strip aggregation, shard only does scan+filter
```

## Case 1: Scalar COUNT(DISTINCT) — No GROUP BY

**PlanFragmenter does NOT rewrite scalar COUNT(DISTINCT).** The AggregationNode has step=SINGLE and empty groupByKeys, so `buildShardPlan()` falls through to the last branch:

```java
// SINGLE: strip aggregation and above, shards only scan+filter
return aggNode.getChild();
```

**Shard plan**: Just `TableScanNode` (or `FilterNode → TableScanNode`). The AggregationNode is stripped entirely.

**Coordinator plan**: `AggregationNode(step=SINGLE)` — coordinator runs full aggregation over raw shard data.

**BUT** — the coordinator (`TransportTrinoSqlAction`) has special fast paths that intercept this before running generic aggregation:

1. `isScalarCountDistinctLong()` → numeric columns: shards send pre-deduped long values, coordinator unions via `LongOpenHashSet`
2. `isScalarCountDistinctVarchar()` → VARCHAR columns: shards send pre-deduped string values, coordinator unions via `SliceRangeHashSet`

The shard executor (`TransportShardExecuteAction`) detects scalar COUNT(DISTINCT) and pre-deduplicates values before sending.

## Case 2: GROUP BY + COUNT(DISTINCT) Only — DEDUP Path

**Condition**: `!aggNode.getGroupByKeys().isEmpty()` AND all aggregate functions are `COUNT(DISTINCT col)`.

**Rewrite** (PlanFragmenter.java:~L120-L130):
```java
// Original: GROUP BY [key1] with [COUNT(DISTINCT col1)]
// Rewritten shard plan:
dedupKeys = [key1, col1]  // original keys + distinct columns
new AggregationNode(child, dedupKeys, ["COUNT(*)"], Step.PARTIAL)
```

**Example**: `SELECT region, COUNT(DISTINCT user_id) FROM t GROUP BY region`
- **Shard plan**: `AggregationNode(PARTIAL, groupBy=[region, user_id], aggs=[COUNT(*)])`
- **Coordinator plan**: `AggregationNode(SINGLE, groupBy=[region], aggs=[COUNT(DISTINCT user_id)])`

The coordinator detects this via `isShardDedupCountDistinct()` which checks:
- Shard plan is `AggregationNode(PARTIAL)`
- Coordinator plan is `AggregationNode(SINGLE)` with GROUP BY
- Shard has MORE group-by keys than coordinator (original + distinct cols)
- Shard agg is exactly `["COUNT(*)"]`

**Coordinator merge** (`mergeDedupCountDistinct`): Two-stage merge:
1. Stage 1: FINAL merge on dedup keys → removes cross-shard duplicates
2. Stage 2: GROUP BY original keys, COUNT rows → gives COUNT(DISTINCT)

Multiple optimized paths exist:
- `mergeDedupCountDistinct2Key()` — ultra-fast 2-key numeric path (flat long[] arrays)
- `mergeDedupCountDistinctViaSets()` — uses pre-built `LongOpenHashSet` attachments from shards
- `mergeDedupCountDistinctViaVarcharSets()` — VARCHAR group keys with `LongOpenHashSet`
- `mergeDedupCountDistinctVarcharKey()` — VARCHAR key + numeric distinct col via `SliceLongDedupMap`

## Case 3: GROUP BY + Mixed Aggregates (COUNT(DISTINCT) + SUM/COUNT/AVG) — MIXED DEDUP Path

**Condition**: Has both COUNT(DISTINCT) and decomposable aggregates (SUM, COUNT(*), AVG, MIN, MAX).

**Rewrite** (`buildMixedDedupShardPlan()`, PlanFragmenter.java:~L155-L210):
```java
// Original: GROUP BY [key1] with [SUM(x), COUNT(*), AVG(y), COUNT(DISTINCT z)]
// Rewritten shard plan:
dedupKeys = [key1, z]  // original keys + distinct columns
shardAggs = ["SUM(x)", "COUNT(*)", "SUM(y)", "COUNT(y)"]  // AVG decomposed, COUNT(DISTINCT) omitted
new AggregationNode(child, dedupKeys, shardAggs, Step.PARTIAL)
```

Key transformations:
- `AVG(col)` → `SUM(col)` + `COUNT(col)` (for weighted merge)
- `COUNT(DISTINCT col)` → omitted (handled by dedup GROUP BY)
- `SUM`, `COUNT(*)`, `MIN`, `MAX` → passed through as-is

**Coordinator merge** (`mergeMixedDedup`): Two-stage:
1. Stage 1: FINAL merge on dedup keys (merges partial SUM/COUNT across shards)
2. Stage 2: Re-aggregate by original keys: SUM the partial sums, compute AVG from SUM/COUNT, COUNT rows for COUNT(DISTINCT)

Optimized paths: `mergeMixedDedup2Key()` (flat arrays), `mergeMixedDedupViaSets()` (HashSet attachments)

## Coordinator Plan Construction

`buildCoordinatorPlan()` (PlanFragmenter.java:~L280-L300):
```java
if (aggNode.step == PARTIAL) → AggregationNode(FINAL, same groupBy, same aggs)
if (aggNode != null)         → AggregationNode(SINGLE, same groupBy, same aggs)
else                         → null (no aggregation)
```

**Important**: For dedup/mixed-dedup paths, the coordinator plan uses the ORIGINAL aggregation's groupBy and aggs (not the rewritten shard plan's). The coordinator plan has `step=SINGLE` with the original `COUNT(DISTINCT col)` in its aggregate functions.

## Summary Table

| Query Pattern | Shard Plan | Coordinator Plan | Merge Strategy |
|---|---|---|---|
| `COUNT(DISTINCT col)` (scalar) | `TableScanNode` only (agg stripped) | `AggregationNode(SINGLE)` | Union distinct values via HashSet |
| `GROUP BY k COUNT(DISTINCT d)` | `AggregationNode(PARTIAL, groupBy=[k,d], aggs=[COUNT(*)])` | `AggregationNode(SINGLE, groupBy=[k], aggs=[COUNT(DISTINCT d)])` | Two-stage: dedup merge → count per group |
| `GROUP BY k SUM(x), COUNT(DISTINCT d)` | `AggregationNode(PARTIAL, groupBy=[k,d], aggs=[SUM(x)])` | `AggregationNode(SINGLE, groupBy=[k], aggs=[SUM(x), COUNT(DISTINCT d)])` | Two-stage: dedup merge → re-aggregate |
| `GROUP BY k COUNT(*)` (no DISTINCT) | `AggregationNode(PARTIAL, groupBy=[k], aggs=[COUNT(*)])` | `AggregationNode(FINAL, groupBy=[k], aggs=[COUNT(*)])` | Standard FINAL merge |

## Key Files

| File | Purpose |
|---|---|
| `dqe/.../coordinator/fragment/PlanFragmenter.java` | Core rewrite logic — splits plan into shard + coordinator |
| `dqe/.../coordinator/transport/TransportTrinoSqlAction.java` | Coordinator merge logic — dispatches to shards, merges results |
| `dqe/.../planner/plan/AggregationNode.java` | Plan node with Step enum (PARTIAL/FINAL/SINGLE) |
| `dqe/.../coordinator/fragment/PlanFragment.java` | Record: (shardPlan, indexName, shardId, nodeId) |
| `dqe/.../shard/transport/TransportShardExecuteAction.java` | Shard-side execution, pre-dedup for scalar COUNT(DISTINCT) |
