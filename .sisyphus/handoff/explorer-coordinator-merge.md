# COUNT(DISTINCT) Pipeline: PlanOptimizer → PlanFragmenter → Coordinator Merge

## 1. PlanOptimizer: Forces SINGLE Mode for COUNT(DISTINCT)

**File:** `dqe/src/main/java/org/opensearch/sql/dqe/planner/optimizer/PlanOptimizer.java`

The `AggregationSplitVisitor.visitAggregation()` (line ~240) calls `hasNonDecomposableAgg()` which checks:
- If any aggregate contains `COUNT(DISTINCT` → returns `true`
- If AVG without companion COUNT in GROUP BY queries → returns `true`

When `hasNonDecomposableAgg()` returns true, the step is forced to **`AggregationNode.Step.SINGLE`** (not PARTIAL).

```java
if (hasNonDecomposableAgg(node.getAggregateFunctions(), node.getGroupByKeys())) {
    return new AggregationNode(optimizedChild, node.getGroupByKeys(),
        node.getAggregateFunctions(), AggregationNode.Step.SINGLE);
}
```

## 2. PlanFragmenter: Shard-Level Dedup for COUNT(DISTINCT)

**File:** `dqe/src/main/java/org/opensearch/sql/dqe/coordinator/fragment/PlanFragmenter.java`

The PlanFragmenter receives the SINGLE-mode aggregation and applies **shard-level dedup optimization** in `buildShardPlan()`:

### Case A: Pure COUNT(DISTINCT) with GROUP BY
When all aggregates are `COUNT(DISTINCT col)`, `extractCountDistinctColumns()` succeeds. The shard plan becomes:
```
AggregationNode(PARTIAL, GROUP BY [original_keys + distinct_columns], [COUNT(*)])
```
Example: `SELECT region, COUNT(DISTINCT user_id) FROM t GROUP BY region` →
Shard plan: `GROUP BY (region, user_id) with COUNT(*)`

### Case B: Mixed Aggregates (COUNT(DISTINCT) + SUM/AVG/etc.)
`buildMixedDedupShardPlan()` handles queries like Q10. The shard plan becomes:
```
AggregationNode(PARTIAL, GROUP BY [original_keys + distinct_columns], [decomposed_aggs])
```
AVG(col) is decomposed to SUM(col) + COUNT(col). COUNT(DISTINCT) is omitted from shard aggs.

### Case C: Scalar COUNT(DISTINCT) (no GROUP BY)
Falls through to `aggNode.getChild()` — shards just scan+filter, sending raw data.

### Coordinator Plan
`buildCoordinatorPlan()` creates `AggregationNode(SINGLE)` for the coordinator since the original step was SINGLE.

## 3. Coordinator Merge Logic in TransportTrinoSqlAction

**File:** `dqe/src/main/java/org/opensearch/sql/dqe/coordinator/transport/TransportTrinoSqlAction.java`

The coordinator dispatches to different merge paths based on the coordinator plan type:

### 3a. Scalar COUNT(DISTINCT long_col) — `mergeCountDistinctValues()`
- Each shard sends pre-deduped distinct long values as Pages
- Coordinator unions via `LongOpenHashSet` (custom open-addressing hash set)
- Returns single-row Page with `hashSet.size()`

### 3b. Scalar COUNT(DISTINCT varchar_col) — `mergeCountDistinctVarcharValues()`
- Each shard sends pre-deduped distinct string values as Pages
- Coordinator unions via `SliceRangeHashSet` (zero-allocation, operates on raw byte ranges)
- Returns single-row Page with `hashSet.size()`

### 3c. Local-node optimization: `mergeCountDistinctValuesViaRawSets()`
- When all shards are local, `ShardExecuteResponse` carries **transient** `LongOpenHashSet` / `Set<String>` attachments
- Coordinator unions raw sets directly (no Page extraction needed)
- Uses parallel contains() checks: finds largest set, counts extras from smaller sets

### 3d. GROUP BY + COUNT(DISTINCT) — `mergeDedupCountDistinct()`
Two-stage merge:
1. **Stage 1 (FINAL dedup):** Merge shard (key+distinct_col, COUNT(*)) tuples to remove cross-shard duplicates
2. **Stage 2 (Re-aggregate):** GROUP BY original keys, count rows per group → COUNT(DISTINCT)

Fast paths:
- **2-key numeric:** `mergeDedupCountDistinct2Key()` — flat long[] arrays, zero allocation
- **VARCHAR key + numeric distinct:** `mergeDedupCountDistinctVarcharKey()` — uses `SliceLongDedupMap` + `SliceCountMap`
- **Via HashSet attachments:** `mergeDedupCountDistinctViaSets()` — unions per-group `LongOpenHashSet` from shard responses
- **Via VARCHAR HashSet attachments:** `mergeDedupCountDistinctViaVarcharSets()` — unions per-group sets with top-K pruning

### 3e. Mixed Aggregates + COUNT(DISTINCT) — `mergeMixedDedup()`
Three-stage merge:
1. **Stage 1:** FINAL merge for dedup keys (removes cross-shard duplicates, sums partial aggs)
2. **Stage 2:** Re-aggregate by original keys: SUM partials, compute weighted AVG, count rows for COUNT(DISTINCT)

Fast paths:
- **2-key numeric:** `mergeMixedDedup2Key()` — flat long[] arrays
- **Via HashSet attachments:** `mergeMixedDedupViaSets()` — merges accumulators + unions HashSets

### 3f. Fallback: `runCoordinatorAggregation()`
When no dedup optimization applies (e.g., SINGLE mode without shard dedup), raw pages from all shards are concatenated and fed through `HashAggregationOperator` at the coordinator.

## 4. ShardExecuteResponse HashSet Attachments

**File:** `dqe/src/main/java/org/opensearch/sql/dqe/shard/transport/ShardExecuteResponse.java`

Transient (local-only, not serialized) fields:
- `Map<Long, LongOpenHashSet> distinctSets` — per-group distinct sets (numeric GROUP BY key)
- `Map<String, LongOpenHashSet> varcharDistinctSets` — per-group distinct sets (VARCHAR GROUP BY key)
- `LongOpenHashSet scalarDistinctSet` — scalar COUNT(DISTINCT numeric_col)
- `Set<String> scalarDistinctStrings` — scalar COUNT(DISTINCT varchar_col)

These are populated by `TransportShardExecuteAction` during local execution and consumed by the coordinator merge methods. For remote shards, these are null (not serialized), and the coordinator falls back to Page-based merge.

## Summary: Mode Decision Flow

```
PlanOptimizer.splitAggregations()
  └─ hasNonDecomposableAgg() detects COUNT(DISTINCT) → forces SINGLE step

PlanFragmenter.buildShardPlan()
  ├─ Pure COUNT(DISTINCT) + GROUP BY → PARTIAL dedup: GROUP BY (keys + distinct_cols), COUNT(*)
  ├─ Mixed aggs + COUNT(DISTINCT) + GROUP BY → PARTIAL mixed dedup: GROUP BY (keys + distinct_cols), decomposed_aggs
  └─ Scalar COUNT(DISTINCT) → raw scan (no shard aggregation)

Coordinator merge (TransportTrinoSqlAction)
  ├─ Scalar numeric → LongOpenHashSet union
  ├─ Scalar varchar → SliceRangeHashSet union
  ├─ GROUP BY dedup → 2-stage: FINAL dedup merge + row counting
  ├─ Mixed dedup → 2-stage: FINAL dedup merge + re-aggregate
  └─ Fallback → full HashAggregationOperator at coordinator
```
