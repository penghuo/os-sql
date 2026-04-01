# COUNT(DISTINCT) Dispatch Paths in TransportShardExecuteAction.java

**File**: `dqe/src/main/java/org/opensearch/sql/dqe/shard/transport/TransportShardExecuteAction.java` (3818 lines)

## executePlan Dispatch Order (line 195)

The `executePlan` method unwraps a top-level `ProjectNode` (if present) to expose the `AggregationNode`, then dispatches through these paths **in order**:

### Path 1: Scalar COUNT(*) (line 237)
- **Guard**: `isScalarCountStar(effectivePlan)`
- **Method**: `executeScalarCountStar` (line 916)
- **Not COUNT(DISTINCT)** — pure COUNT(*) shortcut

### Path 2: Sorted Scan (line 247)
- **Guard**: `extractSortedScanSpec(plan) != null`
- **Not COUNT(DISTINCT)** — ORDER BY + LIMIT optimization

### Path 3: FusedScanAggregate — scalar aggregations (line 258)
- **Guard**: `effectivePlan instanceof AggregationNode && FusedScanAggregate.canFuse(aggNode)`
- **Method**: `executeFusedScanAggregate` (line 949)
- **canFuse** (FusedScanAggregate.java:88): requires `groupByKeys.isEmpty()` and child is `TableScanNode`
- **Relevance**: Handles scalar `COUNT(DISTINCT col)` when Calcite doesn't decompose it (no GROUP BY)

### Path 4: Bare numeric column scan — pre-dedup (line 268)
- **Guard**: `isBareSingleNumericColumnScan(plan)` (line 2998)
- **Method**: `executeDistinctValuesScanWithRawSet` (line 3084)
- **What**: PlanFragmenter strips AggregationNode for SINGLE `COUNT(DISTINCT numericCol)`, leaving bare `TableScanNode`. Shard collects distinct values into `LongOpenHashSet`, attaches to response. Coordinator unions sets.
- **Key detail**: Only fires for bare `TableScanNode` (not `AggregationNode`), meaning the plan was already decomposed by PlanFragmenter.

### Path 5: Bare VARCHAR column scan — pre-dedup (line 276)
- **Guard**: `isBareSingleVarcharColumnScan(plan)` (line 3020)
- **Method**: `executeDistinctValuesScanVarcharWithRawSet` (line 3114)
- **What**: Same as Path 4 but for VARCHAR columns. Uses ordinal-based dedup via `FixedBitSet`.

### Path 6: Fused eval-aggregate (line 283)
- **Guard**: `FusedScanAggregate.canFuseWithEval(aggEvalNode)`
- **Method**: `executeFusedEvalAggregate` (line 971)
- **Not COUNT(DISTINCT)** — handles `SUM(col + constant)` patterns

### Path 7: COUNT(DISTINCT) dedup with HashSets (line 296) ⭐ MAIN DISPATCH
- **Guard**: `effectivePlan instanceof AggregationNode aggDedupNode && step == PARTIAL && groupByKeys.size() >= 2 && FusedGroupByAggregate.canFuse(...)`
- **This is the Calcite-decomposed COUNT(DISTINCT) path**. Calcite rewrites `COUNT(DISTINCT y) GROUP BY x` into inner `GROUP BY (x, y) + COUNT(*)` then outer `GROUP BY x + COUNT(*)`.

**Sub-paths within Path 7:**

| Sub-path | Line | Guard | Method | Pattern |
|----------|------|-------|--------|---------|
| 7a: 2-key numeric | 328 | `numKeys==2 && both non-VARCHAR` | `executeCountDistinctWithHashSets` (line 1001) | Q4/Q5/Q8/Q9: GROUP BY (numKey0, numKey1) + COUNT(*) |
| 7b: VARCHAR key0 + numeric key1 | 336 | `numKeys==2 && key0 is VARCHAR && key1 is numeric` | `executeVarcharCountDistinctWithHashSets` (line 2471) | Q14: GROUP BY (SearchPhrase, UserID) |
| 7c: N-key all-numeric | 354 | `numKeys>=3 && all numeric` | `executeNKeyCountDistinctWithHashSets` (line 1390) | Q11/Q13: GROUP BY (k0, k1, ..., kN-1) |
| 7d: N-key mixed-type | 375 | `numKeys>=3 && last key numeric && some VARCHAR` | `executeMixedTypeCountDistinctWithHashSets` (line 1710) | Q11 variant: GROUP BY (MobilePhone, MobilePhoneModel, UserID) |
| 7e: Mixed dedup (SUM+COUNT) | 395 | `isMixedDedup && numKeys==2 && both numeric` | `executeMixedDedupWithHashSets` (line 1992) | Q10: GROUP BY (k0, k1) with SUM/COUNT aggregates |

### Path 8+: Generic fused GROUP BY (line 405+)
- `canFuseWithExpressionKey` → `executeFusedExprGroupByAggregate` (line 2928)
- `FusedGroupByAggregate.canFuse` → `executeFusedGroupByAggregate` (line 2895)
- Sort+Limit wrapping → `executeFusedGroupByAggregateWithTopN` (line 2951)
- **Fallback**: generic operator pipeline (LucenePageSource → HashAggregationOperator)

## canFuse() Methods

### FusedScanAggregate.canFuse (FusedScanAggregate.java:88)
- Requires: `groupByKeys.isEmpty()`, child is `TableScanNode`, all agg functions match `AGG_FUNCTION` regex
- Handles scalar aggregations only (no GROUP BY)

### FusedScanAggregate.canFuseWithEval (FusedScanAggregate.java:111)
- Requires: `groupByKeys.isEmpty()`, child is `EvalNode → TableScanNode`, all aggs are SUM/COUNT/AVG (non-distinct)

### FusedGroupByAggregate.canFuse (FusedGroupByAggregate.java:198)
- Requires: `groupByKeys` non-empty, child chain reaches `TableScanNode`
- Supports: VARCHAR, numeric, timestamp group keys; DATE_TRUNC/EXTRACT/arithmetic expressions; EvalNode-computed keys
- Agg args must be physical columns, `*`, or `length(varchar_col)`

### FusedGroupByAggregate.canFuseWithExpressionKey (FusedGroupByAggregate.java:314)
- Requires: exactly 1 group-by key, child is `EvalNode → TableScanNode`
- For ordinal-cached expression evaluation (e.g., REGEXP_REPLACE)

## Key Insight for COUNT(DISTINCT) Performance

The **doubled key space problem**: Calcite decomposes `SELECT x, COUNT(DISTINCT y) FROM t GROUP BY x` into:
1. Inner: `GROUP BY (x, y)` with `COUNT(*)` — this is what the shard sees as `aggDedupNode`
2. Outer: `GROUP BY (x)` with `COUNT(*)` — done at coordinator

Path 7 (line 296) detects this pattern and **fuses** it: instead of materializing all (x, y) tuples, it builds `Map<x, HashSet<y>>` during DocValues iteration, outputting only `(x, count)` per shard. This avoids the key-space explosion.

The guard `groupByKeys.size() >= 2` is the signal that this is a decomposed COUNT(DISTINCT) — the last key is the dedup key, the rest are the original GROUP BY keys.
