# executePlan() Dispatch Branches — TransportShardExecuteAction.java

**File**: `dqe/src/main/java/org/opensearch/sql/dqe/shard/transport/TransportShardExecuteAction.java`
**Method**: `executePlan()` (lines 194–720)
**Logging**: **NONE** — zero logger.info/debug/warn/trace calls exist anywhere in this file.

## Preamble: ProjectNode Unwrap (lines 203–210)
Before dispatch, if `plan instanceof ProjectNode` wrapping an `AggregationNode`, it unwraps to `effectivePlan = proj.getChild()` so fused paths can match. Stores `topProject` for re-application later.

---

## Branch Table

| # | Lines | Path Name | Condition | Method Called | Query Pattern |
|---|-------|-----------|-----------|---------------|---------------|
| 1 | 212–217 | **Scalar COUNT(\*)** | `scanFactory == null && isScalarCountStar(effectivePlan)` | `executeScalarCountStar()` | `SELECT COUNT(*) FROM t` — no GROUP BY, no other aggs |
| 2 | 219–232 | **Lucene-native sorted scan** | `scanFactory == null && extractSortedScanSpec(plan) != null` | `executeSortedScan()` | `SELECT col FROM t WHERE ... ORDER BY col LIMIT N` — LimitNode→SortNode→TableScanNode |
| 3 | 235–243 | **Fused scan-aggregate (scalar)** | `scanFactory == null && effectivePlan instanceof AggregationNode && FusedScanAggregate.canFuse(aggNode)` | `executeFusedScanAggregate()` | Scalar aggs (SUM/MIN/MAX/COUNT) with no GROUP BY — flat array path |
| 4 | 246–254 | **Bare single numeric column scan** | `scanFactory == null && isBareSingleNumericColumnScan(plan)` | `executeDistinctValuesScanWithRawSet()` | Bare TableScanNode, single numeric col — pre-dedup for COUNT(DISTINCT) via LongOpenHashSet |
| 5 | 256–262 | **Bare single VARCHAR column scan** | `scanFactory == null && isBareSingleVarcharColumnScan(plan)` | `executeDistinctValuesScanVarcharWithRawSet()` | Bare TableScanNode, single VARCHAR col — ordinal-based dedup via FixedBitSet |
| 6 | 264–270 | **Fused eval-aggregate** | `scanFactory == null && effectivePlan instanceof AggregationNode && FusedScanAggregate.canFuseWithEval(aggEvalNode)` | `executeFusedEvalAggregate()` | `SUM(col + constant)` patterns — scalar agg with expression |
| 7a | 276–305 | **2-key COUNT(DISTINCT) — numeric/numeric** | Branch 7 outer + `isSingleCountStar && t0 non-varchar && t1 non-varchar` | `executeCountDistinctWithHashSets()` | `GROUP BY key0 COUNT(DISTINCT key1)` where both keys numeric (Q9 pattern) |
| 7b | 307–313 | **2-key COUNT(DISTINCT) — varchar/numeric** | Branch 7 outer + `isSingleCountStar && t0 instanceof VarcharType && t1 non-varchar` | `executeVarcharCountDistinctWithHashSets()` | `GROUP BY SearchPhrase COUNT(DISTINCT UserID)` — Q14 pattern |
| 7c | 315–331 | **2-key mixed dedup — numeric/numeric** | Branch 7 outer + `isMixedDedup && t0 non-varchar && t1 non-varchar` | `executeMixedDedupWithHashSets()` | `GROUP BY (key0, key1)` with mixed SUM/COUNT aggs, both keys numeric (Q10 pattern) |

**Branch 7 outer condition** (lines 276–282): `scanFactory == null && effectivePlan instanceof AggregationNode && step == PARTIAL && groupByKeys.size() == 2 && FusedGroupByAggregate.canFuse()`

| 8 | 336–351 | **Expression GROUP BY (ordinal-cached)** | `scanFactory == null && effectivePlan instanceof AggregationNode && FusedGroupByAggregate.canFuseWithExpressionKey()` | `executeFusedExprGroupByAggregate()` | GROUP BY computed expression (e.g., REGEXP_REPLACE) over VARCHAR — Q29 pattern. **NOTE**: Must come before branch 9 |
| 9 | 354–363 | **Generic fused GROUP BY** | `scanFactory == null && effectivePlan instanceof AggregationNode && FusedGroupByAggregate.canFuse()` | `executeFusedGroupByAggregate()` | Generic GROUP BY with string/numeric group keys — ordinal-based DocValues path |
| 10 | 366–569 | **Fused GROUP BY + sort+limit** | `scanFactory == null && extractAggFromSortedLimit(plan) != null && FusedGroupByAggregate.canFuse()` | Multiple sub-paths | LimitNode→[ProjectNode]→SortNode→AggregationNode pattern |
| 10a | 401–458 | — HAVING sub-path | `havingFilter != null` (FilterNode between Sort and Agg) | `executeFusedGroupByAggregate()` or `executeFusedExprGroupByAggregate()` + `applyHavingFilter()` + SortOperator | GROUP BY + HAVING + ORDER BY + LIMIT |
| 10b | 461–499 | — Fused top-N sub-path | `sortIndices.size() == 1 && sortIdx >= numGroupByCols` (single agg sort col) | `executeFusedGroupByAggregateWithTopN()` | High-cardinality GROUP BY + ORDER BY single_agg LIMIT N — top-N on flat accData array |
| 10c | 502–569 | — Sort+limit fallback | Default when 10a/10b don't match | `executeFusedGroupByAggregateWithTopN()` (if primary sort is agg col) or `executeFusedGroupByAggregate()` + SortOperator | GROUP BY + ORDER BY + LIMIT — Q33 pattern with multi-key sort |
| 11 | 576–593 | **Fused GROUP BY + limit (no sort)** | `scanFactory == null && extractAggFromLimit(plan) != null && FusedGroupByAggregate.canFuse()` | `executeFusedGroupByAggregateWithTopN()` | `GROUP BY key1, key2 LIMIT N` — Q18 pattern, returns first N groups |
| 12 | 597–677 | **HAVING + sort (no limit)** | `scanFactory == null && extractAggFromSortedFilter(plan) != null && FusedGroupByAggregate.canFuse()` | `executeFusedGroupByAggregate()` or `executeFusedExprGroupByAggregate()` + `applyHavingFilter()` + SortOperator | [ProjectNode]→SortNode→FilterNode→AggregationNode — Q28 pattern, HAVING with coordinator-level LIMIT |
| 13 | 681–720 | **Generic pipeline fallback** | All above branches failed | `LocalExecutionPlanner` → `Operator pipeline` → drain pages | Any query not matching fused paths — builds LucenePageSource → HashAggregationOperator pipeline |

---

## Dispatch Priority Order (matches AGENTS.md)

1. **Scalar COUNT(\*)** → short-circuit (line 212)
2. **Sorted scan** → Lucene-native top-N (line 219)
3. **Scalar agg** → `FusedScanAggregate.canFuse()` → flat array (line 235)
4. **Bare single-col numeric scan** → COUNT(DISTINCT) LongOpenHashSet (line 246)
5. **Bare single-col VARCHAR scan** → COUNT(DISTINCT) FixedBitSet (line 256)
6. **Fused eval-aggregate** → `SUM(col + constant)` (line 264)
7. **2-key COUNT(DISTINCT)** → HashSet paths (line 276) — 3 sub-branches by type combo
8. **Expression GROUP BY** → ordinal-cached path (line 336) — **before** generic canFuse
9. **Generic GROUP BY** → `FusedGroupByAggregate.canFuse()` → fused ordinal path (line 354)
10. **GROUP BY + sort+limit** → fused agg + sort (line 366) — 3 sub-paths (HAVING/topN/fallback)
11. **GROUP BY + limit (no sort)** → fused top-N (line 576)
12. **HAVING + sort (no limit)** → fused agg + filter + sort (line 597)
13. **Generic pipeline fallback** → `LocalExecutionPlanner` operator pipeline (line 681)

## Key Observations

- **Zero logging exists** in the entire file — no way to determine which branch a query takes without adding instrumentation.
- All fused paths are gated by `scanFactory == null` (i.e., not using injected scan factory).
- The `ProjectNode` unwrap at lines 203–210 is critical for single-shard indices.
- Branch 8 (expression GROUP BY) **must** precede branch 9 (generic GROUP BY) per explicit comment at line 340.
- Branch 10 has complex nested sub-dispatch: HAVING → fused top-N → sort+limit fallback.
- The fallback (branch 13) uses `LocalExecutionPlanner` which builds `LucenePageSource → HashAggregationOperator` — described as "much slower" in comments.
