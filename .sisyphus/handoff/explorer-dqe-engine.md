# DQE Engine: GROUP BY & Aggregation Optimization Analysis

## Key Files

| File | Path | Lines | Purpose |
|------|------|-------|---------|
| TransportShardExecuteAction | `dqe/.../shard/transport/TransportShardExecuteAction.java` | ~2200 | Shard-level plan execution with dispatch logic |
| FusedGroupByAggregate | `dqe/.../shard/source/FusedGroupByAggregate.java` | ~12700 | Ordinal-based fused GROUP BY via DocValues |
| FusedScanAggregate | `dqe/.../shard/source/FusedScanAggregate.java` | ~1800 | Fused scalar aggregation (no GROUP BY) |

## Dispatch Priority in `executePlan()` (line 194)

The method unwraps `ProjectNode` → `AggregationNode` first (line 207-213), then checks fused paths in order:

| Priority | Check | Lines | Pattern | Fast Path |
|----------|-------|-------|---------|-----------|
| 1 | `isScalarCountStar()` | 218 | `SELECT COUNT(*) FROM t` | Direct index stats, no pipeline |
| 2 | `extractSortedScanSpec()` | 226 | `SELECT col ORDER BY col LIMIT N` | Lucene native sorted scan with early termination |
| 3 | `FusedScanAggregate.canFuse()` | 237 | Scalar agg (SUM/COUNT/MIN/MAX/AVG, no GROUP BY) over TableScanNode | Flat array DocValues scan |
| 4 | `isBareSingleNumericColumnScan()` | 247 | Bare numeric column scan for COUNT(DISTINCT) | LongOpenHashSet raw dedup |
| 5 | `isBareSingleVarcharColumnScan()` | 254 | Bare varchar column scan for COUNT(DISTINCT) | FixedBitSet ordinal dedup |
| 6 | `FusedScanAggregate.canFuseWithEval()` | 261 | `SUM(col + constant)` patterns | Algebraic shortcut: SUM(col+k) = SUM(col) + k*COUNT(*) |
| 7 | 2-key COUNT(DISTINCT) numeric | 271 | `GROUP BY (k0, k1)` + COUNT(*), both numeric | Per-group LongOpenHashSet |
| 8 | 2-key COUNT(DISTINCT) varchar+numeric | 307 | `GROUP BY (varchar_k0, numeric_k1)` + COUNT(*) | Per-group HashSet with ordinals |
| 9 | 2-key mixed dedup | 318 | `GROUP BY (k0, k1)` + SUM/COUNT mix | Reduces k0×k1 rows to k0 rows |
| 10 | `canFuseWithExpressionKey()` | 340 | `GROUP BY expr(varchar_col)` e.g. REGEXP_REPLACE | Ordinal-cached: eval N ordinals not M docs |
| 11 | `FusedGroupByAggregate.canFuse()` | 356 | Generic GROUP BY with supported types | Ordinal-based DocValues aggregation |
| 12 | Fused GROUP BY + Sort + Limit | 365 | `GROUP BY ... ORDER BY ... LIMIT N` | Fused agg + in-process sort, with top-N optimization |
| 13 | Fused GROUP BY + HAVING + Sort | 600 | `GROUP BY ... HAVING ... ORDER BY ...` | Fused agg + filter + sort |
| 14 | LimitNode → AggregationNode (no sort) | 575 | `GROUP BY ... LIMIT N` | Early-exit after N groups |
| **FALLBACK** | `LocalExecutionPlanner` | 703 | Everything else | Generic pipeline: LucenePageSource → HashAggregationOperator |

## FusedScanAggregate.canFuse() (line 78)

Matches scalar aggregations (no GROUP BY) where:
- Child is plain `TableScanNode`
- All agg functions match pattern: `(COUNT|SUM|MIN|MAX|AVG)((DISTINCT )? arg)`
- Supports: `COUNT(*)`, `SUM(col)`, `MIN(col)`, `MAX(col)`, `AVG(col)`, `COUNT(DISTINCT col)`

`canFuseWithEval()` (line 101) extends this for `EvalNode → TableScanNode` with algebraic identity optimization.

## FusedGroupByAggregate.canFuse() (line 197)

Matches GROUP BY aggregations where:
- Has ≥1 group-by key
- Child is `TableScanNode` or `EvalNode → TableScanNode`
- Group-by keys are: VARCHAR, numeric, timestamp, DATE_TRUNC(ts), EXTRACT(field FROM ts), arithmetic expr, or EvalNode-computed expression
- Agg functions match `(COUNT|SUM|MIN|MAX|AVG)((DISTINCT )? arg)` with resolvable args
- Special: `length(varchar_col)` supported as inline aggregate argument

`canFuseWithExpressionKey()` (line 313) is a specialized path for single computed expression key over VARCHAR, enabling ordinal caching (~58x reduction for REGEXP_REPLACE patterns).

## Patterns That Fall Through to Generic Pipeline

1. **Multi-level plan trees** not matching any recognized pattern (e.g., subqueries, joins)
2. **Unsupported group-by key types** (not VARCHAR/numeric/timestamp)
3. **Unsupported aggregate functions** (e.g., custom UDAFs, window functions)
4. **Complex expression keys** that don't match DATE_TRUNC/EXTRACT/arithmetic/EvalNode patterns
5. **Aggregate arguments referencing non-physical columns** (except `length(varchar_col)`)
6. **Plans with `scanFactory != null`** (non-local execution paths) — all fused paths are skipped
7. **3+ key GROUP BY with COUNT(DISTINCT)** — only 2-key dedup is optimized
8. **VARCHAR-only multi-key dedup** — only varchar+numeric combo is handled
9. **AVG in mixed dedup** — only SUM/COUNT are allowed in the 2-key mixed path

## Optimization Opportunities

1. **3+ key COUNT(DISTINCT)**: Currently falls through; could extend HashSet dedup to N keys
2. **VARCHAR×VARCHAR dedup**: 2-key dedup only handles varchar+numeric; varchar×varchar falls to generic
3. **AVG in mixed dedup**: Could decompose AVG → SUM/COUNT in the 2-key mixed path
4. **Multi-expression GROUP BY**: `canFuseWithExpressionKey` only handles single expression key
5. **HAVING without Sort**: No dedicated fast path for `GROUP BY ... HAVING ...` without ORDER BY
