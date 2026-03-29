# Q04 & Q05 Dispatch Trace

## Queries
- **Q04**: `SELECT COUNT(DISTINCT UserID) FROM hits` — UserID is `long` → `BigintType`
- **Q05**: `SELECT COUNT(DISTINCT SearchPhrase) FROM hits` — SearchPhrase is `keyword` → `VarcharType`

## PlanFragmenter (scalar COUNT(DISTINCT), no GROUP BY)

Both queries have `aggNode.getGroupByKeys().isEmpty() == true` and `Step == SINGLE`.

`buildShardPlan()` flow (PlanFragmenter.java:110-169):
1. Skips PARTIAL branch (step is SINGLE).
2. Skips GROUP BY dedup branch (line 150: `!aggNode.getGroupByKeys().isEmpty()` is false).
3. Falls through to line 168: `return aggNode.getChild()` — **strips the AggregationNode**.

**Shard plan**: bare `TableScanNode` scanning only the single column.
**Coordinator plan**: `AggregationNode(Step.SINGLE)` with `COUNT(DISTINCT col)` — coordinator runs full aggregation over concatenated shard results.

## TransportShardExecuteAction (lines 246-261)

The shard receives a bare `TableScanNode` (no AggregationNode wrapping it), so `scanFactory == null`.

### Q04 (UserID → BigintType): hits `isBareSingleNumericColumnScan` ✅
- Line 251: `isBareSingleNumericColumnScan(plan)` returns **true** (BigintType matches).
- Dispatches to `executeDistinctValuesScanWithRawSet()` (line 1867).
- Collects distinct longs into `LongOpenHashSet`, attaches raw set to response via `setScalarDistinctSet()`.

### Q05 (SearchPhrase → VarcharType): hits `isBareSingleVarcharColumnScan` ✅
- Line 251: `isBareSingleNumericColumnScan(plan)` returns **false** (VarcharType not numeric).
- Line 259: `isBareSingleVarcharColumnScan(plan)` returns **true** (VarcharType matches).
- Dispatches to `executeDistinctValuesScanVarcharWithRawSet()` (line 1897).
- Collects distinct strings into `HashSet<String>`, attaches via `setScalarDistinctStrings()`.

## Key Files
| File | Lines | Purpose |
|------|-------|---------|
| `dqe/.../fragment/PlanFragmenter.java` | 110-169 | `buildShardPlan()` strips AggregationNode for SINGLE scalar |
| `dqe/.../fragment/PlanFragmenter.java` | 517-534 | `buildCoordinatorPlan()` creates SINGLE coordinator agg |
| `dqe/.../transport/TransportShardExecuteAction.java` | 246-261 | Bare scan dispatch: numeric vs varchar fast paths |
| `dqe/.../transport/TransportShardExecuteAction.java` | 1781-1815 | `isBareSingleNumericColumnScan` / `isBareSingleVarcharColumnScan` |
| `dqe/.../transport/TransportShardExecuteAction.java` | 1867-1930 | Raw set collection methods |
| `integ-test/.../clickbench_index_mapping.json` | 236,305 | SearchPhrase=keyword, UserID=long |
