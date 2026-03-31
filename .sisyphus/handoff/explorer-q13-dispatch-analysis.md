# DQE Dispatch Path Analysis: 13 Above-2x ClickBench Queries

Key file: `TransportShardExecuteAction.java` (TSSA), method `executePlan()`.
Dispatch is a sequential if-else chain; first matching path wins.

## Category 1: Scalar COUNT(DISTINCT) — Bare TableScanNode Path

### Q04 (3.77x): `SELECT COUNT(DISTINCT UserID) FROM hits`
- **Calcite decomposition**: SINGLE step → PlanFragmenter strips agg, sends bare `TableScanNode(UserID)`
- **Dispatch**: `isBareSingleNumericColumnScan()` → `executeDistinctValuesScanWithRawSet()` (TSSA ~L225)
- **Execution**: `FusedScanAggregate.collectDistinctValuesRaw()` — builds LongOpenHashSet from DocValues
- **Bottleneck**: 17M+ unique UserIDs → huge LongOpenHashSet (~256MB), GC pressure, coordinator union cost

## Category 2: GROUP BY + COUNT(DISTINCT) — Two-Level Dedup Pattern

PlanFragmenter decomposes `COUNT(DISTINCT col)` into shard plan: `AggregationNode(PARTIAL, groupBy=[origKeys + distinctCol], aggs=[COUNT(*)])`. TSSA checks `aggDedupNode.getStep() == PARTIAL && groupByKeys.size() >= 2 && canFuse()` at ~L237.

### Q08 (4.46x): `GROUP BY RegionID, COUNT(DISTINCT UserID)`
- **Shard plan**: `AggregationNode(PARTIAL, groupBy=[RegionID, UserID], aggs=[COUNT(*)])`
- **Dispatch**: 2-key numeric dedup → `executeCountDistinctWithHashSets()` (TSSA ~L268)
- **Execution**: Parallel segment scan, per-RegionID LongOpenHashSet of UserIDs
- **Bottleneck**: ~400 regions × ~17M UserIDs → large per-group HashSets

### Q09 (5.81x): `GROUP BY RegionID + SUM/COUNT/AVG/COUNT(DISTINCT)`
- **Shard plan**: `AggregationNode(PARTIAL, groupBy=[RegionID, UserID], aggs=[SUM(AdvEngineID), COUNT(*), ...])`
- **Dispatch**: 2-key mixed dedup (`isMixedDedup`) → `executeMixedDedupWithHashSets()` (TSSA ~L305)
- **Execution**: GROUP BY RegionID with per-group HashSet + SUM/COUNT accumulators
- **Bottleneck**: Same as Q08 plus accumulator overhead for SUM/AVG

### Q11 (6.55x): `GROUP BY MobilePhone, MobilePhoneModel + COUNT(DISTINCT UserID)`
- **Shard plan**: `AggregationNode(PARTIAL, groupBy=[MobilePhone, MobilePhoneModel, UserID], aggs=[COUNT(*)])`
- **Dispatch**: 3-key, mixed-type (MobilePhoneModel=VARCHAR) → `executeMixedTypeCountDistinctWithHashSets()` (TSSA ~L293)
- **Execution**: Object-based composite keys, ordinal-based VARCHAR within segment, resolve at end
- **Bottleneck**: Per-doc Object[] allocation for composite keys, cross-segment String resolution

### Q13 (7.96x): `GROUP BY SearchPhrase + COUNT(DISTINCT UserID)`
- **Shard plan**: `AggregationNode(PARTIAL, groupBy=[SearchPhrase, UserID], aggs=[COUNT(*)])`
- **Dispatch**: 2-key, VARCHAR key0 + numeric key1 → `executeVarcharCountDistinctWithHashSets()` (TSSA ~L280)
- **Execution**: Global ordinal map for SearchPhrase, per-ordinal LongOpenHashSet of UserIDs
- **Bottleneck**: ~376K unique SearchPhrases × UserID sets, global ordinal map build cost

### Q14 (2.79x): `WHERE SearchPhrase<>'' GROUP BY SearchEngineID,SearchPhrase + COUNT(*)`
- **Shard plan**: Filtered PARTIAL agg with Sort+Limit
- **Dispatch**: `extractAggFromSortedLimit()` → `FusedGroupByAggregate.canFuse()` → `executeFusedGroupByAggregateWithTopN()` (TSSA ~L340)
- **Execution**: `executeInternal()` → `executeWithVarcharKeys()` (has VARCHAR SearchPhrase key)
- **Bottleneck**: Filtered scan + multi-key VARCHAR grouping + ordinal resolution

## Category 3: High-Cardinality GROUP BY + Sort + Limit

These hit the `extractAggFromSortedLimit()` path (TSSA ~L316), which detects `LimitNode → [ProjectNode] → SortNode → AggregationNode` and routes to fused GROUP BY with top-N.

### Q15 (3.84x): `GROUP BY UserID ORDER BY COUNT(*) DESC LIMIT 10`
- **Dispatch**: Sort on aggregate col (COUNT) → `executeFusedGroupByAggregateWithTopN()` (TSSA ~L340)
- **Execution**: `executeInternal()` → `executeNumericOnly()` → `executeSingleKeyNumeric()` → `executeSingleKeyNumericFlat()` (FGA L5143)
- **Parallelized?**: Yes, but for >10M docs it falls back to sequential scan (FGA ~L5210: `totalDocs > 10_000_000` → sequential for cache locality)
- **Bottleneck**: ~4.4M unique UserIDs → huge flat hash map, L3 cache thrashing

### Q16 (7.04x): `GROUP BY UserID, SearchPhrase ORDER BY COUNT(*) DESC LIMIT 10`
- **Dispatch**: Same `extractAggFromSortedLimit()` → `executeFusedGroupByAggregateWithTopN()`
- **Execution**: `executeInternal()` → has VARCHAR (SearchPhrase) → `executeWithVarcharKeys()` (FGA ~L6900+)
- **Bottleneck**: ~25K unique (UserID, SearchPhrase) pairs per shard, VARCHAR ordinal resolution, HashMap overhead

### Q18 (10.17x): `GROUP BY UserID, EXTRACT(minute FROM EventTime), SearchPhrase ORDER BY COUNT(*) DESC LIMIT 10`
- **Dispatch**: Same `extractAggFromSortedLimit()` path
- **Execution**: `executeInternal()` → has VARCHAR + EXTRACT expression → `executeWithVarcharKeys()`
- **Bottleneck**: 3-key grouping with expression evaluation, ~98K groups, VARCHAR ordinal overhead

## Category 4: Expression GROUP BY (REGEXP_REPLACE + HAVING)

### Q28 (2.35x): `REGEXP_REPLACE GROUP BY HAVING`
- **Dispatch**: `extractAggFromSortedFilter()` (TSSA ~L395) detects `SortNode → FilterNode → AggregationNode`
- **Then**: `canFuseWithExpressionKey()` returns true → `executeFusedExprGroupByAggregate()` (TSSA ~L405)
- **Execution**: `FusedGroupByAggregate.executeWithExpressionKey()` — ordinal-cached REGEXP_REPLACE (~16K evals vs ~921K docs)
- **Bottleneck**: HAVING filter applied post-aggregation, regex evaluation cost even with caching

## Category 5: Full-Table High-Cardinality GROUP BY

### Q32 (3.39x): `GROUP BY WatchID, ClientIP WHERE SearchPhrase<>'' + COUNT/SUM/AVG`
- **Dispatch**: `extractAggFromSortedLimit()` → `executeFusedGroupByAggregateWithTopN()`
- **Execution**: `executeInternal()` → `executeNumericOnly()` → `executeTwoKeyNumeric()` → `executeTwoKeyNumericFlat()` (FGA L6327)
- **Bottleneck**: Filtered but still high cardinality (~100K+ groups), two-key flat hash map

### Q35 (3.75x): `SELECT 1, URL, COUNT(*) GROUP BY 1, URL ORDER BY c DESC LIMIT 10`
- **Dispatch**: `extractAggFromSortedLimit()` → `executeFusedGroupByAggregateWithTopN()`
- **Execution**: `executeInternal()` → has VARCHAR (URL) → `executeWithVarcharKeys()`
- **Bottleneck**: ~21K unique URLs, full-table scan, VARCHAR ordinal resolution across segments

## Category 6: Worst Query — Filtered + CASE WHEN + OFFSET

### Q39 (14.66x): `CounterID=62, 5 GROUP BY keys including CASE WHEN, OFFSET 1000`
```sql
SELECT TraficSourceID, SearchEngineID, AdvEngineID,
  CASE WHEN (SearchEngineID=0 AND AdvEngineID=0) THEN Referer ELSE '' END AS Src,
  URL AS Dst, COUNT(*) AS PageViews
FROM hits WHERE CounterID=62 AND EventDate>=... AND IsRefresh=0
GROUP BY TraficSourceID, SearchEngineID, AdvEngineID,
  CASE WHEN ... END, URL
ORDER BY PageViews DESC OFFSET 1000 LIMIT 10
```
- **Dispatch**: `extractAggFromSortedLimit()` → inner agg has EvalNode (CASE WHEN) → `canFuse()` returns true (eval key detected)
- **Execution**: `executeInternal()` → has VARCHAR keys + eval key → `executeWithEvalKeys()` (FGA L7170)
- **Bottleneck (14.66x — worst)**:
  1. **5 GROUP BY keys** — composite key allocation per doc
  2. **CASE WHEN expression** — per-doc Block materialization for eval
  3. **VARCHAR keys** (Referer, URL) — ordinal resolution overhead
  4. **Narrow filter** (CounterID=62) — few matching docs but full expression eval pipeline
  5. **OFFSET 1000** — must compute all groups, sort, skip 1000

## Dispatch Decision Points (Line Numbers in TransportShardExecuteAction.java)

| Line | Check | Queries |
|------|-------|---------|
| ~L198 | `isScalarCountStar()` | — |
| ~L210 | `extractSortedScanSpec()` | — |
| ~L218 | `FusedScanAggregate.canFuse()` (scalar agg) | — |
| ~L225 | `isBareSingleNumericColumnScan()` | Q04 |
| ~L231 | `isBareSingleVarcharColumnScan()` | — |
| ~L237 | N-key dedup: `PARTIAL + groupByKeys>=2 + canFuse()` | Q08,Q09,Q11,Q13 |
| ~L316 | `extractAggFromSortedLimit()` + `canFuse()` | Q14,Q15,Q16,Q18,Q32,Q35,Q39 |
| ~L380 | `extractAggFromLimit()` (no Sort) | — |
| ~L395 | `extractAggFromSortedFilter()` (HAVING) | Q28 |

## Summary: Same Path Queries

- **Q16, Q18, Q35**: All use `extractAggFromSortedLimit()` → `executeWithVarcharKeys()` (VARCHAR GROUP BY key)
- **Q15, Q32**: Both use `extractAggFromSortedLimit()` → numeric-only flat paths (`executeSingleKeyNumericFlat` / `executeTwoKeyNumericFlat`)
- **Q08, Q09, Q11, Q13**: All use the N-key dedup path but different sub-dispatches (2-key numeric, mixed dedup, mixed-type, varchar+numeric)
- **Q39**: Unique — only query hitting `executeWithEvalKeys()` due to CASE WHEN
