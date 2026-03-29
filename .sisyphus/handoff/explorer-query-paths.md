# ClickBench Queries Above 2x Ratio — SQL Text & Execution Paths

## COUNT(DISTINCT) Queries

### Q04 (line 5) — Scalar COUNT(DISTINCT) numeric
```sql
SELECT COUNT(DISTINCT UserID) FROM hits;
```
**Path**: PlanFragmenter strips AggregationNode → bare `TableScanNode` with single numeric column → `isBareSingleNumericColumnScan()` → `executeDistinctValuesScanWithRawSet()` (LongOpenHashSet per shard, coordinator unions).

### Q05 (line 6) — Scalar COUNT(DISTINCT) varchar
```sql
SELECT COUNT(DISTINCT SearchPhrase) FROM hits;
```
**Path**: Bare `TableScanNode` with single VARCHAR column → `isBareSingleVarcharColumnScan()` → `executeDistinctValuesScanVarcharWithRawSet()` (ordinal-based FixedBitSet dedup, raw string set attached).

### Q08 (line 9) — GROUP BY + COUNT(DISTINCT), 2 numeric keys
```sql
SELECT RegionID, COUNT(DISTINCT UserID) AS u FROM hits GROUP BY RegionID ORDER BY u DESC LIMIT 10;
```
**Path**: AggregationNode with 2 keys (RegionID=groupKey, UserID=dedupKey), both numeric, single COUNT(*) → `executeCountDistinctWithHashSets()` (per-group LongOpenHashSet). Then sort+limit via `extractAggFromSortedLimit` → fused top-N path.

### Q09 (line 10) — GROUP BY + mixed aggs + COUNT(DISTINCT)
```sql
SELECT RegionID, SUM(AdvEngineID), COUNT(*) AS c, AVG(ResolutionWidth), COUNT(DISTINCT UserID) FROM hits GROUP BY RegionID ORDER BY c DESC LIMIT 10;
```
**Path**: 2 keys, mixed SUM/COUNT aggregates → `isMixedDedup=true` → `executeMixedDedupWithHashSets()` (per-group HashSet for key1 + accumulators for SUM/COUNT). Then sort+limit.

### Q11 (line 12) — GROUP BY + COUNT(DISTINCT), 3 keys
```sql
SELECT MobilePhone, MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM hits WHERE MobilePhoneModel <> '' GROUP BY MobilePhone, MobilePhoneModel ORDER BY u DESC LIMIT 10;
```
**Path**: AggregationNode has **3 keys** (MobilePhone, MobilePhoneModel, UserID) after COUNT(DISTINCT) decomposition. The 2-key dedup fast path requires exactly 2 keys → **FALLS THROUGH** to `FusedGroupByAggregate.canFuse()` (ordinal-based GROUP BY) or generic operator pipeline if canFuse fails.

### Q13 (line 14) — VARCHAR key + COUNT(DISTINCT) numeric
```sql
SELECT SearchPhrase, COUNT(DISTINCT UserID) AS u FROM hits WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY u DESC LIMIT 10;
```
**Path**: 2 keys (SearchPhrase=VARCHAR, UserID=numeric), single COUNT(*) → VARCHAR key0 + numeric key1 branch → `executeVarcharCountDistinctWithHashSets()`.

## Non-COUNT(DISTINCT) High-Cardinality GROUP BY Queries

### Q18 (line 19) — 3-key GROUP BY, ~50M groups
```sql
SELECT UserID, extract(minute FROM EventTime) AS m, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, extract(minute FROM EventTime), SearchPhrase ORDER BY COUNT(*) DESC LIMIT 10;
```
**Path**: `extractAggFromSortedLimit()` finds LimitNode→SortNode→AggregationNode. Inner agg has expression key (`extract(minute FROM EventTime)`) → checks `FusedGroupByAggregate.canFuse()`. If canFuse succeeds: fused top-N path (`executeFusedGroupByAggregateWithTopN`). Sort key is COUNT(*) (aggregate column) → single-sort-key fused top-N. **Bottleneck**: ~50M groups must all be accumulated before top-N selection.

### Q35 (line 36) — GROUP BY 1, URL (18M unique URLs)
```sql
SELECT 1, URL, COUNT(*) AS c FROM hits GROUP BY 1, URL ORDER BY c DESC LIMIT 10;
```
**Path**: Same as Q18 pattern — `extractAggFromSortedLimit()` → `FusedGroupByAggregate.canFuse()` → fused GROUP BY with top-N. **Bottleneck**: 18M unique URLs = 18M groups, all accumulated in ordinal-based hash map.

### Q36 (line 37) — GROUP BY ClientIP expressions (4 keys)
```sql
SELECT ClientIP, ClientIP - 1, ClientIP - 2, ClientIP - 3, COUNT(*) AS c FROM hits GROUP BY ClientIP, ClientIP - 1, ClientIP - 2, ClientIP - 3 ORDER BY c DESC LIMIT 10;
```
**Path**: Has expression keys (ClientIP - 1, etc.) → `canFuseWithExpressionKey()` check first (but that handles single expression key). With 4 keys including expressions, likely falls to `FusedGroupByAggregate.canFuse()` or **generic operator pipeline** (LucenePageSource → HashAggregationOperator). The expressions may prevent fused path matching.

### Q39 (line 40) — 5 keys + CASE WHEN
```sql
SELECT TraficSourceID, SearchEngineID, AdvEngineID, CASE WHEN (SearchEngineID = 0 AND AdvEngineID = 0) THEN Referer ELSE '' END AS Src, URL AS Dst, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND IsRefresh = 0 GROUP BY TraficSourceID, SearchEngineID, AdvEngineID, CASE WHEN ... END, URL ORDER BY PageViews DESC OFFSET 1000 LIMIT 10;
```
**Path**: 5 group-by keys including CASE WHEN expression + URL. Complex expression keys likely prevent all fused paths → **generic operator pipeline** (LucenePageSource → HashAggregationOperator → SortOperator). OFFSET 1000 adds additional overhead.

## Dispatch Priority Summary

| Priority | Check | Queries Matched |
|----------|-------|----------------|
| 1 | `isScalarCountStar` | — |
| 2 | `extractSortedScanSpec` (sorted scan) | — |
| 3 | `FusedScanAggregate.canFuse` (scalar agg) | — |
| 4 | `isBareSingleNumericColumnScan` | Q04 |
| 5 | `isBareSingleVarcharColumnScan` | Q05 |
| 6 | `FusedScanAggregate.canFuseWithEval` | — |
| 7 | 2-key COUNT(DISTINCT) dedup (numeric) | Q08, Q09 |
| 8 | 2-key COUNT(DISTINCT) dedup (varchar+numeric) | Q13 |
| 9 | `canFuseWithExpressionKey` | — |
| 10 | `FusedGroupByAggregate.canFuse` | Q11(?), Q18, Q35 |
| 11 | `extractAggFromSortedLimit` + fused top-N | Q18, Q35 |
| 12 | `extractAggFromLimit` (no sort) | — |
| 13 | **Generic pipeline** (fallback) | Q36(?), Q39 |
