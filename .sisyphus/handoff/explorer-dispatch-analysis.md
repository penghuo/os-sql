# DQE Dispatch Analysis: 18 Above-2x Queries

## File: `dqe/src/main/java/org/opensearch/sql/dqe/shard/transport/TransportShardExecuteAction.java`
## Method: `executePlan()` (line 194)

## Dispatch Branch Order (priority top-to-bottom)

| Priority | Branch | Lines | Pattern | Method |
|----------|--------|-------|---------|--------|
| 1 | Scalar COUNT(*) | 213 | `AggNode(groupBy=[], aggs=["count(*)"])` → `TableScanNode` | `executeScalarCountStar` (L875) |
| 2 | Sorted Scan | 226 | `LimitNode → [ProjectNode] → SortNode → TableScanNode` | `executeSortedScan` (L2975) |
| 3 | FusedScanAggregate | 237 | `AggNode(no groupBy)` + `canFuse()` | `executeFusedScanAggregate` (L908) |
| 4 | Bare numeric col scan | 251 | Bare `TableScanNode` with single numeric col (SINGLE path) | `executeDistinctValuesScanWithRawSet` (L2505) |
| 5 | Bare varchar col scan | 259 | Bare `TableScanNode` with single varchar col (SINGLE path) | `executeDistinctValuesScanVarcharWithRawSet` (L2535) |
| 6 | FusedScanAggregate+Eval | 266 | `AggNode(no groupBy)` + `canFuseWithEval()` (SUM(col+const)) | `executeFusedEvalAggregate` (L930) |
| 7 | N-key COUNT(DISTINCT) dedup | 280 | `AggNode(PARTIAL, groupBy≥2, COUNT(*))` — 2-key numeric | `executeCountDistinctWithHashSets` (L960) |
| 7b | N-key COUNT(DISTINCT) varchar | 316 | 2-key: varchar key0 + numeric key1 | `executeVarcharCountDistinctWithHashSets` (L2016) |
| 7c | N-key COUNT(DISTINCT) 3+ keys | 332 | 3+ numeric keys | `executeNKeyCountDistinctWithHashSets` (L1248) |
| 7d | Mixed dedup (SUM+COUNT+DISTINCT) | 349 | 2-key mixed aggregates | `executeMixedDedupWithHashSets` (L1537) |
| 8 | Expression GROUP BY | 369 | `AggNode` + `canFuseWithExpressionKey()` (REGEXP_REPLACE etc) | `executeFusedExprGroupByAggregate` (L2349) |
| 9 | FusedGroupByAggregate | 381 | `AggNode` + `canFuse()` with group keys | `executeFusedGroupByAggregate` (L2316) |
| 10 | Sort+Limit+GroupBy (HAVING) | 395-598 | `LimitNode → [ProjectNode] → SortNode → [FilterNode] → AggNode` | `executeFusedGroupByAggregateWithTopN` (L2372) or `executeFusedGroupByAggregate` + SortOperator |
| 11 | Limit+GroupBy (no sort) | 600-622 | `LimitNode → AggNode` (no Sort) | `executeFusedGroupByAggregateWithTopN` (L2372) |
| 12 | Sort+HAVING (no limit) | 625-703 | `[ProjectNode] → SortNode → FilterNode → AggNode` | `executeFusedGroupByAggregate` + HAVING filter + SortOperator |
| 13 | Generic pipeline | 704-740 | Everything else | `LocalExecutionPlanner` → `LucenePageSource` → operator chain |

## PlanFragmenter Decomposition (coordinator/fragment/PlanFragmenter.java)

COUNT(DISTINCT) queries use a **two-level aggregation** pattern:
- **SINGLE step with GROUP BY**: Shard plan becomes `GROUP BY (original_keys + distinct_columns)` with `COUNT(*)` (PARTIAL step). This deduplicates at the shard level.
- **SINGLE step without GROUP BY**: Shard plan strips the aggregation entirely — shards only scan+filter, sending bare column values. The shard dispatch then hits the "bare scan" fast paths (branches 4/5).
- **Mixed aggregates** (COUNT(DISTINCT) + SUM/AVG): Shard plan becomes `GROUP BY (original_keys + distinct_columns)` with partial SUM/COUNT aggregates.

---

## Query-to-Dispatch Mapping (18 Above-2x Queries)

### Q02 (3.34x) — `SELECT SUM(AdvEngineID), COUNT(*), AVG(ResolutionWidth) FROM hits`
- **SQL Pattern**: Scalar aggregation (no GROUP BY), multiple aggs including AVG
- **PlanFragmenter**: PARTIAL step. AVG decomposed to SUM+COUNT at shard level (L398-410)
- **Dispatch Branch**: **#3 FusedScanAggregate** (L237-243)
- **Method**: `executeFusedScanAggregate` → `FusedScanAggregate.execute()`

### Q04 (5.31x) — `SELECT COUNT(DISTINCT UserID) FROM hits`
- **SQL Pattern**: Scalar COUNT(DISTINCT) on numeric column, no GROUP BY
- **PlanFragmenter**: SINGLE step — strips aggregation, shard sends bare `UserID` column
- **Dispatch Branch**: **#4 Bare numeric col scan** (L251-254)
- **Method**: `executeDistinctValuesScanWithRawSet` → `FusedScanAggregate.collectDistinctValuesRaw()`

### Q05 (5.18x) — `SELECT COUNT(DISTINCT SearchPhrase) FROM hits`
- **SQL Pattern**: Scalar COUNT(DISTINCT) on varchar column, no GROUP BY
- **PlanFragmenter**: SINGLE step — strips aggregation, shard sends bare `SearchPhrase` column
- **Dispatch Branch**: **#5 Bare varchar col scan** (L259-262)
- **Method**: `executeDistinctValuesScanVarcharWithRawSet` → `FusedScanAggregate.collectDistinctStringsRaw()`

### Q08 (4.38x) — `SELECT RegionID, COUNT(DISTINCT UserID) AS u FROM hits GROUP BY RegionID ORDER BY u DESC LIMIT 10`
- **SQL Pattern**: Single GROUP BY key (numeric) + COUNT(DISTINCT numeric) + Sort + Limit
- **PlanFragmenter**: SINGLE with GROUP BY → dedup: `GROUP BY (RegionID, UserID)` with `COUNT(*)` (PARTIAL)
- **Dispatch Branch**: **#7 executeCountDistinctWithHashSets** (L280-315) — 2-key numeric path
- **Method**: `executeCountDistinctWithHashSets` — builds per-RegionID HashSets of UserID values

### Q09 (5.28x) — `SELECT RegionID, SUM(AdvEngineID), COUNT(*), AVG(ResolutionWidth), COUNT(DISTINCT UserID) FROM hits GROUP BY RegionID ORDER BY c DESC LIMIT 10`
- **SQL Pattern**: Single GROUP BY + mixed aggregates (SUM, COUNT, AVG, COUNT(DISTINCT))
- **PlanFragmenter**: Mixed dedup → `GROUP BY (RegionID, UserID)` with partial SUM/COUNT aggregates
- **Dispatch Branch**: **#7d Mixed dedup** (L349-365) — 2-key mixed path
- **Method**: `executeMixedDedupWithHashSets` — per-RegionID HashSets + SUM/COUNT accumulators

### Q11 (12.21x) — `SELECT MobilePhone, MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM hits WHERE MobilePhoneModel <> '' GROUP BY MobilePhone, MobilePhoneModel ORDER BY u DESC LIMIT 10`
- **SQL Pattern**: 2 GROUP BY keys (varchar, varchar) + COUNT(DISTINCT numeric) + WHERE + Sort + Limit
- **PlanFragmenter**: SINGLE with GROUP BY → dedup: `GROUP BY (MobilePhone, MobilePhoneModel, UserID)` with `COUNT(*)` (PARTIAL)
- **Dispatch Branch**: **#7c N-key COUNT(DISTINCT) 3+ keys** (L332-345) — 3 keys, but varchar keys may fail `allNumeric` check
- **Likely Fallback**: **#10 Sort+Limit+GroupBy** (L395-598) → `executeFusedGroupByAggregateWithTopN` since the dedup plan has 3 GROUP BY keys with PARTIAL step

### Q13 (7.87x) — `SELECT SearchPhrase, COUNT(DISTINCT UserID) AS u FROM hits WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY u DESC LIMIT 10`
- **SQL Pattern**: Single GROUP BY key (varchar) + COUNT(DISTINCT numeric) + WHERE + Sort + Limit
- **PlanFragmenter**: SINGLE with GROUP BY → dedup: `GROUP BY (SearchPhrase, UserID)` with `COUNT(*)` (PARTIAL)
- **Dispatch Branch**: **#7b VARCHAR COUNT(DISTINCT)** (L316-325) — varchar key0 + numeric key1
- **Method**: `executeVarcharCountDistinctWithHashSets` — ordinal-based varchar grouping with per-group HashSets

### Q14 (2.57x) — `SELECT SearchEngineID, SearchPhrase, COUNT(*) AS c FROM hits WHERE SearchPhrase <> '' GROUP BY SearchEngineID, SearchPhrase ORDER BY c DESC LIMIT 10`
- **SQL Pattern**: 2 GROUP BY keys (numeric, varchar) + COUNT(*) + WHERE + Sort + Limit
- **PlanFragmenter**: PARTIAL step (no COUNT(DISTINCT))
- **Dispatch Branch**: **#10 Sort+Limit+GroupBy** (L395-598) → single sort key is aggregate → `executeFusedGroupByAggregateWithTopN`
- **Method**: `executeFusedGroupByAggregateWithTopN` (L2372)

### Q15 (26.02x) — `SELECT UserID, COUNT(*) FROM hits GROUP BY UserID ORDER BY COUNT(*) DESC LIMIT 10`
- **SQL Pattern**: Single GROUP BY key (numeric) + COUNT(*) + Sort + Limit — HIGH CARDINALITY
- **PlanFragmenter**: PARTIAL step
- **Dispatch Branch**: **#10 Sort+Limit+GroupBy** (L395-598) → sort key is aggregate col → `executeFusedGroupByAggregateWithTopN`
- **Method**: `executeFusedGroupByAggregateWithTopN` — critical for high-cardinality (UserID has ~17M distinct values)

### Q16 (6.21x) — `SELECT UserID, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, SearchPhrase ORDER BY COUNT(*) DESC LIMIT 10`
- **SQL Pattern**: 2 GROUP BY keys (numeric, varchar) + COUNT(*) + Sort + Limit — VERY HIGH CARDINALITY
- **PlanFragmenter**: PARTIAL step
- **Dispatch Branch**: **#10 Sort+Limit+GroupBy** (L395-598) → sort key is aggregate col → `executeFusedGroupByAggregateWithTopN`
- **Method**: `executeFusedGroupByAggregateWithTopN`

### Q18 (9.69x) — `SELECT UserID, extract(minute FROM EventTime) AS m, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, extract(minute FROM EventTime), SearchPhrase ORDER BY COUNT(*) DESC LIMIT 10`
- **SQL Pattern**: 3 GROUP BY keys (numeric, expression, varchar) + COUNT(*) + Sort + Limit
- **PlanFragmenter**: PARTIAL step. Expression key (extract) may produce EvalNode
- **Dispatch Branch**: **#10 Sort+Limit+GroupBy** (L395-598) — checks `canFuseWithExpressionKey` for expression keys
- **Method**: Likely `executeFusedExprGroupByAggregate` or `executeFusedGroupByAggregateWithTopN` depending on expression key handling

### Q27 (2.21x) — `SELECT CounterID, AVG(length(URL)) AS l, COUNT(*) AS c FROM hits WHERE URL <> '' GROUP BY CounterID HAVING COUNT(*) > 100000 ORDER BY l DESC LIMIT 25`
- **SQL Pattern**: Single GROUP BY + AVG(expr) + COUNT(*) + HAVING + Sort + Limit
- **PlanFragmenter**: PARTIAL step. AVG decomposed to SUM+COUNT
- **Dispatch Branch**: **#10 Sort+Limit+GroupBy with HAVING** (L395-598, HAVING branch at ~L440-500)
- **Method**: `executeFusedGroupByAggregate` → `applyHavingFilter` → SortOperator

### Q28 (3.06x) — `SELECT REGEXP_REPLACE(...) AS k, AVG(length(Referer)) AS l, COUNT(*) AS c, MIN(Referer) FROM hits WHERE Referer <> '' GROUP BY REGEXP_REPLACE(...) HAVING COUNT(*) > 100000 ORDER BY l DESC LIMIT 25`
- **SQL Pattern**: Expression GROUP BY key (REGEXP_REPLACE) + AVG(expr) + HAVING + Sort + Limit
- **PlanFragmenter**: PARTIAL step. Expression key → EvalNode
- **Dispatch Branch**: **#12 Sort+HAVING (no limit at shard)** (L625-703) or **#10 with HAVING** — expression key path
- **Method**: `executeFusedExprGroupByAggregate` → `applyHavingFilter` → SortOperator

### Q29 (2.28x) — `SELECT SUM(ResolutionWidth), SUM(ResolutionWidth + 1), ... SUM(ResolutionWidth + 89) FROM hits`
- **SQL Pattern**: Scalar aggregation with 90 SUM(col + constant) expressions, no GROUP BY
- **PlanFragmenter**: PARTIAL step. AVG decomposition not needed (all SUM)
- **Dispatch Branch**: **#6 FusedScanAggregate+Eval** (L266-271) — `canFuseWithEval()` matches SUM(col+const)
- **Method**: `executeFusedEvalAggregate` → `FusedScanAggregate.executeWithEval()`

### Q30 (2.34x) — `SELECT SearchEngineID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits WHERE SearchPhrase <> '' GROUP BY SearchEngineID, ClientIP ORDER BY c DESC LIMIT 10`
- **SQL Pattern**: 2 GROUP BY keys (numeric, numeric) + COUNT(*) + SUM + AVG + WHERE + Sort + Limit
- **PlanFragmenter**: PARTIAL step. AVG decomposed to SUM+COUNT
- **Dispatch Branch**: **#10 Sort+Limit+GroupBy** (L395-598) → sort key is aggregate → `executeFusedGroupByAggregateWithTopN`
- **Method**: `executeFusedGroupByAggregateWithTopN`

### Q35 (3.95x) — `SELECT ClientIP, ClientIP - 1, ClientIP - 2, ClientIP - 3, COUNT(*) AS c FROM hits GROUP BY ClientIP, ClientIP - 1, ClientIP - 2, ClientIP - 3 ORDER BY c DESC LIMIT 10`
- **SQL Pattern**: 4 GROUP BY keys (numeric + expressions) + COUNT(*) + Sort + Limit
- **PlanFragmenter**: PARTIAL step. Expression keys → EvalNode
- **Dispatch Branch**: **#10 Sort+Limit+GroupBy** (L395-598) — checks expression key handling
- **Method**: `executeFusedExprGroupByAggregate` or `executeFusedGroupByAggregateWithTopN`

### Q36 (6.67x) — `SELECT URL, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= ... AND ... GROUP BY URL ORDER BY PageViews DESC LIMIT 10`
- **SQL Pattern**: Single GROUP BY key (varchar) + COUNT(*) + multi-predicate WHERE + Sort + Limit
- **PlanFragmenter**: PARTIAL step. WHERE compiled to Lucene query
- **Dispatch Branch**: **#10 Sort+Limit+GroupBy** (L395-598) → sort key is aggregate → `executeFusedGroupByAggregateWithTopN`
- **Method**: `executeFusedGroupByAggregateWithTopN` — Lucene query filters first, then fused GROUP BY on matching docs

### Q39 (30.34x) — `SELECT URL, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= ... AND IsRefresh = 0 AND IsLink <> 0 AND IsDownload = 0 GROUP BY URL ORDER BY PageViews DESC OFFSET 1000 LIMIT 10`
- **SQL Pattern**: Single GROUP BY key (varchar) + COUNT(*) + multi-predicate WHERE + Sort + OFFSET + Limit
- **PlanFragmenter**: PARTIAL step. OFFSET+LIMIT → topN = 1010
- **Dispatch Branch**: **#10 Sort+Limit+GroupBy** (L395-598) → sort key is aggregate → `executeFusedGroupByAggregateWithTopN`
- **Method**: `executeFusedGroupByAggregateWithTopN`

---

## Summary by Dispatch Category

| Dispatch Path | Queries | Count |
|---------------|---------|-------|
| FusedScanAggregate (scalar, no GROUP BY) | Q02 | 1 |
| FusedScanAggregate+Eval (SUM(col+const)) | Q29 | 1 |
| Bare numeric col scan (COUNT DISTINCT numeric) | Q04 | 1 |
| Bare varchar col scan (COUNT DISTINCT varchar) | Q05 | 1 |
| executeCountDistinctWithHashSets (2-key numeric) | Q08 | 1 |
| executeVarcharCountDistinctWithHashSets (varchar+numeric) | Q13 | 1 |
| executeMixedDedupWithHashSets (mixed aggs) | Q09 | 1 |
| N-key COUNT(DISTINCT) / Sort+Limit fallback | Q11 | 1 |
| Sort+Limit+GroupBy → executeFusedGroupByAggregateWithTopN | Q14, Q15, Q16, Q18, Q30, Q35, Q36, Q39 | 8 |
| Sort+Limit+GroupBy with HAVING | Q27 | 1 |
| Sort+HAVING with expression key | Q28 | 1 |

**Dominant pattern**: 8 of 18 queries (44%) hit the `executeFusedGroupByAggregateWithTopN` path — these are GROUP BY + Sort + Limit queries. The highest speedups (Q15=26x, Q39=30x) are in this category, suggesting the fused top-N path is the biggest performance differentiator for high-cardinality GROUP BY.
