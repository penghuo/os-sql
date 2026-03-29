# ClickBench Above-2x Query Dispatch Analysis

## Dispatch Paths Available (in order of precedence in executePlan)

1. **ScalarCountStar** — `COUNT(*)` only, no GROUP BY → `searcher.count()`
2. **SortedScan** — `LIMIT+ORDER BY` on bare scan → Lucene native sort
3. **FusedScanAgg** — scalar aggs (SUM/COUNT/MIN/MAX/AVG), no GROUP BY → DocValues single-pass
4. **BareScanNumeric** — bare TableScanNode, 1 numeric col → raw HashSet for COUNT(DISTINCT)
5. **BareScanVarchar** — bare TableScanNode, 1 varchar col → ordinal-based dedup
6. **FusedEvalAgg** — scalar SUM(col+k) pattern → algebraic shortcut
7. **CountDistinctHashSet** — 2-key numeric dedup (GROUP BY key + DISTINCT key, COUNT(*))
8. **VarcharCountDistinctHashSet** — VARCHAR key0 + numeric key1 dedup
9. **NKeyCountDistinctHashSet** — 3+ numeric key dedup
10. **MixedDedupHashSet** — 2-key numeric dedup with mixed SUM/COUNT aggs
11. **FusedExprGroupBy** — single expression key (REGEXP_REPLACE) with ordinal caching
12. **FusedGroupBy** — ordinal-based GROUP BY (no sort/limit wrapper)
13. **FusedGroupBy+Sort+Limit** — GROUP BY with sort+limit, optional HAVING, optional top-N
14. **FusedGroupBy+Limit** — GROUP BY with limit only (no sort)
15. **FusedGroupBy+HAVING** — GROUP BY with HAVING filter (no limit)
16. **GenericPipeline** — fallback: LucenePageSource → HashAggregationOperator

## Query Dispatch Table

| Q# | Ratio | SQL Summary | Shard Plan Shape | Dispatch Path | Why Slow |
|----|-------|-------------|------------------|---------------|----------|
| Q02 | 3.26x | `SUM, COUNT(*), AVG` scalar | AggNode(PARTIAL)→Scan | **FusedScanAgg** ✅ | Full table scan; AVG may block flat-array path |
| Q04 | 5.57x | `COUNT(DISTINCT UserID)` scalar | bare Scan(UserID) | **BareScanNumeric** ✅ | Ships raw HashSet (~18M longs) to coordinator |
| Q05 | 5.13x | `COUNT(DISTINCT SearchPhrase)` scalar | bare Scan(SearchPhrase) | **BareScanVarchar** ✅ | Ships raw string set to coordinator |
| Q08 | 4.15x | `GB RegionID + COUNT(DISTINCT UserID)` +SORT+LIMIT | AggNode(PARTIAL, gb=[RegionID,UserID], COUNT(*)) | **CountDistinctHashSet** ✅ | Per-region HashSet of UserIDs; ~18M values across ~200 regions |
| Q09 | 5.57x | `GB RegionID + SUM,COUNT,AVG,COUNT(DISTINCT)` +SORT+LIMIT | AggNode(PARTIAL, gb=[RegionID,UserID], SUM+COUNT) | **MixedDedupHashSet** ✅ | Mixed dedup: per-region HashSet + accumulators |
| Q11 | 12.04x | `GB MobilePhone,MobilePhoneModel + COUNT(DISTINCT UserID)` +WHERE+SORT+LIMIT | AggNode(PARTIAL, gb=[MobilePhone,MobilePhoneModel,UserID], COUNT(*)) | **FusedGroupBy+Sort+Limit** ⚠️ | 3-key dedup with 2 VARCHAR keys; high cardinality after WHERE |
| Q13 | 7.96x | `GB SearchPhrase + COUNT(DISTINCT UserID)` +WHERE+SORT+LIMIT | AggNode(PARTIAL, gb=[SearchPhrase,UserID], COUNT(*)) | **VarcharCountDistinctHashSet** ✅ | VARCHAR key0 + numeric key1 dedup; high cardinality SearchPhrase |
| Q14 | 2.45x | `GB SearchEngineID,SearchPhrase + COUNT(*)` +WHERE+SORT+LIMIT | AggNode(PARTIAL)→Scan | **FusedGroupBy+Sort+Limit** ✅ | 2-key GB with WHERE filter; moderate cardinality |
| Q15 | 26.91x | `GB UserID + COUNT(*)` +SORT+LIMIT | AggNode(PARTIAL)→Scan | **FusedGroupBy+Sort+Limit** (top-N) ✅ | ~18M groups; top-N helps but still scans all docs |
| Q16 | 6.79x | `GB UserID,SearchPhrase + COUNT(*)` +SORT+LIMIT | AggNode(PARTIAL)→Scan | **FusedGroupBy+Sort+Limit** (top-N) ✅ | ~25M+ groups; extreme cardinality 2-key GB |
| Q18 | 9.70x | `GB UserID,EXTRACT(min,EventTime),SearchPhrase + COUNT(*)` +SORT+LIMIT | AggNode(PARTIAL)→Scan | **FusedGroupBy+Sort+Limit** ✅ | 3-key GB with EXTRACT; massive cardinality |
| Q27 | 2.38x | `GB CounterID + AVG(length(URL)),COUNT(*)` +WHERE+HAVING+SORT+LIMIT | AggNode(PARTIAL)→Scan | **FusedGroupBy+Sort+Limit+HAVING** ✅ | HAVING post-filter; length() inline eval |
| Q28 | 3.13x | `GB REGEXP_REPLACE(Referer) + AVG(length),COUNT(*),MIN` +WHERE+HAVING+SORT+LIMIT | AggNode(PARTIAL)→EvalNode→Scan | **FusedExprGroupBy+HAVING** ✅ | REGEXP ordinal caching; MIN(varchar) overhead |
| Q29 | 2.01x | 90× `SUM(ResolutionWidth+k)` scalar | AggNode(PARTIAL)→EvalNode→Scan | **FusedEvalAgg** ✅ | 90 aggs; algebraic shortcut but still heavy |
| Q30 | 2.44x | `GB SearchEngineID,ClientIP + COUNT(*),SUM,AVG` +WHERE+SORT+LIMIT | AggNode(PARTIAL)→Scan | **FusedGroupBy+Sort+Limit** ✅ | 2-key GB with WHERE; moderate cardinality |
| Q35 | 3.96x | `GB ClientIP,ClientIP-1,-2,-3 + COUNT(*)` +SORT+LIMIT | AggNode(PARTIAL)→Scan | **FusedGroupBy+Sort+Limit** ✅ | 4 expression keys; all derivable from ClientIP (redundant) |
| Q36 | 5.83x | `GB URL + COUNT(*)` +WHERE(multi)+SORT+LIMIT | AggNode(PARTIAL)→Scan | **FusedGroupBy+Sort+Limit** ✅ | High-cardinality VARCHAR GB (URL) with complex WHERE |
| Q39 | 27.80x | `GB 5 keys(incl CASE WHEN) + COUNT(*)` +WHERE+SORT+OFFSET+LIMIT | AggNode(PARTIAL)→EvalNode→Scan | **FusedGroupBy+Sort+Limit** ✅ | 5-key GB with CASE WHEN + OFFSET; extreme cardinality |

## Bottleneck Categories

### A. High-Cardinality GROUP BY (biggest impact: Q15=27x, Q39=28x, Q18=10x, Q16=7x)
**Queries**: Q15, Q16, Q18, Q35, Q39
All hit fused GROUP BY + top-N but suffer from millions of groups:
- Q15: ~18M UserID groups → top-N prunes output but still builds all groups
- Q39: 5-key GB → combinatorial explosion of groups
- Opportunity: **Streaming/approximate top-N**, **segment-level group pruning**, or **ClickHouse-style partial sort with early termination**

### B. COUNT(DISTINCT) Dedup Overhead (Q04=6x, Q05=5x, Q08=4x, Q09=6x, Q11=12x, Q13=8x)
**Queries**: Q04, Q05, Q08, Q09, Q11, Q13
SINGLE step forces raw value shipping or dedup GROUP BY expansion:
- Q04/Q05: Ship entire HashSet/StringSet to coordinator
- Q08/Q09: Per-group HashSets work but still heavy
- Q11: 3-key dedup with VARCHAR keys → falls to generic FusedGroupBy, not specialized dedup
- Opportunity: **HyperLogLog approximation**, **bloom filter pre-filtering**, or **better VARCHAR dedup paths**

### C. Already Optimized, Marginal Gains (Q02=3x, Q14=2x, Q27=2x, Q28=3x, Q29=2x, Q30=2x, Q36=6x)
**Queries**: Q02, Q14, Q27, Q28, Q29, Q30, Q36
All hit appropriate fast paths. Remaining gap is likely:
- Per-doc overhead in DocValues iteration vs ClickHouse columnar storage
- WHERE filter evaluation cost
- Q29: 90 aggregates even with algebraic shortcut
- Q36: VARCHAR GROUP BY on URL (high cardinality + complex WHERE)

## Top 5 Optimization Opportunities (by expected impact)

1. **Q15/Q39 (27-28x)**: High-cardinality GB with small LIMIT. Current top-N still materializes all groups before selecting top. A **streaming top-N** that prunes groups during accumulation (not after) could dramatically reduce memory and time.

2. **Q11 (12x)**: 3-key dedup (2 VARCHAR + 1 numeric) falls to generic FusedGroupBy instead of specialized dedup path. Adding a **VARCHAR multi-key dedup** fast path would help.

3. **Q18 (10x)**: 3-key GB with EXTRACT expression. EXTRACT is evaluated per-doc. **Caching EXTRACT results per unique timestamp** (like ordinal caching for REGEXP_REPLACE) could reduce evaluations.

4. **Q13 (8x)**: VARCHAR key + COUNT(DISTINCT) dedup. Already has VarcharCountDistinctHashSet path but still 8x. May need **ordinal-based dedup** for the VARCHAR key to avoid string hashing.

5. **Q35 (4x)**: 4 expression keys (ClientIP-1, -2, -3) are all derivable from ClientIP. **Detecting redundant derived keys** and using single-key GB would eliminate 3/4 of key computation.
