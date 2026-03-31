# ClickBench Above-2x Queries Analysis

Source: `benchmarks/clickbench/queries/queries_trino.sql` (1-based line numbering: line 1 = Q00)

## Category 1: COUNT(DISTINCT) — Full Table Scans

**Q04** (line 5):
```sql
SELECT COUNT(DISTINCT UserID) FROM hits;
```
- Full-table COUNT(DISTINCT) on high-cardinality column. No filter, no GROUP BY.

**Q07** (line 8):
```sql
SELECT AdvEngineID, COUNT(*) FROM hits WHERE AdvEngineID <> 0 GROUP BY AdvEngineID ORDER BY COUNT(*) DESC;
```
- Filtered GROUP BY + ORDER BY on aggregation result. Low cardinality grouping but requires sort.

## Category 2: COUNT(DISTINCT) + GROUP BY (High Cardinality)

**Q08** (line 9):
```sql
SELECT RegionID, COUNT(DISTINCT UserID) AS u FROM hits GROUP BY RegionID ORDER BY u DESC LIMIT 10;
```
- COUNT(DISTINCT UserID) grouped by RegionID. High cardinality on both dimensions.

**Q09** (line 10):
```sql
SELECT RegionID, SUM(AdvEngineID), COUNT(*) AS c, AVG(ResolutionWidth), COUNT(DISTINCT UserID) FROM hits GROUP BY RegionID ORDER BY c DESC LIMIT 10;
```
- Mixed aggregations (SUM, COUNT, AVG, COUNT DISTINCT) grouped by RegionID. Multiple agg functions + COUNT(DISTINCT).

**Q11** (line 12):
```sql
SELECT MobilePhone, MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM hits WHERE MobilePhoneModel <> '' GROUP BY MobilePhone, MobilePhoneModel ORDER BY u DESC LIMIT 10;
```
- COUNT(DISTINCT) with multi-column GROUP BY + string filter.

**Q13** (line 14):
```sql
SELECT SearchPhrase, COUNT(DISTINCT UserID) AS u FROM hits WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY u DESC LIMIT 10;
```
- COUNT(DISTINCT UserID) grouped by high-cardinality string column (SearchPhrase).

## Category 3: High-Cardinality GROUP BY (no COUNT DISTINCT)

**Q14** (line 15):
```sql
SELECT SearchEngineID, SearchPhrase, COUNT(*) AS c FROM hits WHERE SearchPhrase <> '' GROUP BY SearchEngineID, SearchPhrase ORDER BY c DESC LIMIT 10;
```
- Multi-column GROUP BY with high-cardinality SearchPhrase. No COUNT(DISTINCT) but huge group count.

**Q15** (line 16):
```sql
SELECT UserID, COUNT(*) FROM hits GROUP BY UserID ORDER BY COUNT(*) DESC LIMIT 10;
```
- GROUP BY on very high-cardinality UserID (full table scan, millions of groups).

**Q16** (line 17):
```sql
SELECT UserID, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, SearchPhrase ORDER BY COUNT(*) DESC LIMIT 10;
```
- Two high-cardinality columns in GROUP BY. Combinatorial explosion of groups.

**Q18** (line 19):
```sql
SELECT UserID, extract(minute FROM EventTime) AS m, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, extract(minute FROM EventTime), SearchPhrase ORDER BY COUNT(*) DESC LIMIT 10;
```
- Three-column GROUP BY including function extraction. Extremely high group count.

## Category 4: Wide Aggregation (Many Columns)

**Q32** (line 33):
```sql
SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits GROUP BY WatchID, ClientIP ORDER BY c DESC LIMIT 10;
```
- GROUP BY on WatchID (near-unique) + ClientIP. Essentially as many groups as rows.

## Category 5: High-Cardinality String GROUP BY

**Q35** (line 36):
```sql
SELECT 1, URL, COUNT(*) AS c FROM hits GROUP BY 1, URL ORDER BY c DESC LIMIT 10;
```
- GROUP BY on URL (very high cardinality string). The `GROUP BY 1` is a constant, so effectively single high-cardinality grouping.

## Category 6: Complex Filtered + Multi-Column GROUP BY

**Q39** (line 40):
```sql
SELECT TraficSourceID, SearchEngineID, AdvEngineID, CASE WHEN (SearchEngineID = 0 AND AdvEngineID = 0) THEN Referer ELSE '' END AS Src, URL AS Dst, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= DATE '2013-07-01' AND EventDate <= DATE '2013-07-31' AND IsRefresh = 0 GROUP BY TraficSourceID, SearchEngineID, AdvEngineID, CASE WHEN (SearchEngineID = 0 AND AdvEngineID = 0) THEN Referer ELSE '' END, URL ORDER BY PageViews DESC OFFSET 1000 LIMIT 10;
```
- 5-column GROUP BY with CASE expression, date range filter, OFFSET. Complex query plan.

---

## Summary by Pattern

| Pattern | Queries | Count |
|---------|---------|-------|
| COUNT(DISTINCT) full scan | Q04 | 1 |
| COUNT(DISTINCT) + GROUP BY | Q08, Q09, Q11, Q13 | 4 |
| High-cardinality GROUP BY (no DISTINCT) | Q07, Q14, Q15, Q16, Q18, Q32, Q35 | 7 |
| Complex multi-column GROUP BY + CASE/OFFSET | Q39 | 1 |

**Key Insight**: 12 of 13 above-2x queries involve GROUP BY with high-cardinality columns (UserID, SearchPhrase, URL, WatchID). 5 of 13 also use COUNT(DISTINCT). The dominant bottleneck pattern is **hash aggregation over high-cardinality groups**, especially when combined with COUNT(DISTINCT) which requires per-group distinct tracking.
