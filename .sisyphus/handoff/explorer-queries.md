# ClickBench Query Definitions & Baseline Times

**Source**: `benchmarks/clickbench/queries/queries.sql` (43 queries, 0-based indexing Q00–Q42)
**Baseline**: `benchmarks/clickbench/results/performance/clickhouse_parquet_official/c6a.4xlarge.json`
**Baseline format**: `[cold, warm1, warm2]` in seconds

> Run scripts use 1-based indexing (`--query 5` = Q04 in 0-based).

---

## Requested Queries

### Q02 (line 3, 1-based=3)
```sql
SELECT SUM(AdvEngineID), COUNT(*), AVG(ResolutionWidth) FROM hits;
```
- **Baseline**: [0.325, 0.105, 0.106]
- **COUNT(DISTINCT)**: No

### Q04 (line 5, 1-based=5)
```sql
SELECT COUNT(DISTINCT UserID) FROM hits;
```
- **Baseline**: [0.573, 0.434, 0.435]
- **COUNT(DISTINCT)**: ✅ Yes — `COUNT(DISTINCT UserID)`

### Q05 (line 6, 1-based=6)
```sql
SELECT COUNT(DISTINCT SearchPhrase) FROM hits;
```
- **Baseline**: [0.905, 0.702, 0.69]
- **COUNT(DISTINCT)**: ✅ Yes — `COUNT(DISTINCT SearchPhrase)`

### Q08 (line 9, 1-based=9)
```sql
SELECT RegionID, COUNT(DISTINCT UserID) AS u FROM hits GROUP BY RegionID ORDER BY u DESC LIMIT 10;
```
- **Baseline**: [0.729, 0.54, 0.54]
- **COUNT(DISTINCT)**: ✅ Yes — `COUNT(DISTINCT UserID)`

### Q09 (line 10, 1-based=10)
```sql
SELECT RegionID, SUM(AdvEngineID), COUNT(*) AS c, AVG(ResolutionWidth), COUNT(DISTINCT UserID) FROM hits GROUP BY RegionID ORDER BY c DESC LIMIT 10;
```
- **Baseline**: [1.202, 0.614, 0.622]
- **COUNT(DISTINCT)**: ✅ Yes — `COUNT(DISTINCT UserID)`

### Q11 (line 12, 1-based=12)
```sql
SELECT MobilePhone, MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM hits WHERE MobilePhoneModel <> '' GROUP BY MobilePhone, MobilePhoneModel ORDER BY u DESC LIMIT 10;
```
- **Baseline**: [0.597, 0.271, 0.268]
- **COUNT(DISTINCT)**: ✅ Yes — `COUNT(DISTINCT UserID)`

### Q13 (line 14, 1-based=14)
```sql
SELECT SearchPhrase, COUNT(DISTINCT UserID) AS u FROM hits WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY u DESC LIMIT 10;
```
- **Baseline**: [2.217, 0.956, 0.967]
- **COUNT(DISTINCT)**: ✅ Yes — `COUNT(DISTINCT UserID)`

### Q15 (line 16, 1-based=16)
```sql
SELECT UserID, COUNT(*) FROM hits GROUP BY UserID ORDER BY COUNT(*) DESC LIMIT 10;
```
- **Baseline**: [0.671, 0.528, 0.52]
- **COUNT(DISTINCT)**: No

### Q16 (line 17, 1-based=17)
```sql
SELECT UserID, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, SearchPhrase ORDER BY COUNT(*) DESC LIMIT 10;
```
- **Baseline**: [2.628, 1.801, 1.794]
- **COUNT(DISTINCT)**: No

### Q18 (line 19, 1-based=19)
```sql
SELECT UserID, extract(minute FROM EventTime) AS m, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, m, SearchPhrase ORDER BY COUNT(*) DESC LIMIT 10;
```
- **Baseline**: [5.18, 3.668, 3.668]
- **COUNT(DISTINCT)**: No

### Q28 (line 29, 1-based=29)
```sql
SELECT REGEXP_REPLACE(Referer, '^https?://(?:www\.)?([^/]+)/.*$', '\1') AS k, AVG(length(Referer)) AS l, COUNT(*) AS c, MIN(Referer) FROM hits WHERE Referer <> '' GROUP BY k HAVING COUNT(*) > 100000 ORDER BY l DESC LIMIT 25;
```
- **Baseline**: [9.734, 9.526, 9.531]
- **COUNT(DISTINCT)**: No

### Q30 (line 31, 1-based=31)
```sql
SELECT SearchEngineID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits WHERE SearchPhrase <> '' GROUP BY SearchEngineID, ClientIP ORDER BY c DESC LIMIT 10;
```
- **Baseline**: [2.31, 0.779, 0.778]
- **COUNT(DISTINCT)**: No

### Q31 (line 32, 1-based=32)
```sql
SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits WHERE SearchPhrase <> '' GROUP BY WatchID, ClientIP ORDER BY c DESC LIMIT 10;
```
- **Baseline**: [5.842, 1.095, 1.1]
- **COUNT(DISTINCT)**: No

### Q32 (line 33, 1-based=33)
```sql
SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits GROUP BY WatchID, ClientIP ORDER BY c DESC LIMIT 10;
```
- **Baseline**: [6.471, 4.541, 4.493]
- **COUNT(DISTINCT)**: No

### Q35 (line 36, 1-based=36)
```sql
SELECT ClientIP, ClientIP - 1, ClientIP - 2, ClientIP - 3, COUNT(*) AS c FROM hits GROUP BY ClientIP, ClientIP - 1, ClientIP - 2, ClientIP - 3 ORDER BY c DESC LIMIT 10;
```
- **Baseline**: [0.507, 0.37, 0.37]
- **COUNT(DISTINCT)**: No

### Q36 (line 37, 1-based=37)
```sql
SELECT URL, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND DontCountHits = 0 AND IsRefresh = 0 AND URL <> '' GROUP BY URL ORDER BY PageViews DESC LIMIT 10;
```
- **Baseline**: [0.359, 0.126, 0.121]
- **COUNT(DISTINCT)**: No

### Q37 (line 38, 1-based=38)
```sql
SELECT Title, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND DontCountHits = 0 AND IsRefresh = 0 AND Title <> '' GROUP BY Title ORDER BY PageViews DESC LIMIT 10;
```
- **Baseline**: [0.319, 0.102, 0.102]
- **COUNT(DISTINCT)**: No

### Q39 (line 40, 1-based=40)
```sql
SELECT TraficSourceID, SearchEngineID, AdvEngineID, CASE WHEN (SearchEngineID = 0 AND AdvEngineID = 0) THEN Referer ELSE '' END AS Src, URL AS Dst, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND IsRefresh = 0 GROUP BY TraficSourceID, SearchEngineID, AdvEngineID, Src, Dst ORDER BY PageViews DESC LIMIT 10 OFFSET 1000;
```
- **Baseline**: [0.439, 0.143, 0.147]
- **COUNT(DISTINCT)**: No

---

## Summary: COUNT(DISTINCT) Queries

| Query | COUNT(DISTINCT) Column | Baseline (warm) |
|-------|----------------------|-----------------|
| Q04   | UserID               | 0.435s          |
| Q05   | SearchPhrase         | 0.69s           |
| Q08   | UserID               | 0.54s           |
| Q09   | UserID               | 0.622s          |
| Q11   | UserID               | 0.268s          |
| Q13   | UserID               | 0.967s          |

Non-COUNT(DISTINCT) queries: Q02, Q15, Q16, Q18, Q28, Q30, Q31, Q32, Q35, Q36, Q37, Q39

---

## Full Baseline Reference (all 43 queries)

| Q#  | Cold    | Warm1   | Warm2   |
|-----|---------|---------|---------|
| Q00 | 0.036   | 0.025   | 0.025   |
| Q01 | 0.238   | 0.068   | 0.063   |
| Q02 | 0.325   | 0.105   | 0.106   |
| Q03 | 0.383   | 0.116   | 0.111   |
| Q04 | 0.573   | 0.434   | 0.435   |
| Q05 | 0.905   | 0.702   | 0.690   |
| Q06 | 0.235   | 0.069   | 0.065   |
| Q07 | 0.201   | 0.059   | 0.064   |
| Q08 | 0.729   | 0.540   | 0.540   |
| Q09 | 1.202   | 0.614   | 0.622   |
| Q10 | 0.593   | 0.251   | 0.248   |
| Q11 | 0.597   | 0.271   | 0.268   |
| Q12 | 0.928   | 0.644   | 0.653   |
| Q13 | 2.217   | 0.956   | 0.967   |
| Q14 | 1.010   | 0.735   | 0.751   |
| Q15 | 0.671   | 0.528   | 0.520   |
| Q16 | 2.628   | 1.801   | 1.794   |
| Q17 | 2.004   | 1.170   | 1.172   |
| Q18 | 5.180   | 3.668   | 3.668   |
| Q19 | 0.299   | 0.110   | 0.114   |
| Q20 | 9.487   | 1.169   | 1.174   |
| Q21 | 11.069  | 1.340   | 1.334   |
| Q22 | 21.533  | 1.983   | 1.963   |
| Q23 | 54.510  | 4.135   | 4.151   |
| Q24 | 2.438   | 0.497   | 0.493   |
| Q25 | 0.739   | 0.272   | 0.290   |
| Q26 | 2.453   | 0.499   | 0.494   |
| Q27 | 9.605   | 1.795   | 1.790   |
| Q28 | 9.734   | 9.526   | 9.531   |
| Q29 | 0.269   | 0.102   | 0.096   |
| Q30 | 2.310   | 0.779   | 0.778   |
| Q31 | 5.842   | 1.095   | 1.100   |
| Q32 | 6.471   | 4.541   | 4.493   |
| Q33 | 10.530  | 3.107   | 3.113   |
| Q34 | 10.542  | 3.107   | 3.123   |
| Q35 | 0.507   | 0.370   | 0.370   |
| Q36 | 0.359   | 0.126   | 0.121   |
| Q37 | 0.319   | 0.102   | 0.102   |
| Q38 | 0.371   | 0.070   | 0.086   |
| Q39 | 0.439   | 0.143   | 0.147   |
| Q40 | 0.297   | 0.078   | 0.071   |
| Q41 | 0.270   | 0.069   | 0.062   |
| Q42 | 0.237   | 0.054   | 0.057   |
