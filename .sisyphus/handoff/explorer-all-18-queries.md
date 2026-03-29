# All 18 Failing Queries (>2x vs ClickHouse)

Source data:
- CH baseline: `benchmarks/clickbench/results/performance/clickhouse_parquet_official/c6a.4xlarge.json`
- DQE result: `benchmarks/clickbench/results/performance/opensearch_phaseD/m5.8xlarge.json` (2026-03-21)
- Queries: `benchmarks/clickbench/queries/queries_trino.sql`
- Note: CH on c6a.4xlarge vs OS on m5.8xlarge (different instance types)

## Summary Table

| Q# | Line# | CH Best (s) | OS Best (s) | Ratio | Category |
|-----|-------|-------------|-------------|-------|----------|
| Q02 | 3 | 0.105 | 0.232 | 2.2x | aggregation |
| Q04 | 5 | 0.434 | 2.334 | 5.4x | COUNT(DISTINCT) full-scan |
| Q05 | 6 | 0.690 | 3.979 | 5.8x | COUNT(DISTINCT) full-scan |
| Q08 | 9 | 0.540 | 1.858 | 3.4x | high-card GROUP BY + COUNT(DISTINCT) |
| Q09 | 10 | 0.614 | 4.902 | 8.0x | high-card GROUP BY + COUNT(DISTINCT) |
| Q11 | 12 | 0.268 | 2.461 | 9.2x | high-card GROUP BY + COUNT(DISTINCT) |
| Q13 | 14 | 0.956 | 5.965 | 6.2x | high-card GROUP BY + COUNT(DISTINCT) |
| Q15 | 16 | 0.520 | 2.126 | 4.1x | GROUP BY aggregation |
| Q16 | 17 | 1.794 | 8.801 | 4.9x | high-cardinality GROUP BY |
| Q18 | 19 | 3.668 | 21.702 | 5.9x | GROUP BY aggregation |
| Q28 | 29 | 9.526 | 25.531 | 2.7x | REGEXP + GROUP BY + HAVING |
| Q30 | 31 | 0.778 | 1.827 | 2.3x | high-cardinality GROUP BY |
| Q31 | 32 | 1.095 | 2.230 | 2.0x | high-cardinality GROUP BY |
| Q32 | 33 | 4.493 | 14.268 | 3.2x | high-cardinality GROUP BY |
| Q35 | 36 | 0.370 | 1.862 | 5.0x | high-cardinality GROUP BY |
| Q36 | 37 | 0.121 | 0.487 | 4.0x | high-cardinality GROUP BY |
| Q37 | 38 | 0.102 | 0.280 | 2.7x | GROUP BY aggregation |
| Q39 | 40 | 0.143 | 2.422 | 16.9x | high-cardinality GROUP BY |

## Full SQL for Each Query

### Q02 (line 3) — 2.2x — aggregation

- CH best: 0.105s | OS best: 0.232s

```sql
SELECT SUM(AdvEngineID), COUNT(*), AVG(ResolutionWidth) FROM hits;
```

### Q04 (line 5) — 5.4x — COUNT(DISTINCT) full-scan

- CH best: 0.434s | OS best: 2.334s

```sql
SELECT COUNT(DISTINCT UserID) FROM hits;
```

### Q05 (line 6) — 5.8x — COUNT(DISTINCT) full-scan

- CH best: 0.690s | OS best: 3.979s

```sql
SELECT COUNT(DISTINCT SearchPhrase) FROM hits;
```

### Q08 (line 9) — 3.4x — high-card GROUP BY + COUNT(DISTINCT)

- CH best: 0.540s | OS best: 1.858s

```sql
SELECT RegionID, COUNT(DISTINCT UserID) AS u FROM hits GROUP BY RegionID ORDER BY u DESC LIMIT 10;
```

### Q09 (line 10) — 8.0x — high-card GROUP BY + COUNT(DISTINCT)

- CH best: 0.614s | OS best: 4.902s

```sql
SELECT RegionID, SUM(AdvEngineID), COUNT(*) AS c, AVG(ResolutionWidth), COUNT(DISTINCT UserID) FROM hits GROUP BY RegionID ORDER BY c DESC LIMIT 10;
```

### Q11 (line 12) — 9.2x — high-card GROUP BY + COUNT(DISTINCT)

- CH best: 0.268s | OS best: 2.461s

```sql
SELECT MobilePhone, MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM hits WHERE MobilePhoneModel <> '' GROUP BY MobilePhone, MobilePhoneModel ORDER BY u DESC LIMIT 10;
```

### Q13 (line 14) — 6.2x — high-card GROUP BY + COUNT(DISTINCT)

- CH best: 0.956s | OS best: 5.965s

```sql
SELECT SearchPhrase, COUNT(DISTINCT UserID) AS u FROM hits WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY u DESC LIMIT 10;
```

### Q15 (line 16) — 4.1x — GROUP BY aggregation

- CH best: 0.520s | OS best: 2.126s

```sql
SELECT UserID, COUNT(*) FROM hits GROUP BY UserID ORDER BY COUNT(*) DESC LIMIT 10;
```

### Q16 (line 17) — 4.9x — high-cardinality GROUP BY

- CH best: 1.794s | OS best: 8.801s

```sql
SELECT UserID, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, SearchPhrase ORDER BY COUNT(*) DESC LIMIT 10;
```

### Q18 (line 19) — 5.9x — GROUP BY aggregation

- CH best: 3.668s | OS best: 21.702s

```sql
SELECT UserID, extract(minute FROM EventTime) AS m, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, extract(minute FROM EventTime), SearchPhrase ORDER BY COUNT(*) DESC LIMIT 10;
```

### Q28 (line 29) — 2.7x — REGEXP + GROUP BY + HAVING

- CH best: 9.526s | OS best: 25.531s

```sql
SELECT REGEXP_REPLACE(Referer, '^https?://(?:www\.)?([^/]+)/.*$', '\1') AS k, AVG(length(Referer)) AS l, COUNT(*) AS c, MIN(Referer) FROM hits WHERE Referer <> '' GROUP BY REGEXP_REPLACE(Referer, '^https?://(?:www\.)?([^/]+)/.*$', '\1') HAVING COUNT(*) > 100000 ORDER BY l DESC LIMIT 25;
```

### Q30 (line 31) — 2.3x — high-cardinality GROUP BY

- CH best: 0.778s | OS best: 1.827s

```sql
SELECT SearchEngineID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits WHERE SearchPhrase <> '' GROUP BY SearchEngineID, ClientIP ORDER BY c DESC LIMIT 10;
```

### Q31 (line 32) — 2.0x — high-cardinality GROUP BY

- CH best: 1.095s | OS best: 2.230s

```sql
SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits WHERE SearchPhrase <> '' GROUP BY WatchID, ClientIP ORDER BY c DESC LIMIT 10;
```

### Q32 (line 33) — 3.2x — high-cardinality GROUP BY

- CH best: 4.493s | OS best: 14.268s

```sql
SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits GROUP BY WatchID, ClientIP ORDER BY c DESC LIMIT 10;
```

### Q35 (line 36) — 5.0x — high-cardinality GROUP BY

- CH best: 0.370s | OS best: 1.862s

```sql
SELECT ClientIP, ClientIP - 1, ClientIP - 2, ClientIP - 3, COUNT(*) AS c FROM hits GROUP BY ClientIP, ClientIP - 1, ClientIP - 2, ClientIP - 3 ORDER BY c DESC LIMIT 10;
```

### Q36 (line 37) — 4.0x — high-cardinality GROUP BY

- CH best: 0.121s | OS best: 0.487s

```sql
SELECT URL, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= DATE '2013-07-01' AND EventDate <= DATE '2013-07-31' AND DontCountHits = 0 AND IsRefresh = 0 AND URL <> '' GROUP BY URL ORDER BY PageViews DESC LIMIT 10;
```

### Q37 (line 38) — 2.7x — GROUP BY aggregation

- CH best: 0.102s | OS best: 0.280s

```sql
SELECT Title, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= DATE '2013-07-01' AND EventDate <= DATE '2013-07-31' AND DontCountHits = 0 AND IsRefresh = 0 AND Title <> '' GROUP BY Title ORDER BY PageViews DESC LIMIT 10;
```

### Q39 (line 40) — 16.9x — high-cardinality GROUP BY

- CH best: 0.143s | OS best: 2.422s

```sql
SELECT TraficSourceID, SearchEngineID, AdvEngineID, CASE WHEN (SearchEngineID = 0 AND AdvEngineID = 0) THEN Referer ELSE '' END AS Src, URL AS Dst, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= DATE '2013-07-01' AND EventDate <= DATE '2013-07-31' AND IsRefresh = 0 GROUP BY Tr ...
```
