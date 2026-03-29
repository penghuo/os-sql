# ClickBench Slow Queries (>2x CH-Parquet) — Categorized

## A) COUNT(DISTINCT) Queries
These all use `COUNT(DISTINCT ...)` which is expensive without HyperLogLog or specialized aggregation.

**Q04** (line 5): `SELECT COUNT(DISTINCT UserID) FROM hits;`

**Q05** (line 6): `SELECT COUNT(DISTINCT SearchPhrase) FROM hits;`

**Q08** (line 9): `SELECT RegionID, COUNT(DISTINCT UserID) AS u FROM hits GROUP BY RegionID ORDER BY u DESC LIMIT 10;`

**Q09** (line 10): `SELECT RegionID, SUM(AdvEngineID), COUNT(*) AS c, AVG(ResolutionWidth), COUNT(DISTINCT UserID) FROM hits GROUP BY RegionID ORDER BY c DESC LIMIT 10;`

**Q11** (line 12): `SELECT MobilePhone, MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM hits WHERE MobilePhoneModel <> '' GROUP BY MobilePhone, MobilePhoneModel ORDER BY u DESC LIMIT 10;`

**Q13** (line 14): `SELECT SearchPhrase, COUNT(DISTINCT UserID) AS u FROM hits WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY u DESC LIMIT 10;`

## B) High-Cardinality GROUP BY
Large result sets from GROUP BY on high-cardinality columns (UserID, SearchPhrase).

**Q15** (line 16): `SELECT UserID, COUNT(*) FROM hits GROUP BY UserID ORDER BY COUNT(*) DESC LIMIT 10;`

**Q16** (line 17): `SELECT UserID, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, SearchPhrase ORDER BY COUNT(*) DESC LIMIT 10;`

**Q18** (line 19): `SELECT UserID, extract(minute FROM EventTime) AS m, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, extract(minute FROM EventTime), SearchPhrase ORDER BY COUNT(*) DESC LIMIT 10;`

## C) Borderline (~2x)
Close to the 2x threshold; mixed patterns.

**Q14** (line 15): `SELECT SearchEngineID, SearchPhrase, COUNT(*) AS c FROM hits WHERE SearchPhrase <> '' GROUP BY SearchEngineID, SearchPhrase ORDER BY c DESC LIMIT 10;`

**Q27** (line 28): `SELECT CounterID, AVG(length(URL)) AS l, COUNT(*) AS c FROM hits WHERE URL <> '' GROUP BY CounterID HAVING COUNT(*) > 100000 ORDER BY l DESC LIMIT 25;`

**Q29** (line 30): `SELECT SUM(ResolutionWidth), SUM(ResolutionWidth + 1), ... SUM(ResolutionWidth + 89) FROM hits;`
*(89 SUM expressions over a single full-table scan)*

**Q30** (line 31): `SELECT SearchEngineID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits WHERE SearchPhrase <> '' GROUP BY SearchEngineID, ClientIP ORDER BY c DESC LIMIT 10;`

## D) VARCHAR GROUP BY
GROUP BY on high-cardinality VARCHAR columns (URL, ClientIP expressions).

**Q35** (line 36): `SELECT URL, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND DontCountHits = 0 AND IsRefresh = 0 AND URL <> '' GROUP BY URL ORDER BY PageViews DESC LIMIT 10;`

**Q36** (line 37): `SELECT Title, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND DontCountHits = 0 AND IsRefresh = 0 AND Title <> '' GROUP BY Title ORDER BY PageViews DESC LIMIT 10;`

**Q39** (line 40): `SELECT TraficSourceID, SearchEngineID, AdvEngineID, CASE WHEN (...) THEN Referer ELSE '' END AS Src, URL AS Dst, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND IsRefresh = 0 GROUP BY TraficSourceID, SearchEngineID, AdvEngineID, CASE WHEN (...) THEN Referer ELSE '' END, URL ORDER BY PageViews DESC OFFSET 1000 LIMIT 10;`

## E) Other

**Q02** (line 3): `SELECT SUM(AdvEngineID), COUNT(*), AVG(ResolutionWidth) FROM hits;`
*(Simple full-scan aggregation — likely expression evaluation overhead)*

**Q28** (line 29): `SELECT REGEXP_REPLACE(Referer, '^https?://(?:www\.)?([^/]+)/.*$', '\1') AS k, AVG(length(Referer)) AS l, COUNT(*) AS c, MIN(Referer) FROM hits WHERE Referer <> '' GROUP BY REGEXP_REPLACE(Referer, '^https?://(?:www\.)?([^/]+)/.*$', '\1') HAVING COUNT(*) > 100000 ORDER BY l DESC LIMIT 25;`
*(REGEXP_REPLACE in both SELECT and GROUP BY — expensive string processing)*

---
## Summary by Category

| Category | Queries | Count | Key Bottleneck |
|----------|---------|-------|----------------|
| A) COUNT(DISTINCT) | Q04,Q05,Q08,Q09,Q11,Q13 | 6 | Exact distinct counting without HLL |
| B) High-card GROUP BY | Q15,Q16,Q18 | 3 | Hash table size for UserID×SearchPhrase |
| C) Borderline | Q14,Q27,Q29,Q30 | 4 | Mixed: string GROUP BY, wide agg, multi-col |
| D) VARCHAR GROUP BY | Q35,Q36,Q39 | 3 | Hashing/comparing long VARCHAR keys |
| E) Other | Q02,Q28 | 2 | Expression eval, REGEXP in GROUP BY |
| **Total** | | **18** | |

Note: The task listed Q32 in category E but the provided query numbers don't include Q32. Q02 and Q28 are the two "Other" queries.
