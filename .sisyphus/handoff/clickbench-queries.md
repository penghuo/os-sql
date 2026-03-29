# ClickBench Query Analysis

## COUNT(DISTINCT) Queries

**Q04** (line 5): `SELECT COUNT(DISTINCT UserID) FROM hits;`
- Pattern: COUNT(DISTINCT) full-table scan, no GROUP BY
- Aggregates: COUNT(DISTINCT UserID)

**Q05** (line 6): `SELECT COUNT(DISTINCT SearchPhrase) FROM hits;`
- Pattern: COUNT(DISTINCT) full-table scan, no GROUP BY
- Aggregates: COUNT(DISTINCT SearchPhrase)

**Q08** (line 9): `SELECT RegionID, COUNT(DISTINCT UserID) AS u FROM hits GROUP BY RegionID ORDER BY u DESC LIMIT 10;`
- Pattern: COUNT(DISTINCT) + GROUP BY + ORDER BY + LIMIT
- GROUP BY: RegionID
- Aggregates: COUNT(DISTINCT UserID)

**Q09** (line 10): `SELECT RegionID, SUM(AdvEngineID), COUNT(*) AS c, AVG(ResolutionWidth), COUNT(DISTINCT UserID) FROM hits GROUP BY RegionID ORDER BY c DESC LIMIT 10;`
- Pattern: Mixed aggregates including COUNT(DISTINCT) + GROUP BY + ORDER BY + LIMIT
- GROUP BY: RegionID
- Aggregates: SUM, COUNT(*), AVG, COUNT(DISTINCT UserID)

**Q11** (line 12): `SELECT MobilePhone, MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM hits WHERE MobilePhoneModel <> '' GROUP BY MobilePhone, MobilePhoneModel ORDER BY u DESC LIMIT 10;`
- Pattern: COUNT(DISTINCT) + multi-key GROUP BY + WHERE filter + ORDER BY + LIMIT
- GROUP BY: MobilePhone, MobilePhoneModel
- Aggregates: COUNT(DISTINCT UserID)

**Q13** (line 14): `SELECT SearchPhrase, COUNT(DISTINCT UserID) AS u FROM hits WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY u DESC LIMIT 10;`
- Pattern: COUNT(DISTINCT) + high-cardinality GROUP BY + WHERE filter + ORDER BY + LIMIT
- GROUP BY: SearchPhrase (high cardinality)
- Aggregates: COUNT(DISTINCT UserID)

## High-Cardinality GROUP BY Queries

**Q15** (line 16): `SELECT UserID, COUNT(*) FROM hits GROUP BY UserID ORDER BY COUNT(*) DESC LIMIT 10;`
- Pattern: High-cardinality GROUP BY on UserID + ORDER BY + LIMIT
- GROUP BY: UserID (very high cardinality)
- Aggregates: COUNT(*)

**Q16** (line 17): `SELECT UserID, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, SearchPhrase ORDER BY COUNT(*) DESC LIMIT 10;`
- Pattern: Multi-key high-cardinality GROUP BY + ORDER BY + LIMIT
- GROUP BY: UserID, SearchPhrase (both high cardinality)
- Aggregates: COUNT(*)

**Q18** (line 19): `SELECT UserID, extract(minute FROM EventTime) AS m, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, extract(minute FROM EventTime), SearchPhrase ORDER BY COUNT(*) DESC LIMIT 10;`
- Pattern: High-cardinality GROUP BY with expression key (extract) + ORDER BY + LIMIT
- GROUP BY: UserID, extract(minute FROM EventTime), SearchPhrase
- Aggregates: COUNT(*)

**Q32** (line 33): `SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits GROUP BY WatchID, ClientIP ORDER BY c DESC LIMIT 10;`
- Pattern: Very high-cardinality GROUP BY (WatchID is near-unique) + multiple aggregates + ORDER BY + LIMIT
- GROUP BY: WatchID, ClientIP
- Aggregates: COUNT(*), SUM(IsRefresh), AVG(ResolutionWidth)

## REGEXP_REPLACE Query

**Q28** (line 29): `SELECT REGEXP_REPLACE(Referer, '^https?://(?:www\.)?([^/]+)/.*$', '\1') AS k, AVG(length(Referer)) AS l, COUNT(*) AS c, MIN(Referer) FROM hits WHERE Referer <> '' GROUP BY REGEXP_REPLACE(Referer, '^https?://(?:www\.)?([^/]+)/.*$', '\1') HAVING COUNT(*) > 100000 ORDER BY l DESC LIMIT 25;`
- Pattern: REGEXP_REPLACE in both SELECT and GROUP BY + HAVING + ORDER BY + LIMIT
- GROUP BY: REGEXP_REPLACE expression (domain extraction)
- Aggregates: AVG(length(Referer)), COUNT(*), MIN(Referer)

## Borderline Query

**Q31** (line 32): `SELECT SearchEngineID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits WHERE SearchPhrase <> '' GROUP BY SearchEngineID, ClientIP ORDER BY c DESC LIMIT 10;`
- Pattern: Medium-cardinality GROUP BY + WHERE filter + multiple aggregates + ORDER BY + LIMIT
- GROUP BY: SearchEngineID, ClientIP
- Aggregates: COUNT(*), SUM(IsRefresh), AVG(ResolutionWidth)
- Borderline because ClientIP has moderate cardinality

## Pattern Summary

| Category | Queries | Key Challenge |
|----------|---------|---------------|
| COUNT(DISTINCT) no GROUP BY | Q04, Q05 | Full-table distinct count |
| COUNT(DISTINCT) + GROUP BY | Q08, Q09, Q11, Q13 | Distinct per group |
| High-cardinality GROUP BY | Q15, Q16, Q18, Q32 | Large number of groups |
| REGEXP_REPLACE | Q28 | Regex function in GROUP BY |
| Borderline | Q31 | Moderate cardinality GROUP BY |
