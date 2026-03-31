# Above-2x Queries: SQL Text & Categorization

## Category A: Scalar COUNT(DISTINCT) — no GROUP BY

| Query | SQL | CD? | WHERE? |
|-------|-----|-----|--------|
| **Q04** | `SELECT COUNT(DISTINCT UserID) FROM hits;` | YES (BIGINT) | NO |

**Calcite plan**: Full table scan → AggregateCall[COUNT DISTINCT] → single row output.
No GROUP BY keys. The CD on BIGINT is the sole bottleneck.

---

## Category B: GROUP BY + COUNT(DISTINCT)

| Query | SQL | GROUP BY keys | Key types | WHERE? |
|-------|-----|---------------|-----------|--------|
| **Q08** | `SELECT RegionID, COUNT(DISTINCT UserID) AS u FROM hits GROUP BY RegionID ORDER BY u DESC LIMIT 10` | RegionID | INTEGER | NO |
| **Q09** | `SELECT RegionID, SUM(AdvEngineID), COUNT(*) AS c, AVG(ResolutionWidth), COUNT(DISTINCT UserID) FROM hits GROUP BY RegionID ORDER BY c DESC LIMIT 10` | RegionID | INTEGER | NO |
| **Q11** | `SELECT MobilePhone, MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM hits WHERE MobilePhoneModel <> '' GROUP BY MobilePhone, MobilePhoneModel ORDER BY u DESC LIMIT 10` | MobilePhone, MobilePhoneModel | SMALLINT, VARCHAR | YES |
| **Q13** | `SELECT SearchPhrase, COUNT(DISTINCT UserID) AS u FROM hits WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY u DESC LIMIT 10` | SearchPhrase | TEXT | YES |

**Calcite plan**: Scan → [Filter] → Aggregate(group=[keys], agg=[COUNT DISTINCT UserID]) → Sort → Limit.
CD on BIGINT UserID within each group. Calcite typically rewrites CD as a two-phase aggregate (expand distinct → group-by-group+distinct-col → aggregate).

---

## Category C: GROUP BY only (no COUNT DISTINCT) — high-cardinality keys

| Query | SQL | GROUP BY keys | Key types | WHERE? |
|-------|-----|---------------|-----------|--------|
| **Q07** | `SELECT AdvEngineID, COUNT(*) FROM hits WHERE AdvEngineID <> 0 GROUP BY AdvEngineID ORDER BY COUNT(*) DESC` | AdvEngineID | SMALLINT | YES |
| **Q14** | `SELECT SearchEngineID, SearchPhrase, COUNT(*) AS c FROM hits WHERE SearchPhrase <> '' GROUP BY SearchEngineID, SearchPhrase ORDER BY c DESC LIMIT 10` | SearchEngineID, SearchPhrase | SMALLINT, TEXT | YES |
| **Q15** | `SELECT UserID, COUNT(*) FROM hits GROUP BY UserID ORDER BY COUNT(*) DESC LIMIT 10` | UserID | BIGINT | NO |
| **Q16** | `SELECT UserID, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, SearchPhrase ORDER BY COUNT(*) DESC LIMIT 10` | UserID, SearchPhrase | BIGINT, TEXT | NO |
| **Q18** | `SELECT UserID, extract(minute FROM EventTime) AS m, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, extract(minute FROM EventTime), SearchPhrase ORDER BY COUNT(*) DESC LIMIT 10` | UserID, EXTRACT(minute), SearchPhrase | BIGINT, INT, TEXT | NO |
| **Q21** | `SELECT SearchPhrase, MIN(URL), COUNT(*) AS c FROM hits WHERE URL LIKE '%google%' AND SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10` | SearchPhrase | TEXT | YES |
| **Q32** | `SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits GROUP BY WatchID, ClientIP ORDER BY c DESC LIMIT 10` | WatchID, ClientIP | BIGINT, INTEGER | NO |
| **Q35** | `SELECT ClientIP, ClientIP - 1, ClientIP - 2, ClientIP - 3, COUNT(*) AS c FROM hits GROUP BY ClientIP, ClientIP - 1, ClientIP - 2, ClientIP - 3 ORDER BY c DESC LIMIT 10` | ClientIP, ClientIP-1, ClientIP-2, ClientIP-3 | INTEGER (×4 exprs) | NO |
| **Q39** | `SELECT TraficSourceID, SearchEngineID, AdvEngineID, CASE WHEN ... THEN Referer ELSE '' END AS Src, URL AS Dst, COUNT(*) AS PageViews FROM hits WHERE CounterID=62 AND EventDate>=... AND EventDate<=... AND IsRefresh=0 GROUP BY TraficSourceID, SearchEngineID, AdvEngineID, CASE WHEN ... END, URL ORDER BY PageViews DESC OFFSET 1000 LIMIT 10` | TraficSourceID, SearchEngineID, AdvEngineID, CASE expr, URL | SMALLINT×3, TEXT×2 | YES (highly selective) |

**Calcite plan**: Scan → [Filter] → Aggregate(group=[keys], agg=[COUNT/SUM/AVG/MIN]) → Sort → Limit.
Key bottleneck: high-cardinality GROUP BY keys (UserID=BIGINT, SearchPhrase=TEXT, WatchID=BIGINT) cause large hash tables.

---

## Category D: Filter-only (no aggregation, no GROUP BY)

| Query | SQL | WHERE? |
|-------|-----|--------|
| **Q19** | `SELECT UserID FROM hits WHERE UserID = 435090932899640449` | YES (point lookup BIGINT) |
| **Q20** | `SELECT COUNT(*) FROM hits WHERE URL LIKE '%google%'` | YES (LIKE on TEXT) |

**Calcite plan**: Scan → Filter → [Aggregate COUNT(*)] or Project.
Q19: point filter on BIGINT, should be fast — slowness likely in scan/push-down.
Q20: full-scan with LIKE '%google%' (no prefix, can't use index), then scalar COUNT(*).

---

## Summary by Pattern

| Pattern | Queries | Count | Primary Bottleneck |
|---------|---------|-------|--------------------|
| Scalar CD (no GROUP BY) | Q04 | 1 | COUNT(DISTINCT) on full table |
| GROUP BY + CD | Q08, Q09, Q11, Q13 | 4 | Two-phase CD expansion + hash agg |
| GROUP BY only (high-card keys) | Q07, Q14, Q15, Q16, Q18, Q21, Q32, Q35, Q39 | 9 | Large hash tables for high-cardinality keys |
| Filter-only / scalar agg | Q19, Q20 | 2 | Scan + filter push-down |

### Key Column Types in GROUP BY

- **BIGINT**: UserID (Q08,Q09,Q15,Q16,Q18,Q32), WatchID (Q32) — 8-byte numeric, high cardinality
- **TEXT**: SearchPhrase (Q13,Q14,Q16,Q18,Q21), URL (Q39) — variable-length, very high cardinality
- **INTEGER**: RegionID (Q08,Q09), ClientIP (Q32,Q35) — 4-byte numeric, moderate cardinality
- **SMALLINT**: AdvEngineID (Q07), SearchEngineID (Q14), MobilePhone (Q11), TraficSourceID (Q39) — 2-byte, low cardinality
- **VARCHAR**: MobilePhoneModel (Q11) — moderate cardinality

### COUNT(DISTINCT) Target
All CD queries target **UserID (BIGINT)** — a single high-cardinality column.
