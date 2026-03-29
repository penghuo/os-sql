# ClickBench Failing Queries Analysis (>2x vs ClickHouse Parquet)

**Sources:**
- Queries: `benchmarks/clickbench/queries/queries_trino.sql`
- CH baseline: `benchmarks/clickbench/results/performance/clickhouse_parquet_official/c6a.4xlarge.json` (c6a.4xlarge)
- DQE current: `benchmarks/clickbench/results/performance/opensearch_phaseD/m5.8xlarge.json` (m5.8xlarge, 2026-03-21)

**Note:** CH min = min of 3 runs; DQE min = min of 5 runs. Different instance types (c6a.4xlarge vs m5.8xlarge).

## Failing Queries Table

| Q# | CH Min (s) | DQE Min (s) | Ratio | Category | SQL Pattern |
|----|-----------|-------------|-------|----------|-------------|
| Q02 | 0.105 | 0.232 | 2.2x | borderline | `SELECT SUM(AdvEngineID), COUNT(*), AVG(ResolutionWidth) FROM hits` |
| Q04 | 0.434 | 2.334 | 5.4x | COUNT DISTINCT global | `SELECT COUNT(DISTINCT UserID) FROM hits` |
| Q05 | 0.690 | 3.979 | 5.8x | COUNT DISTINCT global | `SELECT COUNT(DISTINCT SearchPhrase) FROM hits` |
| Q08 | 0.540 | 1.858 | 3.4x | GROUP BY + COUNT DISTINCT | `SELECT RegionID, COUNT(DISTINCT UserID) AS u FROM hits GROUP BY RegionID ORDER BY u DESC LIMIT 10` |
| Q09 | 0.614 | 4.902 | 8.0x | GROUP BY + mixed aggs + COUNT DISTINCT | `SELECT RegionID, SUM(AdvEngineID), COUNT(*), AVG(ResolutionWidth), COUNT(DISTINCT UserID) FROM hits GROUP BY RegionID ORDER BY c DESC LIMIT 10` |
| Q11 | 0.268 | 2.461 | 9.2x | GROUP BY multi-col + COUNT DISTINCT | `SELECT MobilePhone, MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM hits WHERE MobilePhoneModel <> '' GROUP BY MobilePhone, MobilePhoneModel ORDER BY u DESC LIMIT 10` |
| Q13 | 0.956 | 5.965 | 6.2x | GROUP BY + COUNT DISTINCT | `SELECT SearchPhrase, COUNT(DISTINCT UserID) AS u FROM hits WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY u DESC LIMIT 10` |
| Q15 | 0.520 | 2.126 | 4.1x | high-cardinality GROUP BY | `SELECT UserID, COUNT(*) FROM hits GROUP BY UserID ORDER BY COUNT(*) DESC LIMIT 10` |
| Q16 | 1.794 | 7.277 | 4.1x | high-cardinality GROUP BY (2 cols) | `SELECT UserID, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, SearchPhrase ORDER BY COUNT(*) DESC LIMIT 10` |
| Q28 | 9.526 | 25.531 | 2.7x | REGEXP_REPLACE + GROUP BY + HAVING | `SELECT REGEXP_REPLACE(Referer, ...) AS k, AVG(length(Referer)) AS l, COUNT(*) AS c, MIN(Referer) FROM hits WHERE Referer <> '' GROUP BY REGEXP_REPLACE(...) HAVING COUNT(*) > 100000 ORDER BY l DESC LIMIT 25` |
| Q31 | 1.095 | 2.230 | 2.0x | high-cardinality GROUP BY (WatchID) | `SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits WHERE SearchPhrase <> '' GROUP BY WatchID, ClientIP ORDER BY c DESC LIMIT 10` |

## Pattern Categories

### 1. COUNT(DISTINCT) — 7 queries (Q04, Q05, Q08, Q09, Q11, Q13, Q15)
**Worst offenders.** Ratios from 3.4x to 9.2x.

- **Global COUNT(DISTINCT)** (Q04, Q05): Full-table distinct count with no GROUP BY. CH: 0.43-0.69s, DQE: 2.3-4.0s.
- **GROUP BY + COUNT(DISTINCT)** (Q08, Q09, Q11, Q13): Distinct count within groups. Q11 is worst at 9.2x — multi-column GROUP BY with COUNT(DISTINCT) on a third column.
- Q09 combines COUNT(DISTINCT) with SUM/COUNT/AVG in same query — 8.0x ratio.

**Root cause hypothesis:** DQE likely uses a naive hash-set-per-group approach for COUNT(DISTINCT). ClickHouse uses optimized HyperLogLog or sorted-based distinct counting with columnar memory layout advantages.

### 2. High-Cardinality GROUP BY — 3 queries (Q15, Q16, Q31)
**Ratios: 2.0x–4.1x.**

- GROUP BY on UserID (high cardinality ~17M distinct values) or WatchID (even higher).
- Q16 groups by (UserID, SearchPhrase) — compound high-cardinality key.
- These don't use COUNT(DISTINCT) but still suffer from hash table overhead with millions of groups.

**Root cause hypothesis:** Hash aggregation with high-cardinality keys causes memory pressure and poor cache locality. ClickHouse benefits from columnar sort-based aggregation.

### 3. REGEXP_REPLACE + GROUP BY + HAVING — 1 query (Q28)
**Ratio: 2.7x.**

- Applies REGEXP_REPLACE to every row of `Referer` column, then groups by the result.
- HAVING clause filters groups with COUNT(*) > 100000.
- The regex is applied twice (SELECT + GROUP BY) unless CSE is applied.

**Root cause hypothesis:** Regex evaluation per-row is expensive. ClickHouse may have optimized regex engine or better CSE for GROUP BY expressions.

### 4. Borderline: SUM+COUNT+AVG full scan — 1 query (Q02)
**Ratio: 2.2x.**

- Simple full-table scan with 3 aggregations, no GROUP BY.
- Suggests basic scan/aggregation throughput gap.

**Root cause hypothesis:** Vectorized execution or SIMD aggregation in ClickHouse vs row-at-a-time in DQE.

## Summary Statistics

| Category | Queries | Avg Ratio | Max Ratio |
|----------|---------|-----------|-----------|
| COUNT(DISTINCT) global | Q04, Q05 | 5.6x | 5.8x |
| GROUP BY + COUNT(DISTINCT) | Q08, Q09, Q11, Q13 | 6.7x | 9.2x |
| High-cardinality GROUP BY | Q15, Q16, Q31 | 3.4x | 4.1x |
| REGEXP_REPLACE | Q28 | 2.7x | 2.7x |
| Full-scan agg (borderline) | Q02 | 2.2x | 2.2x |

## Recommendations

1. **Prioritize COUNT(DISTINCT) optimization** — affects 7/11 failing queries, highest ratios (up to 9.2x). Consider HyperLogLog approximation or sorted-based distinct counting.
2. **Improve high-cardinality hash aggregation** — affects 3 queries at 2-4x. Look at memory layout, pre-sizing hash tables, or sort-based aggregation fallback.
3. **Add Common Subexpression Elimination (CSE)** for GROUP BY expressions — Q28 evaluates REGEXP_REPLACE twice.
4. **Investigate vectorized aggregation** — Q02's 2.2x gap on a simple full-scan suggests room for SIMD/batch processing improvements.
