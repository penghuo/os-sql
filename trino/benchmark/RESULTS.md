# ClickBench Benchmark Results

## Environment
- **Dataset**: hits.parquet (14GB, 99,997,497 rows, 105 columns)
- **Engine**: Trino 440 embedded in OpenSearch via shadow jar + DistributedQueryRunner
- **Connector**: Hive (external Parquet table)
- **Hardware**: Single node, 16GB JVM heap, ~10GB Trino query memory
- **Endpoint**: `/_plugins/_trino_sql/v1/statement`
- **Date**: 2026-04-04

## Results: 43/43 queries pass (100%)

| Query | Time (s) | Description |
|-------|----------|-------------|
| Q0  |  4.1 | `COUNT(*)` → 99,997,497 |
| Q1  |  4.2 | `COUNT(*)` with filter |
| Q2  |  4.6 | `SUM`, `COUNT`, `AVG` |
| Q3  |  4.1 | `AVG(UserID)` |
| Q4  |  5.6 | `COUNT(DISTINCT UserID)` |
| Q5  |  8.0 | `COUNT(DISTINCT SearchPhrase)` |
| Q6  |  4.1 | `MIN/MAX(EventDate)` |
| Q7  |  4.1 | `GROUP BY AdvEngineID` |
| Q8  |  7.9 | `COUNT(DISTINCT)` + `GROUP BY` |
| Q9  |  9.9 | Multi-agg `GROUP BY RegionID` |
| Q10 |  4.7 | `GROUP BY MobilePhoneModel` |
| Q11 |  4.8 | Multi-column `GROUP BY` |
| Q12 |  7.5 | `GROUP BY SearchPhrase` |
| Q13 | 10.6 | `COUNT(DISTINCT)` + `GROUP BY SearchPhrase` |
| Q14 |  7.5 | `GROUP BY SearchEngineID, SearchPhrase` |
| Q15 |  6.1 | `GROUP BY UserID` |
| Q16 | 13.5 | `GROUP BY UserID, SearchPhrase` |
| Q17 | 11.5 | `GROUP BY UserID, SearchPhrase` (no ORDER BY) |
| Q18 | 22.9 | `GROUP BY UserID, minute, SearchPhrase` |
| Q19 |  4.1 | Point lookup `UserID = ...` |
| Q20 |  9.5 | `URL LIKE '%google%'` |
| Q21 |  8.8 | `URL + SearchPhrase` filter |
| Q22 |  9.8 | `Title + URL + SearchPhrase` filter |
| Q23 | 17.6 | `SELECT * WHERE URL LIKE ORDER BY` |
| Q24 |  5.2 | `ORDER BY EventTime LIMIT 10` |
| Q25 |  4.9 | `ORDER BY SearchPhrase LIMIT 10` |
| Q26 |  5.3 | `ORDER BY EventTime, SearchPhrase` |
| Q27 |  9.2 | `HAVING COUNT(*) > 100000` |
| Q28 | 34.9 | `REGEXP_REPLACE` + `GROUP BY` |
| Q29 | 26.2 | 90-column `SUM` |
| Q30 |  7.0 | `GROUP BY SearchEngineID, ClientIP` |
| Q31 |  7.7 | `GROUP BY WatchID, ClientIP` (filtered) |
| Q32 | 29.6 | `GROUP BY WatchID, ClientIP` (unfiltered) |
| Q33 | 21.6 | `GROUP BY URL` |
| Q34 | 24.1 | `GROUP BY URL` with literal |
| Q35 |  9.2 | `GROUP BY ClientIP` arithmetic |
| Q36 |  4.2 | `CounterID = 62` + date filter |
| Q37 |  4.0 | Title aggregation + filters |
| Q38 |  4.1 | URL + link/download filters |
| Q39 |  4.4 | `CASE WHEN` + multi-column `GROUP BY` |
| Q40 |  3.9 | `URLHash` + date + traffic filter |
| Q41 |  4.0 | Window dimensions + hash filter |
| Q42 |  4.0 | Timestamp truncation + `GROUP BY` |

## Summary
- **43/43 passed (100%)**
- **Total query time**: 409s
- **Average**: 9.5s per query
- **Fastest**: 3.9s (Q40)
- **Slowest**: 34.9s (Q28 — REGEXP_REPLACE + GROUP BY HAVING)

## How to Run
```bash
# Download data (14GB, one-time)
wget -O trino/benchmark/data/hits.parquet \
  https://datasets.clickhouse.com/hits_compatible/hits.parquet

# Build plugin
./gradlew :opensearch-sql-plugin:bundlePlugin

# Run benchmark
./trino/benchmark/run_clickbench.sh
```
