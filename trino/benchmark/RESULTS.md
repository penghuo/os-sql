# ClickBench Benchmark Results

## Environment
- **Dataset**: hits.parquet (14GB, 99,997,497 rows, 105 columns)
- **Engine**: Trino 440 embedded in OpenSearch via shadow jar + DistributedQueryRunner
- **Connector**: Hive (external Parquet table, DATE/TIMESTAMP mapped to INT/BIGINT)
- **Hardware**: Single node, 8GB JVM heap
- **Endpoint**: `/_plugins/_trino_sql/v1/statement`
- **Date**: 2026-04-04

## Results: 38/43 queries pass (88%)

| Query | Time (s) | Status | Notes |
|-------|----------|--------|-------|
| Q0  |  4.6 | PASS | `COUNT(*)` → 99,997,497 |
| Q1  |  4.7 | PASS | `COUNT(*)` with filter |
| Q2  |  5.2 | PASS | `SUM`, `COUNT`, `AVG` |
| Q3  |  4.8 | PASS | `AVG(UserID)` |
| Q4  |  6.4 | PASS | `COUNT(DISTINCT UserID)` → 17,630,976 |
| Q5  |  8.6 | PASS | `COUNT(DISTINCT SearchPhrase)` → 6,019,103 |
| Q6  |  4.8 | PASS | `MIN/MAX(EventDate)` |
| Q7  |  4.7 | PASS | `GROUP BY AdvEngineID` |
| Q8  |  8.7 | PASS | `COUNT(DISTINCT)` + `GROUP BY` |
| Q9  | 11.5 | PASS | Multi-agg `GROUP BY RegionID` |
| Q10 |  5.3 | PASS | `GROUP BY MobilePhoneModel` |
| Q11 |  5.5 | PASS | Multi-column `GROUP BY` |
| Q12 |  8.5 | PASS | `GROUP BY SearchPhrase` |
| Q13 | 12.3 | PASS | `COUNT(DISTINCT)` + `GROUP BY SearchPhrase` |
| Q14 |  8.7 | PASS | `GROUP BY SearchEngineID, SearchPhrase` |
| Q15 |  7.0 | PASS | `GROUP BY UserID` |
| Q16 | 15.2 | PASS | `GROUP BY UserID, SearchPhrase` |
| Q17 | 13.2 | PASS | `GROUP BY UserID, SearchPhrase` (no ORDER BY) |
| Q18 |    - | FAIL | Memory limit exceeded (3-column GROUP BY with extract) |
| Q19 |  4.8 | PASS | Point lookup `UserID = 435090932899640449` |
| Q20 | 10.9 | PASS | `URL LIKE '%google%'` |
| Q21 | 10.0 | PASS | `URL + SearchPhrase` filter |
| Q22 | 11.2 | PASS | `Title + URL + SearchPhrase` filter |
| Q23 |    - | FAIL | JSON control character in URL data (response parsing) |
| Q24 |  6.1 | PASS | `ORDER BY EventTime LIMIT 10` |
| Q25 |  5.9 | PASS | `ORDER BY SearchPhrase LIMIT 10` |
| Q26 |  6.5 | PASS | `ORDER BY EventTime, SearchPhrase` |
| Q27 | 11.6 | PASS | `HAVING COUNT(*) > 100000` |
| Q28 | 36.4 | PASS | `REGEXP_REPLACE` + `GROUP BY` |
| Q29 | 28.5 | PASS | 90-column `SUM(ResolutionWidth + N)` |
| Q30 |  8.0 | PASS | `GROUP BY SearchEngineID, ClientIP` |
| Q31 |  9.0 | PASS | `GROUP BY WatchID, ClientIP` (filtered) |
| Q32 |    - | FAIL | Memory limit exceeded (`GROUP BY WatchID, ClientIP` unfiltered) |
| Q33 |    - | FAIL | Memory limit exceeded (`GROUP BY URL`) |
| Q34 |    - | FAIL | Memory limit exceeded (`GROUP BY URL`) |
| Q35 | 10.2 | PASS | `GROUP BY ClientIP` arithmetic |
| Q36 |  4.8 | PASS | `CounterID = 62` + date filter |
| Q37 |  4.7 | PASS | Title aggregation with filters |
| Q38 |  4.7 | PASS | URL with link/download filters |
| Q39 |  5.0 | PASS | `CASE WHEN` + multi-column `GROUP BY` |
| Q40 |  4.8 | PASS | `URLHash` + date filter |
| Q41 |  4.6 | PASS | Window dimensions + hash filter |
| Q42 |  4.6 | PASS | Timestamp truncation + `GROUP BY` |

## Summary
- **38/43 passed (88%)**
- **5 failures**: 4 memory limits (need 16GB+ heap or spill), 1 JSON encoding (control chars)
- **Total query time**: 332s for 38 queries
- **Average**: 8.7s per query on 100M rows, single node

## Failure Details
| Query | Error | Fix |
|-------|-------|-----|
| Q18 | Memory limit 2.4GB | Increase heap to 16GB or enable spill-to-disk |
| Q23 | JSON control character | Fix JSON escaping in TrinoEngine response serializer |
| Q32 | Memory limit 2.4GB | Increase heap or enable spill-to-disk |
| Q33 | Memory limit 2.4GB | Increase heap or enable spill-to-disk |
| Q34 | Memory limit 2.4GB | Increase heap or enable spill-to-disk |
