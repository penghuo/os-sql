# ClickBench Benchmark Results

## Environment
- **Dataset**: hits.parquet (14GB, 99,997,497 rows, 105 columns)
- **Engine**: Trino 440 embedded in OpenSearch via shadow jar + DistributedQueryRunner
- **Connector**: Hive (external Parquet table)
- **Hardware**: Single node, 16GB JVM heap
- **Endpoint**: `/_plugins/_trino_sql/v1/statement`
- **Date**: 2026-04-04

## Results: 42/43 queries pass (98%)

| Query | Time (s) | Status |
|-------|----------|--------|
| Q0  |  4.6 | PASS |
| Q1  |  4.8 | PASS |
| Q2  |  5.3 | PASS |
| Q3  |  4.8 | PASS |
| Q4  |  6.6 | PASS |
| Q5  |  8.8 | PASS |
| Q6  |  4.2 | PASS |
| Q7  |  4.0 | PASS |
| Q8  |  7.7 | PASS |
| Q9  |  9.7 | PASS |
| Q10 |  4.5 | PASS |
| Q11 |  4.7 | PASS |
| Q12 |  7.3 | PASS |
| Q13 | 10.6 | PASS |
| Q14 |  7.5 | PASS |
| Q15 |  6.1 | PASS |
| Q16 | 13.2 | PASS |
| Q17 | 11.6 | PASS |
| Q18 | 22.3 | PASS |
| Q19 |  4.2 | PASS |
| Q20 |  9.5 | PASS |
| Q21 |  8.7 | PASS |
| Q22 |  9.9 | PASS |
| Q23 | 17.9 | PASS |
| Q24 |  5.2 | PASS |
| Q25 |  5.0 | PASS |
| Q26 |  5.1 | PASS |
| Q27 |  9.1 | PASS |
| Q28 | 31.3 | PASS |
| Q29 | 23.0 | PASS |
| Q30 |  7.0 | PASS |
| Q31 |  7.5 | PASS |
| Q32 |    - | FAIL (OOM: GROUP BY WatchID,ClientIP on 100M rows, needs >4.8GB) |
| Q33 | 20.8 | PASS |
| Q34 | 21.4 | PASS |
| Q35 |  8.6 | PASS |
| Q36 |  4.0 | PASS |
| Q37 |  3.9 | PASS |
| Q38 |  4.1 | PASS |
| Q39 |  4.3 | PASS |
| Q40 |  3.9 | PASS |
| Q41 |  3.9 | PASS |
| Q42 |  4.1 | PASS |

## Summary
- **42/43 passed (98%)**
- **Total query time**: 370s for 42 queries
- **Average**: 8.8s per query
- **Fastest**: 3.9s (Q37, Q40, Q41)
- **Slowest**: 31.3s (Q28 — REGEXP_REPLACE)
- **Only failure**: Q32 — memory limit for unfiltered GROUP BY on 2 high-cardinality columns
