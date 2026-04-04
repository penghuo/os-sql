# ClickBench Benchmark: Iceberg Table on Trino-OpenSearch

**Date**: 2026-04-04 22:20 UTC
**Dataset**: hits (99,997,497 rows, 105 columns) — Iceberg table with Parquet format

## Environment

| Setting | Value |
|---|---|
| **Engine** | Trino 440 embedded in OpenSearch 3.6 |
| **Machine** | Single node, dev instance |
| **Heap** | 32GB JVM (`-Xms32g -Xmx32g`) |
| **Query memory** | ~22.9GB (70% of heap) |
| **Table format** | Apache Iceberg v2, Parquet + Zstd compression |
| **Catalog type** | TESTING_FILE_METASTORE |
| **Data source** | CTAS from Hive external Parquet with DATE/TIMESTAMP conversion |
| **Endpoint** | `/_plugins/_trino_sql/v1/statement` |
| **Runs** | Single run (cold) |

## Results: 43/43 queries pass (100%)

| Query | Iceberg (s) | Hive Parquet (s) | Spark (s) | Iceberg vs Spark |
|-------|-------------|------------------|-----------|-----------------|
| Q0  |  0.1 |  4.1 |  0.78 | 0.1x |
| Q1  |  0.5 |  4.2 |  0.81 | 0.6x |
| Q2  |  1.0 |  4.6 |  0.93 | 1.1x |
| Q3  |  0.4 |  4.1 |  0.91 | 0.4x |
| Q4  |  2.0 |  5.6 |  3.45 | 0.6x |
| Q5  |  4.3 |  8.0 |  3.12 | 1.4x |
| Q6  |  0.2 |  4.1 |  0.81 | 0.2x |
| Q7  |  0.3 |  4.1 |  1.01 | 0.3x |
| Q8  |  4.3 |  7.9 |  3.78 | 1.1x |
| Q9  |  5.7 |  9.9 |  4.58 | 1.2x |
| Q10 |  1.0 |  4.7 |  1.92 | 0.5x |
| Q11 |  1.2 |  4.8 |  1.90 | 0.6x |
| Q12 |  4.0 |  7.5 |  3.12 | 1.3x |
| Q13 |  5.3 | 10.6 |  5.45 | 1.0x |
| Q14 |  4.1 |  7.5 |  3.28 | 1.3x |
| Q15 |  2.2 |  6.1 |  3.51 | 0.6x |
| Q16 |  9.7 | 13.5 |  5.80 | 1.7x |
| Q17 |  7.7 | 11.5 |  4.78 | 1.6x |
| Q18 | 20.1 | 22.9 | 10.44 | 1.9x |
| Q19 |  0.5 |  4.1 |  1.11 | 0.5x |
| Q20 |  7.5 |  9.5 |  3.53 | 2.1x |
| Q21 |  6.0 |  8.8 |  4.34 | 1.4x |
| Q22 |  9.1 |  9.8 |  7.12 | 1.3x |
| Q23 | 27.3 | 17.6 | 39.38 | 0.7x |
| Q24 |  2.1 |  5.2 |  1.69 | 1.2x |
| Q25 |  1.2 |  4.9 |  1.48 | 0.8x |
| Q26 |  2.1 |  5.3 |  1.69 | 1.2x |
| Q27 |  6.7 |  9.2 |  5.27 | 1.3x |
| Q28 | 24.6 | 34.9 | 18.48 | 1.3x |
| Q29 | 18.2 | 26.2 |  5.23 | 3.5x |
| Q30 |  3.7 |  7.0 |  3.19 | 1.2x |
| Q31 |  4.3 |  7.7 |  3.86 | 1.1x |
| Q32 | 21.5 | 29.6 | 12.63 | 1.7x |
| Q33 | 19.9 | 21.6 | 10.04 | 2.0x |
| Q34 | 20.7 | 24.1 | 10.00 | 2.1x |
| Q35 |  5.1 |  9.2 |  3.62 | 1.4x |
| Q36 |  1.0 |  4.2 |  1.18 | 0.8x |
| Q37 |  0.7 |  4.0 |  1.11 | 0.6x |
| Q38 |  0.7 |  4.1 |  1.09 | 0.6x |
| Q39 |  1.1 |  4.4 |  2.14 | 0.5x |
| Q40 |  0.8 |  3.9 |  1.00 | 0.8x |
| Q41 |  0.6 |  4.0 |  0.96 | 0.6x |
| Q42 |  0.5 |  4.0 |  1.04 | 0.5x |
| **Total** | **260** | **409** | **201.5** | **1.3x** |

## Summary

| Metric | Value |
|---|---|
| Queries passed | **43/43 (100%)** |
| Total time (Iceberg) | **260s** |
| Total time (Hive Parquet) | 409s |
| Total time (Spark) | 201.5s |
| **Iceberg vs Hive** | **1.6x faster** |
| **Iceberg vs Spark** | **1.3x slower** |
| Trino-OS wins vs Spark | 19/43 |
| Spark wins | 24/43 |
| Avg per query (Iceberg) | 6.0s |
| Fastest | 0.1s (Q0) |
| Slowest | 27.3s (Q23) |

## Key Improvements over Hive Parquet Benchmark

| Factor | Hive (16GB heap) | Iceberg (32GB heap) | Impact |
|---|---|---|---|
| **Per-query overhead** | ~4s floor | ~0.1-0.5s | Eliminated REST/parse overhead visible in simple queries |
| **Table format** | Raw Parquet via Hive | Iceberg v2 with metadata | Predicate pushdown using column stats |
| **Date/Time types** | INTEGER/BIGINT workaround | Native DATE/TIMESTAMP | No runtime conversion needed |
| **Heap size** | 16GB | 32GB | More memory for complex queries |
| **Query memory** | ~10GB | ~22.9GB | Handles Q28/Q32-Q34 without OOM |

## Where Trino-OS Beats Spark (19 queries)

- **Q0 (0.1s vs 0.78s)**: Simple COUNT(*) — Iceberg metadata optimization
- **Q3 (0.4s vs 0.91s)**: AVG(UserID) — fast single-column scan
- **Q6 (0.2s vs 0.81s)**: MIN/MAX(EventDate) — Iceberg metadata pushdown
- **Q7 (0.3s vs 1.01s)**: GROUP BY with filter
- **Q10-Q11 (1.0-1.2s vs 1.9s)**: GROUP BY with DISTINCT
- **Q15 (2.2s vs 3.51s)**: GROUP BY UserID
- **Q19 (0.5s vs 1.11s)**: Point lookup
- **Q23 (27.3s vs 39.4s)**: SELECT * WHERE LIKE ORDER BY LIMIT — TopN pushdown
- **Q25 (1.2s vs 1.48s)**: ORDER BY SearchPhrase
- **Q36-Q42**: Filtered queries with CounterID = 62

## Where Spark Still Wins

- **Q29 (18.2s vs 5.23s, 3.5x)**: 90-column SUM — Spark's vectorized reader
- **Q33-Q34 (20s vs 10s, 2x)**: Heavy GROUP BY URL — Spark's hash aggregation
- **Q16-Q17 (8-10s vs 5-6s, 1.6x)**: Multi-column GROUP BY

## Analysis

The 32GB heap + Iceberg format eliminated the two biggest bottlenecks:
1. **Per-query overhead reduced from ~4s to ~0.1s** — the Hive benchmark's floor was caused by Parquet file scanning on every query. Iceberg metadata caches after first access.
2. **Proper DATE/TIMESTAMP types** — queries use native date predicates instead of integer arithmetic.
3. **More query memory** — prevents OOM on heavy aggregation queries.

The overall ratio went from **2.0x slower than Spark** to **1.3x slower**, with Trino-OS actually beating Spark on 19/43 queries. The remaining gap is in heavy aggregation queries where Spark's vectorized reader and shuffle implementation are more optimized.
