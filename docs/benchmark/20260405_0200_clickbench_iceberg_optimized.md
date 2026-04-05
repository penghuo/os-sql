# ClickBench Benchmark: Optimized Trino-OpenSearch vs Spark

**Date**: 2026-04-05 02:00 UTC
**Dataset**: hits (99,997,497 rows, 105 columns) — Iceberg table with Parquet format

## Environment

| Setting | Value |
|---|---|
| **Engine** | Trino 440 embedded in OpenSearch 3.6 |
| **Machine** | Single node, 32 CPUs, 123GB RAM |
| **Heap** | 32GB JVM |
| **Query memory** | ~22.9GB (70% of heap) |
| **Table format** | Apache Iceberg v2, Parquet + Zstd |
| **Trino nodes** | 4 (in-process distributed execution) |
| **task_concurrency** | 32 (matched to CPU count) |
| **dictionary_aggregation** | true |
| **Parquet block size** | 64MB / 65536 rows |
| **Endpoint** | `/_plugins/_trino_sql/v1/statement` |
| **Runs** | Best of 3 |

## Spark Baseline

| Setting | Value |
|---|---|
| **Engine** | Spark (ClickBench reference) |
| **Machine** | c6a.4xlarge (16 vCPU, 32GB RAM) |
| **Runs** | Best of 3 (warm cache) |
| **Source** | [ClickBench](https://github.com/ClickHouse/ClickBench/blob/main/spark/results/c6a.4xlarge.json) |

## Results: 42/43 queries beat Spark

| Query | Trino-OS (s) | Spark (s) | Ratio | Winner |
|-------|-------------|-----------|-------|--------|
| Q0  |  0.04 |  0.78 | 0.04x | **Trino-OS** |
| Q1  |  0.12 |  0.81 | 0.15x | **Trino-OS** |
| Q2  |  0.23 |  0.93 | 0.25x | **Trino-OS** |
| Q3  |  0.14 |  0.91 | 0.15x | **Trino-OS** |
| Q4  |  0.50 |  3.45 | 0.14x | **Trino-OS** |
| Q5  |  1.15 |  3.12 | 0.37x | **Trino-OS** |
| Q6  |  0.10 |  0.81 | 0.12x | **Trino-OS** |
| Q7  |  0.13 |  1.01 | 0.13x | **Trino-OS** |
| Q8  |  1.10 |  3.78 | 0.29x | **Trino-OS** |
| Q9  |  2.30 |  4.58 | 0.50x | **Trino-OS** |
| Q10 |  0.30 |  1.92 | 0.16x | **Trino-OS** |
| Q11 |  0.34 |  1.90 | 0.18x | **Trino-OS** |
| Q12 |  1.10 |  3.12 | 0.36x | **Trino-OS** |
| Q13 |  1.42 |  5.45 | 0.26x | **Trino-OS** |
| Q14 |  1.21 |  3.28 | 0.37x | **Trino-OS** |
| Q15 |  0.60 |  3.51 | 0.17x | **Trino-OS** |
| Q16 |  2.70 |  5.80 | 0.47x | **Trino-OS** |
| Q17 |  2.44 |  4.78 | 0.51x | **Trino-OS** |
| Q18 |  5.60 | 10.44 | 0.54x | **Trino-OS** |
| Q19 |  0.13 |  1.11 | 0.12x | **Trino-OS** |
| Q20 |  2.10 |  3.53 | 0.60x | **Trino-OS** |
| Q21 |  1.90 |  4.34 | 0.44x | **Trino-OS** |
| Q22 |  3.60 |  7.12 | 0.51x | **Trino-OS** |
| Q23 |  8.90 | 39.38 | 0.23x | **Trino-OS** |
| Q24 |  0.64 |  1.69 | 0.38x | **Trino-OS** |
| Q25 |  0.44 |  1.48 | 0.30x | **Trino-OS** |
| Q26 |  0.63 |  1.69 | 0.37x | **Trino-OS** |
| Q27 |  2.10 |  5.27 | 0.40x | **Trino-OS** |
| Q28 |  8.70 | 18.48 | 0.47x | **Trino-OS** |
| **Q29** | **6.20** | **5.23** | **1.19x** | **Spark** |
| Q30 |  1.05 |  3.19 | 0.33x | **Trino-OS** |
| Q31 |  1.21 |  3.86 | 0.31x | **Trino-OS** |
| Q32 |  5.70 | 12.63 | 0.45x | **Trino-OS** |
| Q33 |  6.45 | 10.04 | 0.64x | **Trino-OS** |
| Q34 |  6.70 | 10.00 | 0.67x | **Trino-OS** |
| Q35 |  1.50 |  3.62 | 0.42x | **Trino-OS** |
| Q36 |  0.35 |  1.18 | 0.30x | **Trino-OS** |
| Q37 |  0.31 |  1.11 | 0.28x | **Trino-OS** |
| Q38 |  0.34 |  1.09 | 0.31x | **Trino-OS** |
| Q39 |  0.61 |  2.14 | 0.29x | **Trino-OS** |
| Q40 |  0.21 |  1.00 | 0.21x | **Trino-OS** |
| Q41 |  0.19 |  0.96 | 0.19x | **Trino-OS** |
| Q42 |  0.19 |  1.04 | 0.18x | **Trino-OS** |
| **Total** | **82** | **202** | **0.41x** | **Trino-OS** |

## Summary

| Metric | Value |
|---|---|
| Queries passed | **43/43 (100%)** |
| Trino-OS wins | **42/43 (98%)** |
| Spark wins | **1/43 (Q29)** |
| Total time (Trino-OS) | **82s** |
| Total time (Spark) | 202s |
| Overall speedup | **2.5x faster than Spark** |
| Avg per query (Trino-OS) | 1.9s |
| Avg per query (Spark) | 4.7s |
| Fastest query | 0.04s (Q0) |

## Optimization Journey

| Config | Total | Wins | vs Spark |
|---|---|---|---|
| Hive Parquet, 16GB, 1 node, cold | 409s | 1/43 | 2.0x slower |
| Iceberg, 32GB, 1 node, cold | 260s | 19/43 | 1.3x slower |
| + task_concurrency=32, best-of-3 | 231s | 23/43 | 1.15x slower |
| + 2-node distributed | 133s | 40/43 | 0.66x (1.5x faster) |
| **+ 4-node distributed** | **82s** | **42/43** | **0.41x (2.5x faster)** |

## Key Optimizations

1. **4-node distributed execution** — Each in-process Trino node gets its own task execution threads, effectively multiplying parallelism for hash-partitioned operations (GROUP BY, JOIN)
2. **task_concurrency = 32** — Matches available CPU count (was default 4)
3. **dictionary_aggregation = true** — Optimizes GROUP BY on low-cardinality columns
4. **Parquet block size 64MB/65536 rows** — Larger batches for better CPU utilization
5. **Iceberg table format** — Column statistics enable predicate pushdown
6. **32GB heap** — Sufficient memory for all queries without spill

## Why Q29 Remains Slower

Q29 computes `SUM(ResolutionWidth + N)` for N=0..89 — 90 separate SUM expressions from a single column. This is a vectorization microbenchmark:
- **Spark**: Reads column in columnar batches, computes all 90 sums in tight vectorized loops
- **Trino**: Row-at-a-time evaluation through 90 separate expression trees

The 1.19x gap (5.8s vs 5.23s) represents the fundamental overhead of Trino's Volcano-model row processing vs Spark's columnar batch processing for wide aggregation queries.

## Hardware Note

Trino-OS runs on a 32-CPU machine vs Spark's 16 vCPU c6a.4xlarge. The 2x CPU advantage enables 4-node distributed execution. On equivalent hardware (16 CPUs), expect Trino-OS results to be ~2x slower (still competitive with Spark).
