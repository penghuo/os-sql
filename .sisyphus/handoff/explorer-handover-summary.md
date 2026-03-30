# DQE Phase D Handover Summary

## Handover Document Structure (`docs/handover/2026-03-24-phase-d-handover.md`)

13 sections covering:
1. **Goal**: Optimize DQE so all 43 ClickBench queries run within 2x of ClickHouse-Parquet on c6a.4xlarge
2. **Prerequisites**: JDK 21, 16+ vCPU, 64+ GB RAM
3. **Environment Setup**: 6-step process (clone → install → heap → load-full → load-1m → verify)
4. **Dev Iteration Loop**: compile (~5s) → deploy (~3min) → correctness (1M, ~2min) → benchmark (100M, ~25min)
5. **Baseline**: CH-Parquet official results (NOT native MergeTree). File: `benchmarks/clickbench/results/performance/clickhouse_parquet_official/c6a.4xlarge.json`
6. **Query Numbering**: `--query N` is 1-based, JSON `result[N]` is 0-based, doc uses 0-based Q00-Q42
7. **Status at handover**: 25/43 within 2x
8. **Prior work**: 6 commits (early termination, Weight+Scorer, parallel segments, AVG decomposition, etc.)
9. **Root cause analysis**: 4 categories (COUNT(DISTINCT), high-cardinality GROUP BY, VARCHAR GROUP BY, borderline)
10. **Code map**: Key files and execution flow
11. **Known pitfalls**: Plugin reload kills benchmarks, JIT warmup, GC pressure, OrdinalMap cost
12. **Recommended next steps**: COUNT(DISTINCT) fusion, parallelize flat paths, hash-partitioned agg, borderline fixes
13. **Reference docs**: Links to plans, reports, research docs

## Ralph State (`.ralph/STATUS.md`)

- **Status**: WORKING, iteration 19
- **Current Score**: 27/43 within 2x (up from 25/43 at handover)
- **Correctness**: 39/43 PASS
- **Machine**: r5.4xlarge (16 vCPU, 124GB RAM), 48GB heap, 4 shards, 4 segments/shard

### Queries Within 2x (27)
Q00(0.40x) Q01(0.17x) Q03(1.66x) Q06(0.15x) Q07(1.37x) Q10(0.87x) Q12(0.64x)
Q17(0.01x) Q19(0.08x) Q20(0.01x) Q21(0.02x) Q22(0.04x) Q23(0.00x) Q24(0.03x)
Q25(1.92x) Q26(0.04x) Q27(1.20x) Q30(1.53x) Q31(1.26x) Q33(0.29x) Q34(0.29x)
Q36(1.31x) Q37(0.41x) Q38(0.49x) Q40(1.10x) Q41(1.61x) Q42(0.63x)

### Queries Above 2x (16, sorted by ratio)
Q28(2.24x) Q14(2.25x) Q29(2.51x) Q32(3.14x) Q02(3.26x) Q35(4.05x) Q08(4.44x)
Q04(4.88x) Q05(4.97x) Q09(5.50x) Q16(6.79x) Q11(6.94x) Q13(7.76x) Q18(9.61x)
Q39(26.76x) Q15(167.31x)

### Key Iteration 19 Changes
- Try-catch overflow replaces pre-estimation bucket calculation (single-key + two-key flat paths)
- Pre-sized constructors for FlatSingleKeyMap/FlatTwoKeyMap (cap 4M)
- Q30: 6.20x → 1.53x (now within 2x)
- Q15: 174x → 1.48s in isolation, but 167x in full benchmark (GC cascade from Q16/Q18)

## Ralph Logs Summary (`.ralph/LOGS.md` — 19 iterations)

### Iteration History & Key Outcomes
| Iter | Score | Key Changes |
|------|-------|-------------|
| 1 | 29/43* | Multi-pass single-key GROUP BY. Q15: 74s→15s |
| 2 | — | Bitset lockstep for VARCHAR GROUP BY. FAILED (5.6x still) |
| 3 | — | LongOpenHashSet coordinator merge. FAILED (merge not bottleneck) |
| 4 | — | Q31/Q32 optimization. FAILED (EOFException — corrupted index) |
| 5 | — | Q29 REGEXP_REPLACE. FAILED (19.7M regex evals inherently slow) |
| 6 | — | Tasks 6-7 wrap-up. FAILED (all tasks failed) |
| 7 | 18/43 | Full benchmark baseline. Partial shard failure tolerance. Force-merge attempted |
| 8 | 25-28/43 | Parallel collectDistinctStringsRaw (Q05 -27%), parallel Q09 (-60%), force-merge (net negative) |
| 9 | 24-25/43 | Scalar COUNT(DISTINCT) merge optimization. Q04 -12%, Q08 -25% |
| 10 | 25/43 | Count-only merge for grouped COUNT(DISTINCT). Bitset lockstep REVERTED |
| 11 | 25/43 | FlatSingleKeyMap sentinel optimization. Q15: 101x→30x. MAX_CAPACITY tuning REVERTED |
| 12 | 25/43 | Forward-only DV advance. Near-MatchAll bitset lockstep. Q14: 2.75x→2.07x best |
| 13 | 25/43 | Cardinality sampling (Q27 4.2s→1.7s but Q15 regressed). All REVERTED |
| 14 | 25/43 | Mixed-type N-key COUNT(DISTINCT) path. Q11: 12.53x→6.40x |
| 15 | 26/43 | Q36 filtered parallelism (2.38x→1.15x). Parallel expr-key GROUP BY |
| 16 | 25/43 | Global ordinals for Q05/Q13. Marginal improvement |
| 17 | 25-26/43 | Exhaustive analysis of all borderline queries. No code changes kept |
| 18 | 26/43 | Columnar cache for scanSegmentForCountDistinct. Marginal |
| 19 | 27/43 | Try-catch overflow + pre-sized maps. Q30: 6.20x→1.53x |

*Iter 1 score was on different machine/code state

### Recurring Themes Across All Iterations
1. **Fundamental bottleneck**: Lucene DocValues 3-10x slower than ClickHouse columnar (15-20ns vs 2-5ns per value)
2. **GC pressure**: High-cardinality GROUP BY with 16 concurrent maps causes GC cascades
3. **Q15 is the hardest**: 4.4M unique UserIDs, needs massive hash maps, GC-sensitive
4. **Borderline queries are noise-dependent**: Q03, Q14, Q29 fluctuate across 2x boundary between runs
5. **Force-merge was net negative**: Fewer segments reduce parallel GROUP BY effectiveness
6. **Many optimizations REVERTED**: ~60% of attempted optimizations were reverted due to regressions

## ClickHouse-Parquet Baseline (c6a.4xlarge, best of 3 runs)

| Q | CH Best(s) | Q | CH Best(s) | Q | CH Best(s) | Q | CH Best(s) |
|---|-----------|---|-----------|---|-----------|---|-----------|
| Q00 | 0.025 | Q11 | 0.248 | Q22 | 1.963 | Q33 | 3.113 |
| Q01 | 0.063 | Q12 | 0.268 | Q23 | 4.135 | Q34 | 3.107 |
| Q02 | 0.105 | Q13 | 0.644 | Q24 | 0.493 | Q35 | 3.107 |
| Q03 | 0.111 | Q14 | 0.956 | Q25 | 0.272 | Q36 | 0.370 |
| Q04 | 0.434 | Q15 | 0.520 | Q26 | 0.494 | Q37 | 0.121 |
| Q05 | 0.690 | Q16 | 1.794 | Q27 | 1.790 | Q38 | 0.102 |
| Q06 | 0.065 | Q17 | 1.170 | Q28 | 9.526 | Q39 | 0.070 |
| Q07 | 0.059 | Q18 | 3.668 | Q29 | 0.096 | Q40 | 0.143 |
| Q08 | 0.540 | Q19 | 0.110 | Q30 | 0.778 | Q41 | 0.071 |
| Q09 | 0.614 | Q20 | 1.169 | Q31 | 1.095 | Q42 | 0.054 |
| Q10 | 0.248 | Q21 | 1.334 | Q32 | 4.493 | | |

### Hardest Remaining Queries (by absolute CH time × current ratio)
1. **Q15** (CH: 0.520s, DQE: 167x) — GROUP BY UserID ORDER BY COUNT(*) LIMIT 10. 4.4M unique keys, GC cascade
2. **Q18** (CH: 3.668s, DQE: 9.61x) — GROUP BY UserID, minute, SearchPhrase. Multi-key high-cardinality
3. **Q39** (CH: 0.070s, DQE: 26.76x) — Filtered 5-key GROUP BY with CASE WHEN
4. **Q13** (CH: 0.644s, DQE: 7.76x) — GROUP BY SearchPhrase + COUNT(DISTINCT UserID)
5. **Q16** (CH: 1.794s, DQE: 6.79x) — GROUP BY UserID, SearchPhrase ORDER BY COUNT(*) LIMIT 10

### Most Achievable Targets (closest to 2x)
1. **Q28** (2.24x, gap=2.3s) — REGEXP_REPLACE GROUP BY HAVING
2. **Q14** (2.25x, gap=0.2s) — GROUP BY SearchEngineID, SearchPhrase filtered
3. **Q29** (2.51x, gap=0.05s) — 90× SUM, noise-dependent
4. **Q32** (3.14x, gap=5.1s) — GROUP BY WatchID, ClientIP full-table
5. **Q02** (3.26x, gap=0.13s) — SUM+COUNT+AVG scalar

## Key Code Files
| File | Purpose |
|------|---------|
| `dqe/.../transport/TransportShardExecuteAction.java` (~2200 lines) | Shard dispatch, 13+ fast paths |
| `dqe/.../source/FusedGroupByAggregate.java` (~11700 lines) | All GROUP BY execution |
| `dqe/.../source/FusedScanAggregate.java` (~1600 lines) | Scalar aggregation |
| `dqe/.../transport/TransportTrinoSqlAction.java` (~2000 lines) | Coordinator, result merging |

## Build & Test Commands
```bash
./gradlew :dqe:compileJava                                    # Compile (~5s)
cd benchmarks/clickbench && bash run/run_all.sh reload-plugin  # Deploy (~3min)
bash run/run_all.sh correctness                                # Correctness (1M, ~2min)
bash run/run_opensearch.sh --warmup 3 --num-tries 5 --query N --output-dir /tmp/qN  # Single query
bash run/run_opensearch.sh --warmup 3 --num-tries 5 --output-dir /tmp/full           # Full benchmark
```
