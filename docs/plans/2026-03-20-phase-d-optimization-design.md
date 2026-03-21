# Phase D: DQE Optimization Plan — 16/43 to 43/43

## Baseline Correction

Previous reports used local ClickHouse **native MergeTree** numbers. The correct
comparison target is the official ClickBench **ClickHouse-Parquet** benchmark
(`c6a.4xlarge`), which reads Parquet files directly — a fairer comparison for
OpenSearch's Lucene segment-based storage.

| Metric | Old (native CH) | Corrected (CH-Parquet) |
|--------|:---------------:|:----------------------:|
| Within 2x | 8/43 | **16/43** |
| CH total time | 32.97s | **47.7s** |
| DQE total time | 160.79s | **157.3s** |
| Overall ratio | 4.88x | **3.3x** |

DQE benchmark re-run with `--warmup 3 --num-tries 5` (JIT-compiled).

**Baseline files:**
- CH-Parquet: `results/performance/clickhouse_parquet_official/c6a.4xlarge.json`
- DQE: `results/performance/opensearch_corrected/m5.8xlarge.json`

---

## Current State: 16/43 Within 2x

### Already within 2x (16 queries)

| Q | CH-Pq (s) | DQE (s) | Ratio | Why fast |
|---|-----------|---------|-------|----------|
| Q00 | 0.025 | 0.007 | 0.3x | COUNT(*) metadata |
| Q01 | 0.063 | 0.008 | 0.1x | Filtered COUNT, fast shard agg |
| Q06 | 0.065 | 0.007 | 0.1x | MIN/MAX date field shortcut |
| Q10 | 0.248 | 0.372 | 1.5x | MobilePhoneModel filter + GROUP BY |
| Q12 | 0.644 | 0.620 | 1.0x | Fused GROUP BY + TopN pushdown |
| Q19 | 0.110 | 0.008 | 0.1x | Point lookup |
| Q20 | 1.169 | 0.795 | 0.7x | URL LIKE + COUNT |
| Q21 | 1.334 | 1.018 | 0.8x | URL LIKE + GROUP BY |
| Q22 | 1.963 | 1.212 | 0.6x | Title LIKE + hash-partitioned COUNT(DISTINCT) |
| Q23 | 4.135 | 0.798 | 0.2x | SearchPhrase LIKE + ORDER BY + LIMIT |
| Q24 | 0.493 | 0.015 | 0.0x | WHERE + ORDER BY LIMIT index pushdown |
| Q25 | 0.272 | 0.332 | 1.2x | SearchPhrase ORDER BY |
| Q26 | 0.494 | 0.017 | 0.0x | Sorted field ORDER BY LIMIT |
| Q27 | 1.790 | 2.722 | 1.5x | CounterID GROUP BY AVG(length(URL)) |
| Q33 | 3.107 | 1.137 | 0.4x | Narrow filter CounterID=62 |
| Q34 | 3.107 | 1.153 | 0.4x | Narrow filter CounterID=62 |

### Borderline (4 queries — need small improvement)

| Q | CH-Pq (s) | DQE (s) | Ratio | Gap to 2x |
|---|-----------|---------|-------|-----------|
| Q02 | 0.105 | 0.260 | 2.48x | 50ms |
| Q14 | 0.735 | 1.726 | 2.35x | 256ms |
| Q31 | 1.095 | 2.470 | 2.26x | 280ms |
| Q40 | 0.071 | 0.176 | 2.48x | 34ms |

### Remaining 23 queries (>2.5x)

| Q | CH-Pq (s) | DQE (s) | Ratio | Category |
|---|-----------|---------|-------|----------|
| Q03 | 0.111 | 1.779 | 16.0x | Full-scan scalar agg |
| Q04 | 0.434 | 2.397 | 5.5x | COUNT(DISTINCT) global |
| Q05 | 0.690 | 4.387 | 6.4x | COUNT(DISTINCT) global |
| Q07 | 0.059 | 0.268 | 4.5x | Low-card GROUP BY |
| Q08 | 0.540 | 3.287 | 6.1x | GROUP BY + COUNT(DISTINCT) |
| Q09 | 0.614 | 4.918 | 8.0x | GROUP BY mixed aggs |
| Q11 | 0.268 | 2.517 | 9.4x | GROUP BY + COUNT(DISTINCT) |
| Q13 | 0.956 | 6.012 | 6.3x | High-card GROUP BY + COUNT(DISTINCT) |
| Q15 | 0.520 | 2.854 | 5.5x | UserID GROUP BY COUNT(*) |
| Q16 | 1.794 | 11.252 | 6.3x | High-card 2-key GROUP BY ORDER BY |
| Q17 | 1.170 | 27.218 | 23.3x | High-card 2-key GROUP BY LIMIT |
| Q18 | 3.668 | 27.230 | 7.4x | High-card 3-key GROUP BY |
| Q28 | 9.526 | 25.108 | 2.6x | REGEXP_REPLACE GROUP BY |
| Q29 | 0.096 | 0.376 | 3.9x | SUM of 90 expressions |
| Q30 | 0.778 | 2.106 | 2.7x | 2-key GROUP BY |
| Q32 | 4.493 | 14.581 | 3.2x | High-card 2-key numeric |
| Q35 | 0.370 | 2.095 | 5.7x | Narrow filter + GROUP BY |
| Q36 | 0.121 | 0.479 | 4.0x | Narrow filter + GROUP BY |
| Q37 | 0.102 | 0.300 | 2.9x | Narrow filter |
| Q38 | 0.070 | 0.203 | 2.9x | Narrow filter |
| Q39 | 0.143 | 2.538 | 17.7x | Many-SUM expression scan |
| Q41 | 0.062 | 0.176 | 2.8x | Narrow filter + OFFSET |
| Q42 | 0.054 | 0.259 | 4.8x | Narrow filter + OFFSET |

---

## Optimization Phases

### Phase D1: Borderline Quick Wins (4 queries → within 2x)

Target: 16/43 → 20/43. Each needs a small improvement.

| # | Query | Gap | Optimization | Effort |
|---|-------|-----|-------------|--------|
| D1.1 | **Q40** | 34ms | Reduce per-query fixed overhead (Weight/Scorer allocation) for narrow-filter + OFFSET queries. Pre-check empty segments. | 2h |
| D1.2 | **Q02** | 50ms | SUM+COUNT+AVG full-scan: tighten flat-array path, ensure recognition of mixed-agg pattern | 2h |
| D1.3 | **Q31** | 280ms | WatchID+ClientIP GROUP BY: optimize 2-key numeric path merge overhead | 3h |
| D1.4 | **Q14** | 256ms | SearchEngineID+SearchPhrase GROUP BY: improve TopN pushdown for filtered queries | 3h |

### Phase D2: Narrow-Filter Queries (7 queries)

Target: 20/43 → 27/43. All filter on CounterID=62 (0.005% selectivity).

Root cause: 200ms+ fixed overhead (Weight/Scorer per 24 segments, buffer allocation)
when actual work is 2-5ms.

| # | Queries | Optimization | Effort |
|---|---------|-------------|--------|
| D2.1 | Q35-Q42 | **Segment skip for narrow filters**: skip Scorer allocation on segments with 0 matching docs. Use `IndexSearcher.count(query)` or min/max metadata to prune. | 4h |
| D2.2 | Q35-Q42 | **Cache Weight object**: reuse across segments instead of rebuilding. Pre-check `Query.matches()` per segment. | 2h |
| D2.3 | Q35,Q36 | **Reduce GROUP BY overhead on tiny result sets**: when filter passes <1K rows, use simpler HashMap path instead of FlatMap + parallel docrange. | 3h |

Expected: Q37 (2.9x→<2x), Q38 (2.9x→<2x), Q41 (2.8x→<2x), Q42 (4.8x→<2x),
Q35 (5.7x→<2x), Q36 (4.0x→<2x), Q40 already in D1.

### Phase D3: Full-Scan Scalar + Low-Cardinality (6 queries)

Target: 27/43 → 33/43.

| # | Queries | Optimization | Effort |
|---|---------|-------------|--------|
| D3.1 | **Q03** (16x) | AVG(UserID) full-scan: recognize SUM+COUNT+AVG flat-array pattern. Currently falls to generic per-doc path. | 3h |
| D3.2 | **Q07** (4.5x) | AdvEngineID GROUP BY: low cardinality (<20 values), use array-indexed agg instead of HashMap | 2h |
| D3.3 | **Q29** (3.9x) | SUM of 90 expressions: algebraic shortcut `SUM(col+k) = SUM(col) + k*COUNT(*)` | 3h |
| D3.4 | **Q39** (17.7x) | Many-SUM expression scan: inline expression evaluation, reduce BlockExpression overhead | 4h |
| D3.5 | **Q04** (5.5x) | COUNT(DISTINCT UserID): native HashSet merge without GROUP BY decomposition | 4h |
| D3.6 | **Q05** (6.4x) | COUNT(DISTINCT SearchPhrase): share OrdinalMap cache with scalar path | 3h |

### Phase D4: COUNT(DISTINCT) Decomposition (4 queries)

Target: 33/43 → 37/43.

Root cause: COUNT(DISTINCT) is decomposed into GROUP BY + COUNT(*), doubling key
space and preventing fused single-pass aggregation.

| # | Queries | Optimization | Effort |
|---|---------|-------------|--------|
| D4.1 | **Q08** (6.1x) | RegionID GROUP BY + COUNT(DISTINCT UserID): fused single-pass with HashSet per group | 5h |
| D4.2 | **Q09** (8.0x) | RegionID mixed aggs + COUNT(DISTINCT): same as D4.1 with additional accumulators | 3h |
| D4.3 | **Q11** (9.4x) | MobilePhone+Model GROUP BY + COUNT(DISTINCT): extend to 2-key path | 3h |
| D4.4 | **Q13** (6.3x) | SearchPhrase COUNT(DISTINCT UserID): high-cardinality, needs ordinal-based approach | 5h |

### Phase D5: High-Cardinality GROUP BY (5 queries)

Target: 37/43 → 42/43.

These are the hardest queries — millions of unique groups.

| # | Query | Optimization | Effort |
|---|-------|-------------|--------|
| D5.1 | **Q17** (23.3x) | **Early termination**: LIMIT 10 without ORDER BY should stop after finding 10 groups. Currently scans all 100M rows. Potential: 27s → <2.3s. | 4h |
| D5.2 | **Q15** (5.5x) | UserID GROUP BY COUNT(*): TopN pushdown with min-heap pruning during scan | 4h |
| D5.3 | **Q30** (2.7x) | SearchEngineID+ClientIP: optimize 2-key numeric merge | 3h |
| D5.4 | **Q16** (6.3x) | UserID+SearchPhrase ORDER BY: further TopN-aware hash partitioning | 6h |
| D5.5 | **Q18** (7.4x) | 3-key GROUP BY: fused parallel path with pre-computed extract(minute) | 8h |

### Phase D6: Heavy Queries (2 queries)

Target: 42/43 → 43/43.

| # | Query | Optimization | Effort |
|---|-------|-------------|--------|
| D6.1 | **Q28** (2.6x) | REGEXP_REPLACE on Referer: hoist regex computation, cache compiled Pattern, or push to native | 6h |
| D6.2 | **Q32** (3.2x) | WatchID+ClientIP high-cardinality 2-key: hash-partitioned aggregation with more buckets | 4h |

---

## Execution Order

```
Phase D1 (borderline, 10h)  ──→  immediate wins: 20/43
Phase D2 (narrow filter, 9h) ──→  27/43
Phase D3 (scalar+lowcard, 19h) ──→  33/43
Phase D4 (COUNT DISTINCT, 16h) ──→  37/43
Phase D5 (high-card GROUP BY, 25h) ──→  42/43
Phase D6 (heavy, 10h) ──→  43/43
```

D1 and D2 are independent. D3 and D4 are independent. D5.1 (Q17 early
termination) is a standalone high-impact item that can be done at any point.

**Recommended parallel tracks:**
- Track A: D1 + D3 (borderline + scalar fixes)
- Track B: D2 + D5.1 (narrow filter + Q17 early termination)
- Track C: D4 (COUNT DISTINCT) after D3 establishes patterns
- Track D: D5.2-D5.5 + D6 (remaining high-card + heavy)

## Total Effort Estimate

~89 engineering hours across all phases. With parallel tracks, calendar time is
~40-50 hours of focused work.

## Correctness Rule

After ANY code change: `cd benchmarks/clickbench && bash run/run_all.sh correctness`

Benchmark re-run: `bash run/run_opensearch.sh --warmup 3 --num-tries 5 --output-dir results/performance/opensearch_corrected`

## Success Criteria

- 43/43 queries within 2x of official CH-Parquet c6a.4xlarge
- 32/43 correctness on 1M (no regression)
- 0 OOM, 0 circuit breaker, 0 crashes
- Each commit includes "x/43 within 2x" progress metric

## Risks

| Risk | Mitigation |
|------|-----------|
| JIT variance on borderline queries | 5 timed runs with 3 warmup passes |
| Q17 early termination may produce different results | Verify correctness: LIMIT without ORDER BY is non-deterministic |
| COUNT(DISTINCT) fused path is complex | Incremental: single-key first, then multi-key |
| High-card GROUP BY may need spill-to-disk | Hash-partitioned aggregation already proven on Q33/Q34 |
| Narrow-filter segment skip may miss edge cases | Guard: fall back to full scan if metadata unavailable |
