# Phase C Progress Report -- 2026-03-19

## Summary

| Metric | Value |
|--------|-------|
| Queries within 2x of ClickHouse | **8 / 43** |
| Total time (CH baseline) | 32.97s |
| Total time (Phase B / off mode) | 327.05s |
| Total time (Phase C latest) | 160.79s |
| Overall speedup vs Phase B | **2.03x** |
| Overall ratio vs ClickHouse | **4.88x** |

**Hardware**: ClickHouse on c6a.4xlarge (16 vCPU, 32GB); OpenSearch on m5.8xlarge (32 vCPU, 128GB).

---

## Full Query Results

| Query | CH (s) | Phase B (s) | Phase C (s) | Ratio vs CH | Within 2x | Phase C vs B |
|-------|--------|-------------|-------------|-------------|-----------|-------------|
| Q0  | 0.001 | 0.007 | 0.007 | 7.0x | -- | 1.00x |
| Q1  | 0.006 | 0.013 | 0.008 | 1.3x | YES | 1.62x |
| Q2  | 0.021 | 0.705 | 0.216 | 10.3x | -- | 3.26x |
| Q3  | 0.027 | 1.907 | 1.788 | 66.2x | -- | 1.07x |
| Q4  | 0.353 | 2.502 | 2.357 | 6.7x | -- | 1.06x |
| Q5  | 0.623 | 4.563 | 4.615 | 7.4x | -- | 0.99x |
| Q6  | 0.010 | 0.007 | 0.007 | 0.7x | YES | 1.00x |
| Q7  | 0.009 | 0.236 | 0.260 | 28.9x | -- | 0.91x |
| Q8  | 0.452 | 3.289 | 3.269 | 7.2x | -- | 1.01x |
| Q9  | 0.522 | 4.973 | 5.234 | 10.0x | -- | 0.95x |
| Q10 | 0.147 | 0.325 | 0.337 | 2.3x | -- | 0.96x |
| Q11 | 0.143 | 3.089 | 2.478 | 17.3x | -- | 1.25x |
| Q12 | 0.599 | 4.413 | 0.540 | 0.9x | YES | 8.17x |
| Q13 | 0.804 | 5.912 | 6.281 | 7.8x | -- | 0.94x |
| Q14 | 0.597 | 10.157 | 1.695 | 2.8x | -- | 5.99x |
| Q15 | 0.384 | 2.765 | 2.802 | 7.3x | -- | 0.99x |
| Q16 | 1.709 | 52.175 | 10.932 | 6.4x | -- | 4.77x |
| Q17 | 0.999 | 53.770 | 28.280 | 28.3x | -- | 1.90x |
| Q18 | 3.041 | 70.773 | 28.543 | 9.4x | -- | 2.48x |
| Q19 | 0.003 | 0.009 | 0.008 | 2.7x | -- | 1.12x |
| Q20 | 0.312 | 1.048 | 0.934 | 3.0x | -- | 1.12x |
| Q21 | 0.098 | 1.245 | 1.174 | 12.0x | -- | 1.06x |
| Q22 | 0.717 | 5.574 | 1.360 | 1.9x | YES | 4.10x |
| Q23 | 0.393 | 1.055 | 0.927 | 2.4x | -- | 1.14x |
| Q24 | 0.147 | 0.015 | 0.071 | 0.5x | YES | 0.21x |
| Q25 | 0.192 | 0.313 | 0.441 | 2.3x | -- | 0.71x |
| Q26 | 0.149 | 0.018 | 0.018 | 0.1x | YES | 1.00x |
| Q27 | 0.083 | 2.692 | 2.687 | 32.4x | -- | 1.00x |
| Q28 | 9.582 | 24.664 | 25.412 | 2.7x | -- | 0.97x |
| Q29 | 0.029 | 0.443 | 0.413 | 14.2x | -- | 1.07x |
| Q30 | 0.342 | 2.084 | 2.098 | 6.1x | -- | 0.99x |
| Q31 | 0.562 | 2.481 | 2.471 | 4.4x | -- | 1.00x |
| Q32 | 3.793 | 14.772 | 14.563 | 3.8x | -- | 1.01x |
| Q33 | 2.782 | 21.697 | 1.039 | 0.4x | YES | 20.88x |
| Q34 | 2.851 | 21.115 | 1.138 | 0.4x | YES | 18.55x |
| Q35 | 0.297 | 2.033 | 2.089 | 7.0x | -- | 0.97x |
| Q36 | 0.043 | 0.483 | 0.498 | 11.6x | -- | 0.97x |
| Q37 | 0.021 | 0.275 | 0.297 | 14.1x | -- | 0.93x |
| Q38 | 0.017 | 0.229 | 0.251 | 14.8x | -- | 0.91x |
| Q39 | 0.077 | 2.605 | 2.568 | 33.4x | -- | 1.01x |
| Q40 | 0.013 | 0.153 | 0.223 | 17.2x | -- | 0.69x |
| Q41 | 0.009 | 0.225 | 0.251 | 27.9x | -- | 0.90x |
| Q42 | 0.008 | 0.238 | 0.213 | 26.6x | -- | 1.12x |

---

## Breakdown: 8 Queries Within 2x of ClickHouse

| Query | CH (s) | Phase C (s) | Ratio | Optimization |
|-------|--------|-------------|-------|-------------|
| Q1  | 0.006 | 0.008 | 1.3x | Filtered COUNT(*) -- fast shard aggregation |
| Q6  | 0.010 | 0.007 | 0.7x | MIN/MAX on date field -- metadata shortcut (faster than CH) |
| Q12 | 0.599 | 0.540 | 0.9x | Fused GROUP BY with TopN pushdown for SearchPhrase COUNT(*) |
| Q22 | 0.717 | 1.360 | 1.9x | Title LIKE + GROUP BY with COUNT(DISTINCT) -- hash-partitioned aggregation |
| Q24 | 0.147 | 0.071 | 0.5x | SELECT * with WHERE + ORDER BY LIMIT -- index pushdown (faster than CH) |
| Q26 | 0.149 | 0.018 | 0.1x | Sorted field ORDER BY LIMIT -- pure index scan (15x faster than CH) |
| Q33 | 2.782 | 1.039 | 0.4x | Narrow filtered GROUP BY -- CounterID=62 filter prunes 99.99% of rows |
| Q34 | 2.851 | 1.138 | 0.4x | Same narrow filter as Q33 -- CounterID=62 with different GROUP BY keys |

**Pattern**: Queries where OpenSearch beats CH tend to exploit index-level metadata or narrow WHERE clauses that eliminate nearly all rows before aggregation.

---

## Categorization of 35 Remaining Queries by Root Cause

### Category 1: Full-Table Scan Bottleneck (16 queries)

These queries scan most or all of the 100M rows with no selective filter. The gap is dominated by raw scan throughput: ClickHouse processes columnar data at ~1-2 GB/s per core, while OpenSearch reads from Lucene doc-value segments at lower throughput.

| Queries | Description | Typical Ratio |
|---------|-------------|---------------|
| Q0 | COUNT(*) -- trivial in CH (metadata), requires scan in OS | 7.0x |
| Q3 | AVG(UserID) -- full scan, single aggregation | 66.2x |
| Q4 | COUNT(DISTINCT UserID) -- full scan + hash set | 6.7x |
| Q5 | COUNT(DISTINCT SearchPhrase) -- full scan + hash set | 7.4x |
| Q7 | AdvEngineID GROUP BY + ORDER BY -- small result, full scan | 28.9x |
| Q8 | RegionID GROUP BY with COUNT(DISTINCT) | 7.2x |
| Q9 | RegionID GROUP BY with mixed aggs + COUNT(DISTINCT) | 10.0x |
| Q15 | UserID GROUP BY COUNT(*) | 7.3x |
| Q19 | Simple UserID point lookup (latency overhead) | 2.7x |
| Q27 | CounterID GROUP BY with AVG(length(URL)) + HAVING | 32.4x |
| Q29 | SUM of 90 expressions over ResolutionWidth | 14.2x |
| Q30 | SearchEngineID, ClientIP GROUP BY | 6.1x |
| Q31 | WatchID, ClientIP GROUP BY (filtered) | 4.4x |
| Q39 | Many-SUM expression scan | 33.4x |
| Q2 | SUM + COUNT + AVG full scan (improved 3.3x by Phase C) | 10.3x |
| Q10 | MobilePhoneModel COUNT(DISTINCT) with filter | 2.3x |

### Category 2: High-Cardinality GROUP BY (7 queries)

GROUP BY with millions of distinct keys. OpenSearch must build large hash maps and potentially multi-pass. CH benefits from columnar sort and merge.

| Queries | Description | Typical Ratio |
|---------|-------------|---------------|
| Q16 | UserID, SearchPhrase GROUP BY ORDER BY -- 52s->11s (4.8x improvement) | 6.4x |
| Q17 | UserID, SearchPhrase GROUP BY LIMIT 10 (no ORDER BY) | 28.3x |
| Q18 | UserID, minute, SearchPhrase GROUP BY ORDER BY | 9.4x |
| Q28 | REGEXP_REPLACE GROUP BY with HAVING | 2.7x |
| Q32 | WatchID, ClientIP GROUP BY (full table, high cardinality) | 3.8x |
| Q13 | SearchPhrase COUNT(DISTINCT UserID) -- decomposed | 7.8x |
| Q14 | SearchEngineID, SearchPhrase GROUP BY (improved 6x) | 2.8x |

### Category 3: COUNT(DISTINCT) Decomposition Overhead (4 queries)

COUNT(DISTINCT) is decomposed into GROUP BY + COUNT(*), doubling the key space and preventing direct aggregation.

| Queries | Description | Typical Ratio |
|---------|-------------|---------------|
| Q4 | COUNT(DISTINCT UserID) -- global | 6.7x |
| Q5 | COUNT(DISTINCT SearchPhrase) -- global | 7.4x |
| Q9 | RegionID with mixed aggs including COUNT(DISTINCT) | 10.0x |
| Q11 | MobilePhone, MobilePhoneModel with COUNT(DISTINCT) | 17.3x |

### Category 4: String Processing (5 queries)

Queries involving LIKE, REGEXP_REPLACE, or length() on VARCHAR columns. String operations are expensive in Java/Lucene.

| Queries | Description | Typical Ratio |
|---------|-------------|---------------|
| Q20 | URL LIKE '%google%' COUNT(*) | 3.0x |
| Q21 | URL LIKE '%google%' with GROUP BY | 12.0x |
| Q25 | SearchPhrase ORDER BY SearchPhrase | 2.3x |
| Q27 | AVG(length(URL)) with HAVING | 32.4x |
| Q28 | REGEXP_REPLACE on Referer with HAVING | 2.7x |

### Category 5: Narrow-Filter Queries with Residual Overhead (3 queries)

Already fast in absolute terms (<0.5s) but CH is extremely fast on these, making the ratio large.

| Queries | Description | Typical Ratio |
|---------|-------------|---------------|
| Q35 | ClientIP expression GROUP BY (CounterID=62 filter) | 7.0x |
| Q36-Q38 | CounterID=62 filtered GROUP BY variants | 11-15x |
| Q40-Q42 | CounterID=62 filtered GROUP BY with OFFSET | 17-27x |

*Note: Some queries appear in multiple categories due to overlapping root causes.*

---

## Top 10 Recommended Next Optimizations (by absolute time savings)

| Rank | Target | Current (s) | CH (s) | Potential Savings | Optimization |
|------|--------|-------------|--------|-------------------|-------------|
| 1 | **Q18** | 28.54 | 3.04 | ~25.5s | Intra-shard parallel segment scanning for high-cardinality GROUP BY. Q18 has UserID+minute+SearchPhrase -- 3-key GROUP BY benefits most from parallelism. |
| 2 | **Q17** | 28.28 | 1.00 | ~27.3s | LIMIT-without-ORDER-BY early termination. Q17 asks for LIMIT 10 with no ORDER BY -- can stop scanning after finding 10 groups. Currently scans all 100M rows. |
| 3 | **Q28** | 25.41 | 9.58 | ~15.8s | Native REGEXP_REPLACE pushdown and streaming aggregation for Referer domain extraction. Avoid Java regex overhead per row. |
| 4 | **Q32** | 14.56 | 3.79 | ~10.8s | WatchID+ClientIP high-cardinality GROUP BY -- hash-partitioned aggregation with parallel passes (same approach as Q33 fix). |
| 5 | **Q16** | 10.93 | 1.71 | ~9.2s | Further optimize UserID+SearchPhrase GROUP BY ORDER BY with TopN-aware hash partitioning. Already improved 4.8x from Phase B. |
| 6 | **Q13** | 6.28 | 0.80 | ~5.5s | SearchPhrase COUNT(DISTINCT UserID) -- native HyperLogLog or exact COUNT(DISTINCT) without decomposition into GROUP BY. |
| 7 | **Q9** | 5.23 | 0.52 | ~4.7s | RegionID mixed aggregation with COUNT(DISTINCT) -- avoid decomposition, compute in single pass with fused aggregator. |
| 8 | **Q5** | 4.62 | 0.62 | ~4.0s | COUNT(DISTINCT SearchPhrase) -- native HashSet merge without GROUP BY decomposition. Similar to existing Q10 native path. |
| 9 | **Q8** | 3.27 | 0.45 | ~2.8s | RegionID COUNT(DISTINCT UserID) -- same native COUNT(DISTINCT) optimization as Q5/Q9. |
| 10 | **Q15** | 2.80 | 0.38 | ~2.4s | UserID GROUP BY COUNT(*) -- TopN pushdown with min-heap pruning during scan. |

**Combined potential savings from top 10: ~108s (67% of total gap with CH)**

---

## Phase C Wins So Far

The following optimizations landed in Phase C and produced measurable improvements:

| Optimization | Queries Helped | Speedup |
|-------------|---------------|---------|
| Fused GROUP BY with TopN pushdown | Q12 (8.2x), Q14 (6.0x), Q16 (4.8x) | Major |
| Hash-partitioned high-cardinality GROUP BY | Q33 (20.9x), Q34 (18.6x) | Major |
| Title/URL LIKE + hash-partitioned COUNT(DISTINCT) | Q22 (4.1x) | Moderate |
| SUM/AVG full-scan expression fusion | Q2 (3.3x) | Moderate |
| Native COUNT(DISTINCT) with HashSet merge | Q18 (2.5x) | Moderate |

---

## Conclusion

Phase C has cut total execution time in half (327s to 161s) and brought 8 queries within 2x of ClickHouse. The dominant remaining bottleneck is **raw scan throughput** for full-table queries and **high-cardinality GROUP BY** for multi-key aggregations. The top 3 optimizations (Q17 early termination, Q18 parallel segments, Q28 native regex) would address ~68s of the remaining 128s gap.
