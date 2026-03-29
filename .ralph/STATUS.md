status: WORKING
iteration: 15

## Current State
- Score: 26/43 within 2x of CH-Parquet (up from 25/43 in iteration 14)
- Correctness: 39/43 PASS (no regression)
- Machine: r5.4xlarge (16 vCPU, 124GB RAM), 48GB heap, 4 shards, 4 segments/shard
- Two optimizations delivered: parallel expr-key GROUP BY, filtered high-cardinality VARCHAR parallelism

## Queries Within 2x (26)
Q00(0.40x) Q01(0.16x) Q03(1.76x) Q06(0.14x) Q07(0.36x) Q10(1.49x) Q12(0.56x)
Q17(0.01x) Q19(0.08x) Q20(0.01x) Q21(0.02x) Q22(0.04x) Q23(0.01x) Q24(0.03x)
Q25(1.54x) Q26(0.04x) Q31(1.30x) Q32(1.77x) Q33(0.31x) Q34(0.32x) Q36(1.15x↓)
Q37(0.48x) Q38(0.49x) Q40(0.24x) Q41(0.87x) Q42(0.44x)

## Queries Above 2x (17, sorted by ratio)
Q14(2.14x) Q27(2.17x) Q28(2.27x) Q29(2.30x) Q30(2.50x) Q35(3.94x)
Q02(4.10x) Q08(4.51x) Q05(4.51x) Q04(5.14x) Q09(5.57x) Q11(6.35x)
Q16(6.80x) Q13(7.69x) Q18(9.96x) Q39(29.09x) Q15(31.96x)

## Changes This Iteration
1. **Parallel expr-key GROUP BY** (FusedGroupByAggregate): Multi-segment parallelism for expression-key GROUP BY path using Weight+Scorer per segment, ordinal pre-computation, and worker-local HashMap merge.
2. **Filtered high-cardinality VARCHAR parallelism** (FusedGroupByAggregate): Modified `executeSingleVarcharCountStar` to allow parallelism for filtered queries even when ordinal count > 500K. Q36: 2.38x → 1.15x.
3. **System.gc() removal REVERTED**: Removing System.gc() calls after GROUP BY caused OOM/GC storms during sequential benchmark runs. Restored.
4. **Q14 N-key varchar parallelism REVERTED**: Adding segment-level parallelism to `executeNKeyVarcharPath` caused regression (1.589s → 2.345s) due to HashMap merge overhead exceeding parallelism benefit.

## Exhaustive Analysis of Remaining Optimization Paths

### Borderline Queries (within 0.5x of 2x threshold)
- Q14 (2.14x, 1.574s vs 0.735s): 2-key varchar GROUP BY (SearchEngineID + SearchPhrase). Hits `executeNKeyVarcharPath` — sequential. Parallelism attempted and reverted (regression). Bottleneck: per-doc HashMap<MergedGroupKey> lookups with BytesRefKey allocation.
- Q27 (2.17x, 3.877s vs 1.790s): Single-key numeric GROUP BY (CounterID) with AVG(length(URL)). Hits numeric GROUP BY path with computed aggregate argument. Bottleneck: per-doc length(URL) evaluation via DocValues.
- Q28 (2.27x, 21.585s vs 9.526s): REGEXP_REPLACE GROUP BY. Pattern already cached, ordinal pre-computation reduces evals from ~921K to ~16K per segment. Bottleneck: regex matching cost on ~16K ordinals × segments.
- Q29 (2.30x, 0.221s vs 0.096s): 90× SUM(col+N). Already uses algebraic shortcut (reads 1 column). Bottleneck: plan compilation for 90 expressions + JVM overhead. Noise-dependent (was 2.07x in iter14).

### Root Cause: Lucene DocValues Overhead
The fundamental performance gap is Lucene's SortedNumericDocValues/SortedSetDocValues encoding:
- Per-doc decode: ~15-20ns per nextDoc()+nextValue() (variable-length integer decompression)
- ClickHouse Parquet: bulk SIMD-optimized column reads, ~2-5ns per value
- This is NOT fixable with code optimizations — requires storage format changes

## Next Steps
1. Borderline queries (Q14, Q27, Q28, Q29) require architectural changes or are noise-dependent
2. Q16 OOM issue: massive hash maps (UserID × SearchPhrase) cause GC storms, crashing subsequent queries
3. To reach 38/43: need m5.8xlarge (32 vCPU) or architectural changes (columnar storage, vectorized execution)
4. Realistic ceiling on r5.4xlarge: ~27-28/43 with aggressive micro-optimizations

## Evidence
- Full benchmark: /tmp/full_iter15d/r5.4xlarge.json (warmup=3, tries=5, clean run)
- Correctness: 39/43 PASS (/tmp/correctness_iter15d.log)
- Q36 improvement: 0.288s → 0.139s (2.38x → 1.15x) via filtered high-cardinality parallelism
- Q03 improvement: 0.294s → 0.195s (2.65x → 1.76x, noise-dependent)
- Build: BUILD SUCCESSFUL
