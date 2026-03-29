status: WORKING
iteration: 13

## Current State
- Score: 25/43 within 2x of CH-Parquet (unchanged from baseline)
- Correctness: 39/43 PASS (no regression)
- Machine: r5.4xlarge (16 vCPU, 124GB RAM), 48GB heap, 4 shards, 4 segments/shard

## Queries Within 2x (25)
Q00(0.40x) Q01(0.16x) Q03(1.36x) Q06(0.14x) Q07(0.36x) Q10(1.42x) Q12(0.65x)
Q17(0.01x) Q19(0.09x) Q20(0.01x) Q21(0.02x) Q22(0.04x) Q23(0.00x) Q24(0.03x)
Q25(1.69x) Q26(0.03x) Q31(1.19x) Q32(1.80x) Q33(0.33x) Q34(0.32x) Q37(0.41x)
Q38(0.61x) Q40(0.24x) Q41(0.73x) Q42(0.44x)

## Queries Above 2x (18, sorted by ratio)
Q14(2.31x) Q27(2.36x) Q29(2.40x) Q30(2.49x) Q28(3.16x) Q02(3.32x) Q35(4.07x)
Q08(4.37x) Q05(5.33x) Q04(5.34x) Q09(5.66x) Q36(6.56x) Q13(7.37x) Q16(7.51x)
Q18(9.92x) Q11(12.53x) Q39(28.92x) Q15(33.85x)

## Key Findings from Iteration 13

### Optimization Attempts
1. **MAX_CAPACITY=32M for FlatSingleKeyMap**: Q15 67s (4x regression) — cache thrashing from 512MB hash map
2. **Single-pass numBuckets=1**: Q15 67s — same cache thrashing issue
3. **Cardinality sampling for numBuckets**: Q27 improved 4.2s→1.7s (0.93x!) but Q15 regressed 17s→78s due to page cache eviction from sampling I/O
4. **Single-pass multi-bucket (executeSingleKeyNumericFlatMultiBucket)**: Q15 137-148s, Q27 31.6s — catastrophic cache thrashing from all bucket maps in memory simultaneously
5. **Removed parallel multi-bucket**: Q15 27s (1.6x regression) — sequential bucket execution is 2x slower than parallel

### Root Cause Analysis
- **Fundamental bottleneck**: Lucene DocValues are 3-10x slower than ClickHouse columnar format for full-table scans
- **Cache locality is critical**: Hash maps must fit in L3 cache (~25-40MB). Maps >100MB cause severe cache thrashing
- **Parallel multi-bucket is essential**: Running bucket passes concurrently hides DocValues latency
- **Cardinality sampling works but has side effects**: Reading 100K docs from DocValues evicts page cache, hurting subsequent scans
- **COUNT(DISTINCT) fusion already exists**: 4 specialized methods in TransportShardExecuteAction handle all patterns

### Achievability Assessment
- **Borderline (Q14, Q27, Q29, Q30)**: Need 10-20% improvement. Possible with targeted optimizations but very tight margins
- **Medium (Q02, Q28, Q35)**: Need 35-50% improvement. Unlikely with code optimizations alone
- **Hard (Q04-Q18, Q36, Q39, Q15)**: Need >50% improvement. NOT achievable without architectural changes (columnar storage, vectorized execution)
- **Target ≥38/43 requires 13 more queries within 2x**: NOT achievable with code optimizations on r5.4xlarge (16 vCPU)

## Next Steps
1. Performance target NOT MET (25/43 vs ≥38/43)
2. Remaining gap is fundamental: Lucene DocValues overhead vs ClickHouse columnar format
3. Architectural changes needed: columnar storage format, vectorized execution, or pre-aggregation
4. Realistic target on r5.4xlarge: ~27-28/43 with aggressive micro-optimizations
5. To reach 38/43: need m5.8xlarge (32 vCPU) or architectural changes

## Evidence
- Full benchmark: /tmp/full_iter13/r5.4xlarge.json (warmup=3, tries=5)
- Correctness: 39/43 PASS (/tmp/correctness_iter13.log)
- Uncommitted changes: FlatTwoKeyMap/FlatThreeKeyMap sentinel optimization + LongOpenHashSet pre-sizing + restored parallel multi-bucket
