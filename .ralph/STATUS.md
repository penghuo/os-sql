status: WORKING
iteration: 23

## Current State
- Score: 26/43 within 2x (stable, same as baseline)
- Correctness: 39/43 PASS (no regression)
- Machine: r5.4xlarge (16 vCPU, 124GB RAM), 48GB heap, 4 shards, 4 segments/shard
- Q04 improved: 2.38s → 1.62s (5.49x → 3.73x, 32% faster)
- Q05 improved: 3.58s → 2.47s (5.19x → 3.58x, 31% faster)

## Queries Above 2x (16)
Q02(3.23x) Q04(3.73x) Q05(3.58x) Q08(4.70x) Q09(5.91x) Q11(6.50x)
Q13(8.30x) Q14(2.37x) Q15(3.57x) Q16(7.66x) Q18(10.47x) Q28(2.41x)
Q29(2.69x) Q32(3.18x) Q35(4.08x) Q39(14.82x)

## What Was Done
1. Q04: Parallel hash insertion — two-phase approach with parallel DocValues load + parallel per-segment LongOpenHashSet construction + merge
2. Q05: ConcurrentHashMap parallel string insertion — workers insert directly into ConcurrentHashMap.newKeySet() instead of producing String[] chunks and sequentially inserting
3. Both verified: BUILD SUCCESSFUL, correctness 39/43 PASS

## Next Steps
1. Performance target (≥38/43) NOT achievable on r5.4xlarge with code optimizations alone
2. All 16 above-2x queries hit optimized fused paths — bottleneck is Lucene DocValues overhead
3. 23 iterations of optimization have exhausted code-level improvements
4. To reach ≥38/43: need m5.8xlarge (32 vCPU) or architectural changes (columnar storage, vectorized execution)
5. Potential remaining optimizations: Q14 (2.37x, needs 16% improvement), Q28 (2.41x, REGEXP_REPLACE caching), Q29 (2.69x, expression eval)

## Evidence
- Full benchmark: /tmp/full_iter23d/r5.4xlarge.json (26/43 within 2x)
- Q04 isolated: /tmp/q04_iter23d/r5.4xlarge.json (1.56s best)
- Q05 isolated: /tmp/q05_iter23d/r5.4xlarge.json (2.58s best)
- Correctness: 39/43 PASS (/tmp/correctness_iter23d.log)
- Build: BUILD SUCCESSFUL
