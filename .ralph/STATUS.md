status: WORKING
iteration: 22

## Current State
- Score: 27/43 within 2x (stable, same as baseline)
- Correctness: 39/43 PASS (no regression)
- Machine: r5.4xlarge (16 vCPU, 124GB RAM), 48GB heap, 4 shards, 4 segments/shard
- Q15 improved: 55.5s → 1.92s (106x → 3.68x) but still above 2x threshold

## Queries Above 2x (16)
Q14(2.53x) Q29(2.78x) Q28(2.38x) Q32(3.08x) Q02(3.48x) Q35(4.06x)
Q05(5.19x) Q08(4.76x) Q04(5.49x) Q09(5.85x) Q11(6.71x) Q16(7.07x)
Q13(8.23x) Q18(10.67x) Q39(14.71x) Q15(3.68x — was 106x)

## What Was Done
1. Implemented sequential scan for high-cardinality MatchAll COUNT(*) GROUP BY
2. Q15: 55.5s → 1.92s (96% improvement)
3. No queries flipped within/above 2x — score stable at 27/43
4. Correctness: 39/43 PASS (no regression)

## Next Steps
1. Performance target (≥38/43) NOT achievable on r5.4xlarge with code optimizations alone
2. All 16 above-2x queries hit optimized fused paths — bottleneck is Lucene DocValues overhead
3. 22 iterations of optimization have exhausted code-level improvements
4. To reach ≥38/43: need m5.8xlarge (32 vCPU) or architectural changes

## Evidence
- Full benchmark: /tmp/iter22_opt2/r5.4xlarge.json (27/43 within 2x)
- Q15 isolated: /tmp/q16_opt/r5.4xlarge.json (1.88s best)
- Correctness: 39/43 PASS (/tmp/correctness_iter22.log)
- Build: BUILD SUCCESSFUL
