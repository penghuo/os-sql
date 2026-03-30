status: WORKING
iteration: 18

## Current State
- Score: 26/43 within 2x (full run), same as iter17 clean run
- Correctness: 39/43 PASS (no regression)
- Machine: r5.4xlarge (16 vCPU, 124GB RAM), 48GB heap, 4 shards, 4 segments/shard
- Code change: columnar cache for scanSegmentForCountDistinct (Q08 path)

## Queries Within 2x (26)
Q00(0.36x) Q01(0.14x) Q03(1.57x) Q06(0.12x) Q07(0.36x) Q10(0.84x) Q12(0.55x)
Q17(0.01x) Q19(0.08x) Q20(0.01x) Q21(0.02x) Q22(0.04x) Q23(0.00x) Q24(0.02x)
Q25(1.68x) Q26(0.05x) Q31(1.12x) Q32(1.69x) Q33(0.31x) Q34(0.28x) Q36(1.17x)
Q37(0.44x) Q38(0.53x) Q40(0.21x) Q41(0.73x) Q42(0.52x)

## Queries Above 2x (17, sorted by ratio)
Q29(2.16x) Q28(2.24x) Q14(2.28x) Q30(2.40x) Q27(2.73x) Q02(3.52x) Q35(4.00x)
Q08(4.29x) Q04(5.04x) Q05(5.45x) Q09(5.53x) Q11(6.88x) Q16(7.08x) Q13(7.78x)
Q18(9.78x) Q39(29.42x) Q15(32.89x)

## What Was Done This Iteration
1. Explored unexplored optimization paths: columnar cache extension, DirectReader bypass, batch DV reads
2. Implemented columnar cache for scanSegmentForCountDistinct (Q08 path): loads both key columns into long[] arrays before iterating
3. Tried extending columnar cache to executeMixedDedupWithHashSets (Q09 path) — REVERTED due to memory pressure regression
4. Q03 improved from 2.12x to 1.57x (noise-dependent, now solidly within 2x in this run)
5. Q08 columnar cache shows marginal improvement in isolation (2.309s → 2.274s, ~1.5%)

## Performance Ceiling Analysis
- 18 iterations of optimization have exhausted code-level improvements on r5.4xlarge
- Remaining gap is fundamental: Lucene DocValues per-doc decode (~3-5ns) vs ClickHouse columnar bulk reads (~0.5-1ns)
- Borderline queries (Q29 2.16x, Q28 2.24x, Q14 2.28x, Q30 2.40x) need 10-20% improvement
- The 10-20% gap cannot be closed with code optimizations — requires either:
  1. More CPU (m5.8xlarge with 32 vCPU)
  2. Bypassing Lucene DocValues API (DirectReader/PackedInts access)
  3. Custom columnar storage format

## Next Steps
1. **DirectReader bypass**: Access Lucene's internal packed integer data directly, bypassing SortedNumericDocValues API. Could reduce per-value cost from ~3ns to ~1ns. Requires reflection or codec fork.
2. **Move to m5.8xlarge**: Doubling CPU count would halve per-shard execution time, potentially bringing borderline queries within 2x.
3. **Q16 OOM mitigation**: Q16 causes GC cascades that affect Q15-Q27 in sequential benchmark runs.

## Evidence
- Full benchmark: /tmp/iter18_full/r5.4xlarge.json (26/43 within 2x)
- Q08 isolated: /tmp/iter18_q08_isolated/r5.4xlarge.json (2.274s best)
- Correctness: 39/43 PASS (/tmp/correctness_iter18c.log)
- Build: BUILD SUCCESSFUL
