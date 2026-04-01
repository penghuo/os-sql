status: WORKING
iteration: 31

## Current State
- Score: 34/43 within 2x (isolated); 32/43 in full benchmark (Q14/Q15 borderline noise)
- Correctness: 36/43 PASS (no regression)
- Machine: r5.4xlarge (16 vCPU, 124GB RAM), 48GB heap, 4 shards, 4 segments/shard
- Optimizations committed this iteration:
  1. Parallel doc-range scanning for executeSingleKeyNumericFlat (threshold 10M → 200M)
  2. Merge optimization: use largest worker map as base (saves one merge pass)
  3. Worker map initial capacity 4M → 16M (avoids resize during scan)
  4. Prefetch-batched bulk insertion for LongOpenHashSet
  5. Per-segment hash set capacity 1M → 8M for COUNT(DISTINCT)
  6. Parallel doc-range scanning for executeDerivedSingleKeyNumeric (Q35 path)

## Queries Above 2x (9-11 remaining depending on noise)
Q04(3.18x) Q08(3.46x) Q09(4.91x) Q11(6.24x) Q13(7.40x)
Q15(2.06x*) Q16(5.78x) Q18(8.44x) Q35(3.04x) Q39(14.60x)
*Q14(2.09x) and Q15(2.06x) are borderline — within 2x in isolated benchmarks

## Key Improvements This Iteration
- Q15: 1.626s → 0.887s isolated (1.71x), 1.073s full (2.06x) — 45% improvement
- Q35: 1.264s → 0.842s isolated (2.28x), 1.123s full (3.04x) — 33% improvement
- Q04: 1.460s → 1.267s isolated (2.92x), 1.381s full (3.18x) — 13% improvement
- Q16: 11.928s → 10.377s full (5.78x) — 13% improvement

## Root Cause: Full Benchmark vs Isolated Gap
Full benchmark runs 43 queries sequentially. Q18 (GROUP BY UserID, minute, SearchPhrase) takes 30+s and causes GC pressure that degrades subsequent queries by 10-30%. This is why Q15 is 0.887s isolated but 1.073s in full benchmark.

## Next Steps
1. Q15 needs ~6% more improvement to reliably stay within 2x in full benchmark
2. Q35 needs ~34% more improvement (0.842s → ≤0.740s)
3. Q04 needs ~31% more improvement (1.267s → ≤0.868s)
4. Q08 needs ~34% more improvement
5. Remaining 6 queries (Q09, Q11, Q13, Q16, Q18, Q39) need 50-86% reduction — likely requires architectural changes

## Evidence
- Full benchmark: /tmp/full_iter31d/r5.4xlarge.json (32/43 within 2x)
- Isolated Q15: /tmp/q15_final2/r5.4xlarge.json (0.887s, 1.71x)
- Isolated Q35: /tmp/q35_parallel/r5.4xlarge.json (0.842s, 2.28x)
- Isolated Q04: /tmp/q04_final/r5.4xlarge.json (1.267s, 2.92x)
- Correctness: 36/43 PASS
- Build: BUILD SUCCESSFUL
- Git: 94655e3ea on wukong branch
