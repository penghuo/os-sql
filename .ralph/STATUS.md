status: WORKING
iteration: 32

## Current State
- Score: 32/43 within 2x (full benchmark); 33/43 isolated (Q15 within 2x at 1.87x)
- Correctness: 36/43 PASS (no regression)
- Machine: r5.4xlarge (16 vCPU, 124GB RAM), 48GB heap, 4 shards, 4 segments/shard
- Optimizations committed this iteration:
  1. NKey COUNT(DISTINCT) path: columnar loading + open-addressing (eliminates per-doc LongArrayKey allocation)
  2. Mixed dedup (Q09) path: columnar loading for key columns
  3. Varchar COUNT(DISTINCT) OOM fix: initial capacity 1024 → 16 for high-cardinality varchar GROUP BY
  4. Prefetch-batched merge for FlatSingleKeyMap/FlatTwoKeyMap/FlatThreeKeyMap (15% improvement for Q15)
  5. GC barrier tuned to 40% threshold for full-benchmark stability

## Queries Above 2x (11 remaining)
Q04(3.24x) Q08(3.45x) Q09(4.99x) Q11(6.10x) Q13(7.34x)
Q14(2.33x) Q15(2.36x) Q16(6.61x) Q18(8.17x) Q35(3.12x) Q39(14.71x)

## Key Improvements This Iteration
- Q15: 1.132s → 0.965s isolated (1.87x), 1.225s full (2.36x) — 15% improvement from prefetch-batched merge
- Q13 (varchar CD): no longer OOMs (was crashing, now 7.34x)
- Q05: was 5.8x → 0.98x ✓ (from NKey columnar loading in previous uncommitted changes)
- Q36: was 4.0x → 0.99x ✓
- Q37: was 2.7x → 0.39x ✓
- Q02: was 2.2x → 0.10x ✓
- Q30: was 2.3x → 1.11x ✓
- Q31: was 2.0x → 0.99x ✓

## Root Cause: Full Benchmark vs Isolated Gap
Q18 (GROUP BY UserID, minute, SearchPhrase) takes 30s and creates ~50M groups using ~6GB heap per shard.
After Q18, GC pressure degrades subsequent queries by 20-30%. This is why Q15 is 0.965s isolated but 1.225s in full benchmark.

## Next Steps
1. Q15 needs GC pressure mitigation or ~20% more improvement to stay within 2x in full benchmark
2. Q14 needs ~14% improvement (1.710s → ≤1.470s)
3. Q35 needs ~36% improvement (1.156s → ≤0.740s)
4. Q04 needs ~38% improvement (1.404s → ≤0.868s)
5. Remaining 7 queries (Q08, Q09, Q11, Q13, Q16, Q18, Q39) need 42-87% reduction — likely requires architectural changes
6. Key bottleneck: hash table cache misses for 17M+ unique keys don't fit in L3 cache (25MB)

## Evidence
- Full benchmark: /tmp/full_iter32e/r5.4xlarge.json (32/43 within 2x)
- Isolated Q15: /tmp/q16_merge3/r5.4xlarge.json (0.973s, 1.87x)
- Correctness: 36/43 PASS
- Build: BUILD SUCCESSFUL
- Git: 6422456e0 on wukong branch
