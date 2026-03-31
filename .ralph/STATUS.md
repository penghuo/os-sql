status: WORKING
iteration: 28

## Current State
- Score: 32/43 within 2x in full benchmark
- Correctness: 36/43 PASS (no regression)
- Machine: r5.4xlarge (16 vCPU, 124GB RAM), 48GB heap, 4 shards, 1 segment/shard
- Optimizations committed:
  1. Fused DV scan for COUNT(DISTINCT) — eliminates 800MB temp array
  2. Right-sized LongOpenHashSet from 512MB to 800KB initial
  3. Prefetch-batched hash probing for single-key GROUP BY (12% improvement)

## Queries Above 2x (11 remaining)
Q04(3.34x) Q08(4.04x) Q09(4.71x) Q11(5.94x) Q13(7.26x)
Q14(2.02x) Q15(3.45x) Q16(6.29x) Q18(8.21x) Q35(3.15x) Q39(14.45x)

Note: Q14 at 2.02x is borderline noise (1.484s vs 1.470s target). In isolation it's ~1.9s (2.57x).
Q19-Q22 recovered from Q18 GC cascade (work in full benchmark).

## Root Cause Analysis
All remaining above-2x queries are fundamentally limited by:
1. **Hash map/set cache misses**: High-cardinality GROUP BY/COUNT(DISTINCT) creates maps >L3 cache
2. **Per-doc DocValues access**: advanceExact() binary search per doc per column
3. **ClickHouse structural advantage**: Two-level hash table, batch processing, SIMD

## What Was Tried This Iteration
1. ✅ Fused DV scan for collectDistinctValuesRaw: 10% improvement for Q04
2. ❌ Doc-range parallelism for single-segment COUNT(DISTINCT): no improvement
3. ✅ Prefetch-batched hash probing: 12% improvement for Q15 in isolation
4. ❌ Pre-loaded DocValues for filtered queries (Q39): only 8% improvement, reverted
5. ✅ Right-sized hash set pre-allocation: reduced memory pressure

## Next Steps
1. To reach 38/43, need to flip 6 more queries — all need 30-86% reduction
2. Most promising approaches not yet tried:
   - Two-level hash table (like ClickHouse) — major effort, could help Q04/Q08/Q09/Q15/Q35
   - DataFusionBridge JNI for grouped COUNT(DISTINCT) — unused native path exists
   - Increase parallelism via more shards (8 instead of 4)
   - Move to m5.8xlarge (32 vCPU) for 2x more parallelism
3. Q14 at 2.02x could flip with noise — not a code change

## Evidence
- Full benchmark: /tmp/full_iter28_final/r5.4xlarge.json (32/43 within 2x)
- Correctness: 36/43 PASS
- Build: BUILD SUCCESSFUL
- Git: commit cc0175dc0 (perf changes), 81569d350 (docs) on wukong branch
