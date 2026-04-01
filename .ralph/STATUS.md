status: WORKING
iteration: 30

## Current State
- Score: 33/43 within 2x in full benchmark
- Correctness: 36/43 PASS (no regression)
- Machine: r5.4xlarge (16 vCPU, 124GB RAM), 48GB heap, 4 shards, 1 segment/shard
- Optimizations committed this iteration:
  1. Improved prefetch batching: batch 32 (was 16), prefetch both keys[] and accData[]
  2. Columnar loading + prefetch path for executeDerivedSingleKeyNumeric (Q35)
  3. DataFusionBridge .so bundled in JAR (not yet wired into execution)

## Queries Above 2x (10 remaining)
Q04(3.40x) Q08(3.49x) Q09(4.93x) Q11(5.79x) Q13(7.02x)
Q15(3.13x) Q16(6.65x) Q18(8.36x) Q35(3.30x) Q39(14.39x)

Note: Q14 fluctuates between 1.89x-2.24x (borderline noise).

## What Was Tried This Iteration
1. ✅ Improved prefetch batching (batch 32, keys+accData): 18-19% improvement in isolation for Q35/Q15
2. ✅ Columnar loading for executeDerivedSingleKeyNumeric: enables prefetch path for Q35
3. ✅ DataFusionBridge .so bundled in JAR (45MB, x86-64)
4. ✅ Scaled DataFusion benchmark (25M rows): DF is 2.3x faster than Rust HashMap for GROUP BY + COUNT(DISTINCT)
5. ❌ Radix-partitioned aggregation: 57-66% REGRESSION (overhead of 2 extra passes + scatter + per-bucket map creation + mergeFrom exceeds L2 cache benefit)
6. ❌ Group-partitioned Q09 (executeMixedDedupWithHashSets): 2x REGRESSION (loading 4 columns × 25M rows = 800MB allocation + 5 passes over data)
7. ❌ DataFusionBridge for COUNT(DISTINCT): NOT FEASIBLE — coordinator merge requires raw LongOpenHashSets, not just counts

## Root Cause Analysis (Updated)
All remaining 10 above-2x queries are fundamentally limited by:
1. **Hash map/set cache misses**: High-cardinality GROUP BY/COUNT(DISTINCT) creates maps >L3 cache
2. **Per-doc DocValues access**: Sequential but decompression overhead per block
3. **ClickHouse structural advantage**: Columnar format (direct mmap), two-level hash table (SIMD probing), batch processing

Key findings this iteration:
- Prefetch batching helps ~18% but can't close the 3-14x gap
- Radix partitioning DOES NOT WORK in Java: the overhead of scatter + per-bucket map creation + mergeFrom exceeds the L2 cache benefit. This is fundamentally different from C++ where memory allocation is cheaper.
- DataFusionBridge can't help COUNT(DISTINCT) queries because coordinator needs raw hash sets for cross-shard union
- DataFusion IS 2.3x faster than Rust HashMap for GROUP BY + COUNT(DISTINCT) at 25M rows, but the JNI integration path is blocked by the coordinator merge requirement

## Next Steps
1. To reach 38/43, need to flip 5 more queries — all need 30-86% reduction
2. Approaches NOT yet tried:
   - DataFusionBridge for GROUP BY + COUNT(*) queries (Q15, Q16, Q18, Q35) — coordinator CAN merge partial counts
   - Increase parallelism via more shards (8 instead of 4) — requires re-indexing
   - Move to m5.8xlarge (32 vCPU) for 2x more parallelism
   - Custom DocValues codec with lighter compression
   - SIMD-friendly hash probing via Vector API (JDK 21 incubator)
   - Lighter group-partitioning for Q09 (load only key0+key1, not aggregate columns)
   - Swiss Table-style metadata array for faster probing

## Evidence
- Full benchmark: /tmp/full_iter30_final/r5.4xlarge.json (33/43 within 2x)
- Correctness: 36/43 PASS
- Build: BUILD SUCCESSFUL
- Git: ef5a2380c on wukong branch
- Isolated benchmarks: Q35 0.991s (was 1.209s), Q15 1.362s (was 1.684s)
