status: WORKING
iteration: 29

## Current State
- Score: 33/43 within 2x in full benchmark
- Correctness: 36/43 PASS (no regression)
- Machine: r5.4xlarge (16 vCPU, 124GB RAM), 48GB heap, 4 shards, 1 segment/shard
- Optimizations committed this iteration:
  1. Pre-sized LongOpenHashSet for collectDistinctValuesRaw (avoids 7 resizes for high-cardinality)
  2. Group-partitioned COUNT(DISTINCT) in scanSegmentForCountDistinct (L2 cache-friendly per-group processing)

## Queries Above 2x (10 remaining)
Q04(3.21x) Q08(3.42x) Q09(4.69x) Q11(5.72x) Q13(7.06x)
Q15(3.40x) Q16(5.98x) Q18(8.30x) Q35(3.14x) Q39(14.66x)

Note: Q14 flipped to within 2x (was borderline 2.02x). Q07 fluctuates with noise.

## What Was Tried This Iteration
1. ✅ Pre-sized LongOpenHashSet: 9% improvement for Q04 in isolation (1.448s → 1.314s)
2. ✅ Group-partitioned COUNT(DISTINCT) for Q08: 16% improvement (2.021s → 1.704s isolated, 2.182s → 1.845s in full)
3. ❌ Radix-partitioned hash set construction for Q04: 28% REGRESSION (1.448s → 1.855s) — overhead of two passes + bucket allocation exceeds cache benefit
4. ❌ Sort-merge based COUNT(DISTINCT) for Q04: 11% REGRESSION (1.448s → 1.607s) — Arrays.sort O(n log n) slower than hash O(n) even with cache misses
5. ❌ Two-level hash table in LongOpenHashSet: 17% REGRESSION for Q04 — extra indirection (subKeys[bucket]) adds cache miss, JIT can't optimize as well as flat path

## Root Cause Analysis (Updated)
All remaining 10 above-2x queries are fundamentally limited by:
1. **Hash map/set cache misses**: High-cardinality GROUP BY/COUNT(DISTINCT) creates maps >L3 cache
2. **Per-doc DocValues access**: Sequential but decompression overhead per block
3. **ClickHouse structural advantage**: Columnar format (direct mmap), two-level hash table (SIMD probing), batch processing

Key finding this iteration: Two-level hash table in Java is SLOWER than flat hash table due to:
- Extra array indirection per operation (subKeys[bucket] dereference)
- JIT compiler optimizes flat array access better than nested array access
- The cache benefit of smaller sub-arrays is offset by the routing overhead

## Next Steps
1. To reach 38/43, need to flip 5 more queries — all need 30-86% reduction
2. Approaches NOT yet tried:
   - DataFusionBridge JNI (native library built but not wired into OpenSearch plugin deployment)
   - Increase parallelism via more shards (8 instead of 4) — requires re-indexing
   - Move to m5.8xlarge (32 vCPU) for 2x more parallelism
   - Custom DocValues codec with lighter compression
   - SIMD-friendly hash probing via Vector API (JDK 21 incubator)
3. Group-partitioning could be extended to Q09 (executeMixedDedupWithHashSets) and Q11/Q13 (VARCHAR key paths)

## Evidence
- Full benchmark: /tmp/full_iter29_final/r5.4xlarge.json (33/43 within 2x)
- Correctness: 36/43 PASS
- Build: BUILD SUCCESSFUL
- Git: changes in FusedScanAggregate.java + TransportShardExecuteAction.java on wukong branch
