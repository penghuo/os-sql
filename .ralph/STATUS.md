status: WORKING
iteration: 28

## Current State
- Score: 32/43 within 2x in full benchmark (up from 29/43 in iter27)
- Correctness: 36/43 PASS (no regression)
- Machine: r5.4xlarge (16 vCPU, 124GB RAM), 48GB heap, 4 shards, 1 segment/shard
- Optimizations committed:
  1. Fused DV scan for COUNT(DISTINCT) — eliminates 800MB temp array
  2. Right-sized LongOpenHashSet from 512MB to 800KB initial
  3. Prefetch-batched hash probing for single-key GROUP BY (12% improvement)

## Queries Flipped This Iteration
- Q19: None → 0.08x (recovered from Q18 GC cascade)
- Q20: None → 0.01x (recovered)
- Q21: None → 0.02x (recovered)
- Q22: None → 0.03x (recovered)
- Q14: 1.92x → 2.08x (REGRESSED — borderline noise, was 1.53s in iter27)

## Queries Above 2x (11 remaining)
Q04(3.24x) Q08(4.03x) Q09(4.67x) Q11(6.18x) Q13(7.20x)
Q14(2.08x) Q15(3.62x) Q16(6.14x) Q18(8.39x) Q35(3.20x) Q39(14.24x)

## Root Cause Analysis
All remaining above-2x queries are fundamentally limited by:
1. **Hash map/set cache misses**: High-cardinality GROUP BY/COUNT(DISTINCT) creates maps that don't fit in L3 cache (~25MB per socket). Every hash probe is a DRAM access (~100ns).
2. **Per-doc DocValues access**: advanceExact() on SortedSetDocValues does binary search per doc. For filtered queries (Q39), this is 722K × 5 columns = 3.6M binary searches.
3. **ClickHouse structural advantage**: Two-level hash table, batch processing (65K rows), SIMD-friendly probing, arena-allocated string keys.

## What Was Tried This Iteration
1. Fused DV scan for collectDistinctValuesRaw (Q04): 10% improvement (1.435s → 1.282s)
2. Doc-range parallelism for single-segment COUNT(DISTINCT): NO improvement (hash map bottleneck)
3. Prefetch-batched hash probing for FlatSingleKeyMap: 12% improvement for Q15 (1.702s → 1.500s)
4. Right-sized hash set pre-allocation: reduced memory pressure, helped Q19-Q22 recovery

## Next Steps
1. To reach 38/43, need to flip 6 more queries from the 11 above 2x
2. All remaining queries need 30-86% reduction — very difficult on r5.4xlarge
3. Possible approaches not yet tried:
   - Two-level hash table (like ClickHouse) — major implementation effort
   - DataFusionBridge JNI for grouped COUNT(DISTINCT) — unused native path exists
   - Columnar batch processing for filtered queries (Q39)
   - Pre-load filtered DocValues into flat arrays
4. Q14 at 2.08x is borderline — may flip with cleaner benchmark run

## Evidence
- Full benchmark: /tmp/full_iter28/r5.4xlarge.json (32/43 within 2x)
- Correctness: 36/43 PASS
- Build: BUILD SUCCESSFUL
- Git: commit cc0175dc0 on wukong branch
