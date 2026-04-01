status: WORKING
iteration: 33

## Current State
- Score: 34/43 within 2x (best-of-3-runs); 33/43 in single run (Q14 borderline at 2.04x)
- Correctness: 36/43 PASS (no regression)
- Machine: r5.4xlarge (16 vCPU, 124GB RAM), 32GB heap, 4 shards, 4 segments/shard
- Optimizations committed this iteration:
  1. Run-length encoded scan (scanDocRangeFlatSingleKeyCountStar): transition-batching RLE
     that detects runs of identical keys in sorted data and skips hash probes. Avg run length ~1.8.
  2. GC barrier tuning: 40% threshold with 100ms+200ms sleep (was 200ms+300ms)
  3. Parallel columnar loading for executeSingleKeyNumericFlat (Q15 path)
  4. Parallel columnar loading for executeDerivedSingleKeyNumeric (Q35 path)

## Queries Above 2x (9 remaining, best-of-3-runs)
Q35(2.55x) Q04(3.12x) Q08(3.42x) Q09(4.95x) Q11(5.99x)
Q16(5.87x) Q13(7.22x) Q18(8.22x) Q39(14.45x)

## Key Improvements This Iteration (vs iter32)
- Q14: 2.33x → 1.92x-2.04x (crosses 2x threshold, borderline noise)
- Q15: 2.36x → 1.86x (solidly within 2x)
- Q35: 3.12x → 2.55x (improved but still above 2x)
- Q04: 3.24x → 3.12x (modest improvement)

## Next Steps
1. Q35 needs ~22% more improvement (0.944s → 0.740s) — explore hash table compaction or SIMD
2. Q04 needs ~38% improvement — scalar COUNT(DISTINCT) already well-optimized
3. Q08 needs ~43% improvement — GROUP BY RegionID + COUNT(DISTINCT UserID)
4. Q09 needs ~60% improvement — mixed aggs + COUNT(DISTINCT)
5. Remaining 5 queries (Q11, Q13, Q16, Q18, Q39) need 67-93% reduction — likely requires architectural changes
6. Key bottleneck: hash table cache misses for 17M+ unique keys don't fit in L3 cache (25MB)
7. Q14 needs stabilization — borderline at 2.04x, sometimes crosses 2x

## Evidence
- Full benchmark: /tmp/full_derived/r5.4xlarge.json (33/43 single run, 34/43 best-of-3)
- Isolated Q15: 0.863s (1.66x)
- Isolated Q35: 0.820s (2.22x)
- Correctness: 36/43 PASS
- Build: BUILD SUCCESSFUL
- Git: 5c0dca97c on wukong branch
