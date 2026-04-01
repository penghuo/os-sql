status: WORKING
iteration: 34

## Current State
- Score: 34/43 within 2x (best-of-3-runs from iter33 baseline, pending re-benchmark with new optimizations)
- Correctness: 36/43 PASS (no regression from any changes this iteration)
- Machine: r5.4xlarge (16 vCPU, 124GB RAM), 32GB heap, 4 shards
- **LIMITATION**: Only hits_1m (1M) index available on this machine. No 100M hits index for performance benchmarking.
- Optimizations committed this iteration (4 commits):
  1. Top-N pruning for GROUP BY key sort columns (21bf3363c)
  2. Hash-partitioned aggregation for executeDerivedSingleKeyNumeric / Q35 path (b05fc30fe)
  3. Hash-partitioned aggregation for executeSingleKeyNumericFlat / Q15 path (9b8f8b6e7)
  4. Hash-partitioned COUNT(DISTINCT) for collectDistinctValuesRaw / Q04 path (7a341ca50)

## Queries Above 2x (9 remaining, from iter33 baseline — needs re-benchmark on 100M)
Q35(2.55x) Q04(3.12x) Q08(3.42x) Q09(4.95x) Q11(5.99x)
Q16(5.87x) Q13(7.22x) Q18(8.22x) Q39(14.45x)

## Expected Impact of Hash-Partitioned Optimizations
- Q35: Eliminates merge step (was ~2x scan cost). Expected ~30-40% improvement → potentially crosses 2x threshold
- Q15: Same pattern, stabilizes borderline query (was 1.86x-2.04x)
- Q04: Smaller partition sets fit in L3 cache during insertion. Expected ~20-30% improvement
- Q14: May benefit from Q15 path improvement (same execution path)

## Next Steps
1. **Load 100M hits index** to benchmark the hash-partitioned optimizations
2. **Q08/Q09/Q11**: Apply hash-partitioned approach to executeCountDistinctWithHashSets
3. **Q16/Q18**: Hash-partitioned aggregation for multi-key GROUP BY with varchar keys
4. **Q39**: Investigate why 14.45x — complex filtered VARCHAR GROUP BY with CASE WHEN
5. **Q13**: VARCHAR GROUP BY + COUNT(DISTINCT) optimization

## Evidence
- Correctness: 36/43 PASS (same as iter33 baseline)
- Build: BUILD SUCCESSFUL for all 4 commits
- Git: 7a341ca50 on wukong branch
