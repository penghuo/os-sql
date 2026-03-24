status: WORKING
iteration: 1

## Current State
Completed 3 optimizations targeting 5+ queries. All compile successfully.

### Optimizations Implemented

1. **Parallelized executeSingleKeyNumericFlat** (FusedGroupByAggregate.java)
   - Target: Q15 (4.1x → expected <2x with ~4x speedup from parallelism)
   - Added `mergeFrom()` to FlatSingleKeyMap for parallel map merging
   - Added `scanSegmentFlatSingleKey()` helper for per-segment processing
   - Segment-level parallelism with CompletableFuture + PARALLEL_POOL
   - Largest-first greedy segment partitioning for balanced load

2. **Extended canFuseWithEval for COUNT/AVG** (FusedScanAggregate.java)
   - Target: Q02 (2.2x → expected <2x, needs ~22ms improvement)
   - Accepts COUNT(*) and AVG alongside SUM in the algebraic identity path
   - Derives COUNT(*) from per-column count, AVG from sum/count
   - Ensures physical columns from aggregate args are scanned

3. **Parallelized multi-bucket hash-partitioned aggregation** (FusedGroupByAggregate.java)
   - Target: Q16 (4.1x), Q18 (5.9x), Q32 (3.2x)
   - Runs hash-partition buckets in parallel via CompletableFuture + PARALLEL_POOL
   - Falls back to sequential when parallelism disabled

### Analysis Summary
- COUNT(DISTINCT) queries (Q04,Q05,Q08,Q09,Q11,Q13): 5/6 already on fast paths, only Q11 needs 3-key dedup extension
- Q31 (2.0x): Within JIT variance of 2x, may already pass with good benchmark run
- Q28 (2.7x): REGEXP_REPLACE bottleneck, needs regex caching (not yet implemented)

## Next Steps
1. Deploy to EC2 m5.8xlarge and run full benchmark to verify improvements
2. Implement 3-key dedup fast-path for Q11
3. Add segment-level parallelism within executeTwoKeyNumericFlat
4. Q28 REGEXP_REPLACE regex caching
5. Remaining borderline queries (Q30, Q37)

## Evidence
- Build: `./gradlew :dqe:compileJava` → BUILD SUCCESSFUL
- Files modified:
  - `dqe/src/main/java/org/opensearch/sql/dqe/shard/source/FusedGroupByAggregate.java` (+216/-288 lines)
  - `dqe/src/main/java/org/opensearch/sql/dqe/shard/source/FusedScanAggregate.java` (+83 lines)
- Git diff: 385 insertions, 288 deletions across 2 DQE source files
