# Ralph Loop Logs

Append-only log of each iteration.

## Iteration 1 — 2026-03-24T22:15

### What I Did

1. **Analyzed all 6 COUNT(DISTINCT) queries** (Q04, Q05, Q08, Q09, Q11, Q13):
   - Found that 5 of 6 are already on optimized fast paths (bare scan, 2-key dedup, varchar dedup)
   - Only Q11 (3-key dedup: MobilePhone, MobilePhoneModel, UserID) falls to generic fused path
   - The "COUNT(DISTINCT) Fusion" from the handover is largely already implemented

2. **Parallelized executeSingleKeyNumericFlat** (FusedGroupByAggregate.java):
   - Added `mergeFrom(FlatSingleKeyMap other)` method to FlatSingleKeyMap inner class
   - Extracted `scanSegmentFlatSingleKey()` helper that processes one segment into a FlatSingleKeyMap
   - Refactored `executeSingleKeyNumericFlat()` to partition segments across workers using largest-first greedy assignment
   - Each worker creates its own FlatSingleKeyMap, processes assigned segments, returns local map
   - Main thread merges all worker maps into global map
   - Preserves all existing fast paths (ultra-fast COUNT-only, lockstep nextDoc, advanceExact, filtered)
   - Falls back to sequential when PARALLELISM_MODE="off" or single segment

3. **Extended canFuseWithEval for COUNT/AVG** (FusedScanAggregate.java):
   - Q02 (`SUM(col), SUM(col+1), SUM(col+2), COUNT(*), AVG(col)`) was falling through to generic operator execution because canFuseWithEval only accepted SUM
   - PlanFragmenter decomposes AVG into SUM+COUNT at shard level, so shard plan has SUM and COUNT only
   - Extended canFuseWithEval to accept COUNT and AVG (non-distinct)
   - Extended executeWithEval to derive COUNT(*) from per-column count, AVG from sum/count
   - Extended resolveEvalAggOutputTypes to return DoubleType for AVG
   - Added collection of physical columns from COUNT(col)/AVG(col) aggregate args

### Results
- Both changes compile successfully: `./gradlew :dqe:compileJava` → BUILD SUCCESSFUL
- Cannot benchmark locally (dev desktop, not EC2 instance with 100M dataset)

### Decisions
- **Skipped Step 1 (COUNT(DISTINCT) Fusion)** as a separate implementation because 5/6 queries already have optimized paths. Only Q11 needs work (3-key dedup extension).
- **Prioritized Step 2 (parallelize flat path)** because it's a clean, well-understood change with proven parallel patterns already in the codebase.
- **Added Q02 optimization** because it's a borderline query (2.2x) that only needs ~22ms improvement, and the fix is straightforward (extend existing algebraic identity path).
- **Used segment-level parallelism** (not doc-range) for the flat path because FlatSingleKeyMap is not thread-safe and creating per-worker maps with segment-level partitioning is simpler and avoids contention.
