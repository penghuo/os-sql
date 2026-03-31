# Borderline Query Execution Path Analysis

## Q02: `SELECT SUM(AdvEngineID), COUNT(*), AVG(ResolutionWidth) FROM hits`
- **Dispatch**: `FusedScanAggregate.execute()` via `executeFusedScanAggregate()` (line ~617 TransportShardExecuteAction)
- **Parallelized**: YES — `tryFlatArrayPath()` at FusedScanAggregate.java:766. Uses `CompletableFuture` + `FusedGroupByAggregate.getParallelPool()`. Segments partitioned across `numWorkers = availableProcessors / dqe.numLocalShards` (16/4=4 workers).
- **AVG decomposition**: AVG is decomposed into SUM+COUNT at the accumulator level. `tryFlatArrayPath` reads each unique column once via column-major `nextDoc()` iteration. No per-doc overhead from AVG — it's just `colSum[c] += val` in the inner loop, same as SUM.
- **Bottleneck**: Already well-optimized. 3 columns read sequentially across 4 parallel workers. The 3.71x gap is likely dominated by ClickHouse's vectorized columnar engine advantage on simple scans. No easy quick win here.

## Q03: `SELECT AVG(UserID) FROM hits`
- **Dispatch**: Same as Q02 — `FusedScanAggregate.execute()` → `tryFlatArrayPath()`
- **Parallelized**: YES — same parallel segment scanning as Q02.
- **AVG handling**: Single column (UserID). Reads SortedNumericDocValues via `nextDoc()`, accumulates sum+count in `long[]`. Final AVG = sum/count computed once.
- **Bottleneck**: Single column scan of ~100M docs. Already parallelized across segments. The 2.29x gap is pure scan throughput — ClickHouse reads columnar Parquet-like format vs Lucene DocValues. No quick win.

## Q29: 90 `SUM(ResolutionWidth + N)` expressions
- **Dispatch**: `executeFusedEvalAggregate()` → `FusedScanAggregate.executeWithEval()` (line ~155 FusedScanAggregate.java)
- **Parallelized**: YES — at line ~227, uses same `CompletableFuture` + `getParallelPool()` pattern. Workers partition segments, each accumulates sum/count per unique physical column.
- **Eval overhead**: ZERO per-doc eval. Uses algebraic identity: `SUM(col + k) = SUM(col) + k * COUNT(*)`. All 90 expressions reference the same physical column `ResolutionWidth`. The column is read ONCE, then 90 results derived from `sum + offset * count`. See `COL_PLUS_CONST` pattern at line ~71.
- **Bottleneck**: Single column scan (same as Q03) + trivial post-computation for 90 results. The 2.58x gap is scan throughput. No quick win — the algebraic shortcut already eliminates all eval overhead.

## Q28: `REGEXP_REPLACE` GROUP BY
- **Dispatch**: `executeFusedExprGroupByAggregate()` → `FusedGroupByAggregate.executeWithExpressionKey()` (line ~393)
- **Parallelized**: YES — `executeWithExpressionKeyImpl()` at line ~528 checks `canParallelize` (line ~551). Uses `CompletableFuture` + `PARALLEL_POOL` for multi-segment parallel path (line ~853+).
- **Regex compiled once**: YES. Pattern is compiled once per batch in `StringFunctions.regexpReplace()` (line ~412 StringFunctions.java). Uses cached `java.util.regex.Pattern` + reused `Matcher`. For anchored patterns with simple group reference (Q28's URL extraction), uses ultra-fast `matcher.group()` path instead of `replaceAll()`.
- **Ordinal caching**: Expression is evaluated once per unique ordinal (~16K) not per doc (~921K). See `canFuseWithExpressionKey()` at line ~314 FusedGroupByAggregate.java. Pre-computes `ordToGroupKey[]` array, then maps docs to groups via ordinal lookup.
- **Bottleneck**: The 2.41x gap comes from: (1) regex evaluation on ~16K ordinals (even cached, regex is expensive), (2) HashMap<String, AccumulatorGroup> lookups per ordinal during scan. **Potential optimization**: Pre-wire ordinal→group mapping (already done in sequential path via `ordGroups[]` array), but verify the parallel path also uses ordinal pre-wiring.

## Q14: `SELECT SearchEngineID, SearchPhrase, COUNT(*) ... WHERE SearchPhrase <> '' GROUP BY ... ORDER BY c DESC LIMIT 10`
- **Dispatch**: Goes through `extractAggFromSortedLimit()` → detects `LimitNode -> SortNode -> AggregationNode` pattern → calls `executeFusedGroupByAggregateWithTopN()` → `FusedGroupByAggregate.executeWithTopN()` → `executeInternal()` with topN params.
- **Key types**: SearchEngineID (numeric) + SearchPhrase (varchar) → routes to `executeWithVarcharKeys()` (line ~8218).
- **Parallelized**: PARTIALLY. The varchar path uses global ordinal maps for multi-segment support (line ~7537). However, the main scan loop at line ~7575 is sequential per-segment (`for (LeafReaderContext leafCtx : ...)`). It does NOT use CompletableFuture parallelism for the scan phase — it collects doc IDs per segment sequentially.
- **Filter handling**: `WHERE SearchPhrase <> ''` is compiled to a Lucene query. Matching docs collected into `int[] segDocIds` per segment, then DocValues read for matching docs only.
- **Cardinality**: SearchEngineID × SearchPhrase could be very high (SearchPhrase has ~16K+ unique values after filtering). The top-N optimization helps — `sortAggIndex >= numGroupByCols` triggers `executeFusedGroupByAggregateWithTopN` which limits output.
- **Bottleneck**: (1) Sequential segment scanning in varchar path — NOT parallelized across segments. (2) Per-doc `advanceExact()` for filtered docs on varchar DocValues. (3) HashMap-based grouping for varchar keys. **Potential optimization**: Add parallel segment scanning to `executeWithVarcharKeys` for filtered queries (the infrastructure exists in other paths).

## Summary of Optimization Opportunities

| Query | Ratio | Parallelized? | Quick Win? | Recommendation |
|-------|-------|--------------|------------|----------------|
| Q02 | 3.71x | YES (segments) | NO | Already optimal — scan throughput gap |
| Q03 | 2.29x | YES (segments) | NO | Already optimal — single column scan |
| Q29 | 2.58x | YES (segments) | NO | Already uses algebraic shortcut |
| Q28 | 2.41x | YES (segments) | MAYBE | Verify parallel path uses ordinal pre-wiring |
| Q14 | 2.36x | PARTIAL | YES | Add parallel segment scanning to executeWithVarcharKeys |

### Top Recommendation
**Q14** is the best candidate for improvement. The `executeWithVarcharKeys` path processes segments sequentially even though the parallel infrastructure (ForkJoinPool, segment partitioning) exists. Adding parallel segment scanning for the filtered 2-key varchar GROUP BY path could reduce wall time by ~2-3x on the shard level, potentially bringing Q14 within 2x.
