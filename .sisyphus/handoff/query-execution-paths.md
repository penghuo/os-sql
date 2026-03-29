# DQE Execution Path Analysis for Borderline ClickBench Queries

## Query-by-Query Execution Path Trace

### Q02: `SELECT COUNT(*) FROM hits WHERE AdvEngineID <> 0`
- **Plan**: AggregationNode(groupBy=[], aggs=["COUNT(*)"]) → TableScanNode(dslFilter=AdvEngineID<>0)
- **Path in executePlan()**: `isScalarCountStar()` → `executeScalarCountStar()`
- **Method**: `engineSearcher.count(luceneQuery)` — O(1) Lucene count
- **Bottleneck**: Lucene query compilation + count. Already optimal.

### Q14: `SELECT SearchPhrase, COUNT(DISTINCT UserID) FROM hits WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY u DESC LIMIT 10`
- **Plan decomposition**: PlanFragmenter detects COUNT(DISTINCT) with GROUP BY → SINGLE mode → shard plan becomes GROUP BY (SearchPhrase, UserID) with COUNT(*)
- **Shard plan**: AggregationNode(PARTIAL, groupBy=[SearchPhrase, UserID], aggs=["COUNT(*)"]) → TableScanNode(dslFilter)
- **Path in executePlan()**: Unwrap ProjectNode → hits the 2-key dedup block: `aggDedupNode.getGroupByKeys().size() >= 2 && FusedGroupByAggregate.canFuse()`
- **Dispatch**: VARCHAR key0 (SearchPhrase) + numeric key1 (UserID) → `executeVarcharCountDistinctWithHashSets()`
- **Method**: `TransportShardExecuteAction.executeVarcharCountDistinctWithHashSets()` — ordinal-based dedup with per-group LongOpenHashSet
- **Bottleneck**: Filtered query (SearchPhrase <> '') requires `advanceExact()` per doc on SortedSetDocValues. The collect-then-sequential-scan optimization helps but the fundamental cost is iterating ~51K matching docs per shard with ordinal resolution + HashSet insertion. The cross-segment merge via `mergeOrdSetsIntoMap()` also does string resolution per ordinal.
- **Optimization opportunities**: 
  1. For filtered queries, the bitset+lockstep approach could avoid advanceExact binary search
  2. Global ordinals could eliminate cross-segment string merge

### Q27: `SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY EventTime, SearchPhrase LIMIT 10`
- **Plan**: LimitNode → SortNode → TableScanNode(dslFilter, columns=[SearchPhrase, EventTime])
- **Path in executePlan()**: `extractSortedScanSpec()` matches LimitNode → SortNode → TableScanNode
- **Method**: `executeSortedScan()` — Lucene-native `IndexSearcher.search(query, topN, Sort)` with early termination
- **Bottleneck**: This is already using Lucene's TopFieldCollector with segment-level competition. The cost is the filter evaluation (SearchPhrase <> '') + sort field resolution. Already near-optimal for this pattern.

### Q28: `SELECT REGEXP_REPLACE(Referer, ...) AS k, AVG(length(Referer)) AS l, COUNT(*) AS c, MIN(Referer) FROM hits WHERE Referer <> '' GROUP BY k HAVING COUNT(*) > 100000 ORDER BY l DESC LIMIT 25`
- **Plan**: LimitNode → ProjectNode → SortNode → FilterNode(HAVING) → AggregationNode(groupBy=[k], aggs=[AVG(length(Referer)), COUNT(*), MIN(Referer)]) → EvalNode(k=REGEXP_REPLACE(...)) → TableScanNode(dslFilter)
- **Path in executePlan()**: 
  1. Unwrap ProjectNode → effectivePlan is still LimitNode (not AggregationNode), so scalar/fused paths don't match
  2. `extractAggFromSortedLimit()` matches: LimitNode → [ProjectNode] → SortNode → FilterNode → AggregationNode
  3. `FusedGroupByAggregate.canFuseWithExpressionKey()` returns true (single expression key over VARCHAR Referer)
  4. Enters the HAVING branch: `havingFilter != null`
- **Method**: `executeFusedExprGroupByAggregate()` → `FusedGroupByAggregate.executeWithExpressionKey()` — ordinal-cached expression evaluation
- **Key optimization**: REGEXP_REPLACE evaluated ~16K times (once per ordinal) instead of ~921K times (once per doc)
- **Then**: applyHavingFilter → SortOperator → projection
- **Bottleneck**: 
  1. The Collector-based scan iterates all matching docs (~921K for Referer <> '')
  2. Per-doc: advanceExact on SortedSetDocValues + ordinal lookup + accumulation into HashMap<String, AccumulatorGroup>
  3. MIN(Referer) requires per-doc string comparison via lookupOrd
  4. AVG(length(Referer)) uses pre-cached ordinal→length but still needs per-doc ordinal resolution
- **Optimization opportunities**: Pre-load numeric columns for AVG args; use global ordinals for multi-segment

### Q29: `SELECT SUM(ResolutionWidth), SUM(ResolutionWidth+1), ..., SUM(ResolutionWidth+89) FROM hits`
- **Plan**: AggregationNode(groupBy=[], aggs=[SUM(expr0)..SUM(expr89)]) → EvalNode(expr0=ResolutionWidth, expr1=ResolutionWidth+1, ...) → TableScanNode
- **Path in executePlan()**: `FusedScanAggregate.canFuseWithEval()` returns true (all SUM, all col+const patterns)
- **Method**: `executeFusedEvalAggregate()` → `FusedScanAggregate.executeWithEval()`
- **Key optimization**: Algebraic shortcut: SUM(col+k) = SUM(col) + k*COUNT. Reads ResolutionWidth column ONCE, derives all 90 results.
- **Implementation**: Column-major iteration with parallel segment scanning via ForkJoinPool
- **Bottleneck**: Single column DocValues read across all docs (~100M). The parallel segment path distributes this across THREADS_PER_SHARD workers. The actual computation is trivial (one addition per doc).
- **Optimization opportunities**: Already well-optimized. The bottleneck is pure DocValues I/O throughput. Could potentially benefit from SIMD-like batch reading if Lucene supported it.

### Q30: `SELECT SearchEngineID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits WHERE SearchPhrase <> '' GROUP BY SearchEngineID, ClientIP ORDER BY c DESC LIMIT 10`
- **Plan**: LimitNode → ProjectNode → SortNode → AggregationNode(PARTIAL, groupBy=[SearchEngineID, ClientIP], aggs=[COUNT(*), SUM(IsRefresh), AVG(ResolutionWidth)]) → TableScanNode(dslFilter)
- **Path in executePlan()**: 
  1. `extractAggFromSortedLimit()` matches: LimitNode → [ProjectNode] → SortNode → AggregationNode
  2. `FusedGroupByAggregate.canFuse()` returns true (2 numeric keys, supported aggs)
  3. Sort key is COUNT(*) at index 0 (after group keys), which is >= numGroupByCols
  4. Enters `executeFusedGroupByAggregateWithTopN()` with sortAggIndex=0, sortAsc=false, topN=10
- **Method**: `FusedGroupByAggregate.executeWithTopN()` → `executeInternal()` → `executeTwoKeyNumeric()` → `executeTwoKeyNumericFlat()`
- **Key optimization**: FlatTwoKeyMap with contiguous long[] accumulators, top-N heap selection
- **AVG decomposition**: AVG(ResolutionWidth) is decomposed into SUM(ResolutionWidth) + COUNT(ResolutionWidth) at the shard level, with 2 flat accumulator slots
- **Bottleneck**: 
  1. Filtered query (SearchPhrase <> '') — ~51K matching docs per shard
  2. Per-doc: advanceExact on 2 key DVs + 3 aggregate DVs (but DV dedup reduces to 2 unique DV reads since SUM and COUNT share ResolutionWidth)
  3. Hash probing in FlatTwoKeyMap per doc
  4. The forward-only DV advance optimization is used for filtered queries (avoids binary search)
- **Optimization opportunities**: 
  1. The DV dedup logic (`hasDvDuplicates`) correctly detects SUM(ResolutionWidth) + COUNT(ResolutionWidth) sharing the same column
  2. Could benefit from bitset+lockstep for the filtered path if selectivity is high

## Summary Table

| Query | Execution Method | Plan Pattern | Bottleneck |
|-------|-----------------|--------------|------------|
| Q02 | `executeScalarCountStar()` → `searcher.count()` | Scalar COUNT(*) + filter | Lucene count (optimal) |
| Q14 | `executeVarcharCountDistinctWithHashSets()` | VARCHAR+numeric dedup GROUP BY | Filtered DV iteration + ordinal→string merge |
| Q27 | `executeSortedScan()` → Lucene TopFieldCollector | Sorted scan + LIMIT | Filter eval + sort (near-optimal) |
| Q28 | `executeFusedExprGroupByAggregate()` + HAVING | Expression key GROUP BY | Collector scan + per-doc ordinal + MIN(Referer) string compare |
| Q29 | `executeFusedEvalAggregate()` → algebraic shortcut | Scalar SUM(col+k) | Single column DV I/O (optimal) |
| Q30 | `executeTwoKeyNumericFlat()` + top-N | 2-key numeric GROUP BY + sort | Filtered DV iteration + hash probing |

## Key Optimization Opportunities for Borderline Queries

1. **Q14 (2.23x)**: The `executeVarcharCountDistinctWithHashSets` path does collect-then-sequential-scan for filtered queries. The cross-segment `mergeOrdSetsIntoMap` does per-ordinal string resolution. Could use global ordinals to avoid string merge entirely.

2. **Q29 (2.22x)**: Already uses the algebraic shortcut (reads column once). Bottleneck is pure DocValues I/O. The parallel segment path is active. Little room for code-level optimization — this is fundamentally limited by Lucene DocValues read throughput vs ClickHouse columnar.

3. **Q30 (2.58x)**: Uses `executeTwoKeyNumericFlat` with DV dedup and forward-only advance. The filtered path (~51K docs) is relatively efficient. The main cost is 5 DV reads per doc (2 keys + 3 aggs, reduced to 4 by dedup). Could benefit from columnar cache for the filtered path if selectivity is high enough.

4. **Q28 (not in borderline list but Q29-adjacent)**: The expression GROUP BY path with HAVING is well-optimized via ordinal caching. MIN(Referer) requires per-doc string comparison which is expensive.

5. **Q27 (2.13x)**: Already uses Lucene-native sorted scan with early termination. Near-optimal for this pattern.
