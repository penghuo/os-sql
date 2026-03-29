# Borderline Query Analysis: Q27, Q14, Q29, Q30, Q02

## 1. Query SQL

### Q02 (3.78x, needs 47% improvement)
```sql
SELECT SUM(AdvEngineID), COUNT(*), AVG(ResolutionWidth) FROM hits;
```
**Type**: Scalar aggregation (no GROUP BY, no WHERE)

### Q14 (2.23x, needs 10% improvement)
```sql
SELECT SearchEngineID, SearchPhrase, COUNT(*) AS c FROM hits
WHERE SearchPhrase <> '' GROUP BY SearchEngineID, SearchPhrase ORDER BY c DESC LIMIT 10;
```
**Type**: 2-key GROUP BY (numeric + VARCHAR) with WHERE filter, ORDER BY + LIMIT

### Q27 (2.13x, needs 6% improvement)
```sql
SELECT CounterID, AVG(length(URL)) AS l, COUNT(*) AS c FROM hits
WHERE URL <> '' GROUP BY CounterID HAVING COUNT(*) > 100000 ORDER BY l DESC LIMIT 25;
```
**Type**: Single numeric key GROUP BY with WHERE filter, length() aggregate, HAVING, ORDER BY + LIMIT

### Q29 (2.22x, needs 10% improvement)
```sql
SELECT SearchEngineID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth)
FROM hits WHERE SearchPhrase <> ''
GROUP BY SearchEngineID, ClientIP ORDER BY c DESC LIMIT 10;
```
**Type**: 2-key numeric GROUP BY with WHERE filter, mixed aggregates, ORDER BY + LIMIT

### Q30 (2.58x, needs 22% improvement)
```sql
SELECT SUM(ResolutionWidth), SUM(ResolutionWidth + 1), ..., SUM(ResolutionWidth + 89) FROM hits;
```
**Type**: Scalar aggregation with 90 SUM expressions (no GROUP BY, no WHERE)

## 2. Execution Path Dispatch in TransportShardExecuteAction

### Q02 → `FusedScanAggregate.execute()`
- Plan: `AggregationNode(PARTIAL, groupBy=[], aggs=[SUM(AdvEngineID), COUNT(*), AVG(ResolutionWidth)]) → TableScanNode`
- Dispatch: `isScalarCountStar()` = false (3 aggs, not just COUNT(*)), then `FusedScanAggregate.canFuse()` = true
- Path: `executeFusedScanAggregate()` → `FusedScanAggregate.execute()`

### Q14 → `executeVarcharCountDistinctWithHashSets()` OR `FusedGroupByAggregate.executeWithTopN()`
- Plan: `LimitNode → ProjectNode → SortNode → AggregationNode(groupBy=[SearchEngineID, SearchPhrase], aggs=[COUNT(*)]) → TableScanNode`
- Dispatch: `extractAggFromSortedLimit()` finds the AggregationNode, `canFuse()` = true
- Since it's 2-key (numeric + VARCHAR) with ORDER BY + LIMIT, goes through `executeFusedGroupByAggregateWithTopN()`
- Inside FusedGroupByAggregate: `executeInternal()` → `executeWithVarcharKeys()` → single-segment 2-key flat path with filtered query (WHERE SearchPhrase <> '')
- **Key detail**: Uses `FlatTwoKeyMap` with ordinals for SearchPhrase, bitset-based filtered scan

### Q27 → HAVING path in `executePlan()`
- Plan: `LimitNode → ProjectNode → SortNode → FilterNode(HAVING) → AggregationNode(groupBy=[CounterID], aggs=[AVG(length(URL)), COUNT(*)]) → TableScanNode`
- Dispatch: `extractAggFromSortedLimit()` finds AggregationNode through FilterNode, `extractFilterFromSortedLimit()` finds the HAVING FilterNode
- Path: HAVING branch → `executeFusedGroupByAggregate()` (no topN pre-filter due to HAVING) → `applyHavingFilter()` → SortOperator
- Inside FusedGroupByAggregate: `executeInternal()` → `executeNumericOnly()` → `executeSingleKeyNumericFlat()` (single numeric key, flat accumulators for AVG(length(URL)) + COUNT(*))
- **Key detail**: `length(URL)` is handled via `applyLength` flag — reads SortedSetDocValues for URL, pre-computes ordinal→length map, then uses O(1) lookup per doc

### Q29 → `executeFusedGroupByAggregateWithTopN()`
- Plan: `LimitNode → ProjectNode → SortNode → AggregationNode(groupBy=[SearchEngineID, ClientIP], aggs=[COUNT(*), SUM(IsRefresh), AVG(ResolutionWidth)]) → TableScanNode`
- Dispatch: `extractAggFromSortedLimit()` finds AggregationNode, sort on COUNT(*) (agg index 0), `canFuse()` = true
- Path: `executeFusedGroupByAggregateWithTopN()` with sortAggIndex=0, sortAscending=false, topN=10
- Inside FusedGroupByAggregate: `executeInternal()` → `executeNumericOnly()` → `executeTwoKeyNumericFlat()` (2 numeric keys, flat accumulators)
- **Key detail**: Filtered query (WHERE SearchPhrase <> ''), uses Weight+Scorer per segment, FlatTwoKeyMap with DV deduplication for AVG (SUM+COUNT share ResolutionWidth DV)

### Q30 → `FusedScanAggregate.executeWithEval()`
- Plan: `AggregationNode(groupBy=[], aggs=[SUM(expr0)..SUM(expr89)]) → EvalNode(expr0=ResolutionWidth+0, ..., expr89=ResolutionWidth+89) → TableScanNode`
- Dispatch: `FusedScanAggregate.canFuseWithEval()` = true
- Path: `executeFusedEvalAggregate()` → `FusedScanAggregate.executeWithEval()`
- **Key detail**: Uses algebraic shortcut: SUM(col + k) = SUM(col) + k * COUNT(*). Reads ResolutionWidth once, derives all 90 results.

## 3. Bottleneck Analysis

### Q02 (3.78x) — BOTTLENECK: Full table scan with 3 DocValues columns
- Scans ALL ~100M docs reading 3 DocValues columns (AdvEngineID, ResolutionWidth for SUM+AVG, plus COUNT(*))
- No filter, no GROUP BY → pure I/O bound on DocValues sequential read
- `FusedScanAggregate.execute()` iterates all segments, reading SortedNumericDocValues for each column per doc
- **Why slow**: 3 advanceExact() calls per doc × 100M docs. No parallelism in FusedScanAggregate for scalar aggs.

### Q14 (2.23x) — BOTTLENECK: Filtered scan + high-cardinality VARCHAR grouping
- WHERE SearchPhrase <> '' filters to ~33% of docs (~33M)
- GROUP BY (SearchEngineID, SearchPhrase) — SearchPhrase has ~18K unique ordinals
- Uses FlatTwoKeyMap with ordinal-based keys in single-segment path
- **Why slow**: Filter evaluation (Scorer iteration) + 2 DocValues reads per matching doc. The filtered path uses advanceExact() which is slower than sequential nextDoc(). Also, the bitset-based filtered scan path may not be triggered if selectivity estimate is wrong.

### Q27 (2.13x) — BOTTLENECK: length(URL) computation + HAVING filter
- WHERE URL <> '' filters to ~67% of docs
- GROUP BY CounterID (single numeric key) with AVG(length(URL)) + COUNT(*)
- length(URL) uses ordinal→length pre-computation (good), but URL has very high cardinality (~18M ordinals)
- **Why slow**: Pre-computing ordinal→length map for ~18M ordinals takes significant time. The HAVING filter prevents topN pre-filtering, so ALL groups must be computed before filtering.

### Q29 (2.22x) — BOTTLENECK: Filtered 2-key numeric GROUP BY with 3 aggregates
- WHERE SearchPhrase <> '' filters to ~33% of docs
- GROUP BY (SearchEngineID, ClientIP) — both numeric, moderate cardinality
- 3 aggregates: COUNT(*), SUM(IsRefresh), AVG(ResolutionWidth) — AVG decomposes to SUM+COUNT sharing DV
- **Why slow**: Filtered scan with 4 DocValues reads per doc (2 keys + IsRefresh + ResolutionWidth). DV deduplication helps (ResolutionWidth read once for SUM+COUNT), but the filter overhead dominates.

### Q30 (2.58x) — BOTTLENECK: 90 SUM expressions on full table scan
- No filter, no GROUP BY — pure scalar aggregation
- 90 SUM(ResolutionWidth + k) expressions
- Uses algebraic shortcut: reads ResolutionWidth once, derives all 90 results
- **Why slow**: Despite the algebraic optimization, still scans ALL ~100M docs. The bottleneck is the single-threaded full scan of ResolutionWidth DocValues. The algebraic shortcut eliminates 89 redundant column reads but the base scan is still O(N).

## 4. Concrete Optimization Ideas

### Q02 (needs 47% improvement)
1. **Intra-shard parallelism for scalar aggregations**: FusedScanAggregate currently runs single-threaded. Split doc ranges across THREADS_PER_SHARD workers, each accumulating partial SUM/COUNT/AVG, then merge. This is the same pattern used in FusedGroupByAggregate's parallel paths.
2. **Columnar cache for scalar aggs**: Pre-load DocValues columns into flat long[] arrays (like `loadNumericColumn()`), then iterate arrays instead of DocValues iterators. Array iteration is ~2x faster than DocValues advanceExact().
3. **Combined DocValues iteration**: Instead of 3 separate advanceExact() calls per doc, use a single nextDoc() lockstep pattern (already used in GROUP BY paths but not in scalar agg path).

### Q14 (needs 10% improvement)
1. **Collect-then-sequential-scan for filtered GROUP BY**: The current filtered path uses advanceExact() per doc. Instead, collect matching doc IDs into a sorted array first, then iterate DocValues sequentially with nextDoc() and match against the sorted array. This converts random I/O to sequential I/O. (Already done for VARCHAR COUNT(DISTINCT) but not for general filtered GROUP BY.)
2. **Bitset-based filter with nextDoc lockstep**: For moderate selectivity (~33%), collect matching docs into FixedBitSet, then iterate all DocValues with nextDoc() checking the bitset. Avoids Scorer overhead per doc.
3. **Global ordinals for multi-segment filtered path**: If the index has multiple segments, building OrdinalMap once and using global ordinals avoids per-segment BytesRefKey allocation during cross-segment merge.

### Q27 (needs 6% improvement)
1. **Lazy ordinal→length map**: Instead of pre-computing length for ALL ~18M ordinals, only compute for ordinals that actually appear in matching docs. Use a FixedBitSet to track seen ordinals during the first pass, then compute lengths only for set bits.
2. **Parallel ordinal→length computation**: The ordinal→length pre-computation loop is sequential. Parallelize it across THREADS_PER_SHARD workers.
3. **TopN-aware HAVING**: If HAVING condition is `COUNT(*) > threshold`, pre-filter groups during aggregation by maintaining a running count and skipping groups that can't possibly exceed the threshold (requires knowing the total doc count).

### Q29 (needs 10% improvement)
1. **Parallel filtered 2-key GROUP BY**: The filtered path in `executeTwoKeyNumericFlat` currently runs sequentially when using advanceExact(). Add segment-parallel scanning for filtered queries (already exists for MatchAll but not for filtered).
2. **Bitset + nextDoc lockstep for filtered path**: Same as Q14 — collect matching docs into FixedBitSet, then use nextDoc() lockstep for all 4 DocValues columns. This is especially beneficial when selectivity is ~33%.
3. **Pre-filter on SearchPhrase before reading other columns**: Since WHERE SearchPhrase <> '' is the filter, read SearchPhrase DocValues first, check the condition, and only read the other 3 columns for matching docs. This saves 3 DocValues reads for ~67% of docs.

### Q30 (needs 22% improvement)
1. **Intra-shard parallelism for eval aggregations**: Same as Q02 — FusedScanAggregate.executeWithEval() runs single-threaded. Split doc ranges across workers, each computing partial SUM + COUNT, then merge. The algebraic shortcut (SUM(col+k) = SUM(col) + k*COUNT) means each worker only needs SUM(ResolutionWidth) and COUNT(*).
2. **Columnar cache**: Pre-load ResolutionWidth into a flat long[] array, then iterate the array in parallel workers. Array access is ~2x faster than DocValues.
3. **SIMD-friendly accumulation**: With the columnar cache, the inner loop becomes `sum += array[i]` which the JVM can auto-vectorize with SIMD instructions.

## 5. Priority Ranking (by impact × effort)

| Query | Gap | Top Optimization | Est. Improvement | Effort |
|-------|-----|-----------------|-----------------|--------|
| Q02 | 47% | Parallel scalar agg + columnar cache | 40-60% | Medium |
| Q30 | 22% | Parallel eval agg + columnar cache | 30-50% | Medium (shares Q02 infra) |
| Q29 | 10% | Parallel filtered 2-key + bitset lockstep | 15-25% | Medium |
| Q14 | 10% | Collect-then-sequential for filtered GROUP BY | 10-20% | Low-Medium |
| Q27 | 6% | Lazy ordinal→length map | 8-15% | Low |
