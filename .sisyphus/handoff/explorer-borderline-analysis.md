# Borderline Query Execution Path Analysis

## Dispatch Flow Summary

All queries enter `executePlan()` (TransportShardExecuteAction.java:191). The method unwraps any top-level `ProjectNode`, then tries fast paths in order. Filters (WHERE clauses) are pushed down to `TableScanNode.dslFilter` and compiled to Lucene queries via `compileOrCacheLuceneQuery()`.

---

## Q03: `SELECT SUM(AdvEngineID), COUNT(*), AVG(ResolutionWidth) FROM hits` — 2.55x

**Execution Path:** `FusedScanAggregate.canFuse()` → `executeFusedScanAggregate()` (line 238-241)

**Fast Path Hit:** YES — `FusedScanAggregate.execute()` (line 472). Since no WHERE clause, query is `MatchAllDocsQuery`.

**Specific Path:** `tryFlatArrayPath()` (line 621/766). This is the ultra-fast column-major path:
- Reads each unique column once via `nextDoc()` sequential iteration
- Accumulates into flat `long[]` arrays (colSum, colCount)
- Parallelizes across segments via ForkJoinPool

**Bottleneck:** This is already on the best scalar agg path. The 2.55x ratio suggests the overhead is inherent to reading 2 distinct columns (AdvEngineID for SUM, ResolutionWidth for AVG) plus COUNT(*). The `tryFlatArrayPath` does column-major iteration which is optimal.

**Potential Optimization:**
- `tryFlatArrayPath` iterates columns sequentially (one column at a time across all docs). For Q03 with only 2 columns, a **row-major fused loop** reading both columns per doc in a single pass could reduce segment iteration overhead (open DV iterators once, iterate once). Currently it opens DV iterators per-column per-segment.
- The parallel path partitions segments across workers. If the index has few segments, parallelism is limited. Consider **intra-segment parallelism** (doc-range splitting within a segment).

---

## Q31: `SELECT SearchEngineID, ClientIP, COUNT(*), SUM(IsRefresh), AVG(ResolutionWidth) FROM hits WHERE SearchPhrase <> '' GROUP BY SearchEngineID, ClientIP ORDER BY c DESC LIMIT 10` — 2.64x

**Execution Path:** Plan is `LimitNode -> SortNode -> AggregationNode -> TableScanNode`. The `extractAggFromSortedLimit()` extracts the inner `AggregationNode`. `FusedGroupByAggregate.canFuse()` succeeds (line 373).

**Key Types:** SearchEngineID=numeric, ClientIP=VARCHAR → `hasVarchar=true` → routes to `executeWithVarcharKeys()` (line 6927).

**Specific Path:** Single-segment → flat two-key path (line 7042). `canUseFlatVarchar=true` because SUM(long) and AVG(long) are supported. Uses `FlatTwoKeyMap` with contiguous `long[]` accData.

**Sort Optimization:** Sort is by COUNT(*) DESC (single aggregate column) → `sortAggIndex >= numGroupByCols` → uses `executeFusedGroupByAggregateWithTopN()` (line 469). This does inline top-N selection on the flat accData, avoiding full ordinal resolution.

**Bottleneck:** The WHERE clause (`SearchPhrase <> ''`) means query is NOT `MatchAllDocsQuery`. This forces the **Collector-based path** (line 7407) instead of the direct doc iteration path. The Collector path has per-doc virtual dispatch overhead (`collect()` method call per matching doc).

**Potential Optimization:**
- For filtered queries, consider a **collect-then-iterate** approach: first collect matching doc IDs into a bitset, then iterate the bitset with the same tight loop used for MatchAllDocsQuery. This eliminates Collector/LeafCollector virtual dispatch overhead.
- The `SearchPhrase <> ''` filter likely matches a large fraction of docs. A **bitset-based iteration** would allow the same sequential DV access pattern as the MatchAll path.

---

## Q32: `SELECT WatchID, ClientIP, COUNT(*), SUM(IsRefresh), AVG(ResolutionWidth) FROM hits WHERE SearchPhrase <> '' GROUP BY WatchID, ClientIP ORDER BY c DESC LIMIT 10` — 2.23x

**Execution Path:** Same as Q31 — `extractAggFromSortedLimit()` → `FusedGroupByAggregate.canFuse()` → `executeWithVarcharKeys()`.

**Key Types:** WatchID=numeric (BIGINT), ClientIP=VARCHAR → same `hasVarchar=true` path.

**Specific Path:** Same flat two-key path as Q31. Same top-N optimization applies.

**Key Difference from Q31:** WatchID is high-cardinality (unique per hit) vs SearchEngineID (low-cardinality). This means the `FlatTwoKeyMap` hash table grows much larger (~millions of entries vs thousands). The top-N pre-filter helps but the hash table still needs to accommodate all unique (WatchID, ClientIP) pairs before top-N selection.

**Bottleneck:** Same Collector-based path issue as Q31, plus high-cardinality hash table pressure.

**Potential Optimization:**
- Since `ORDER BY c DESC LIMIT 10` with high-cardinality keys means most groups have count=1, a **streaming top-N with early termination** could help: maintain a min-heap of size 10, and once the heap minimum exceeds 1, skip any new groups (they can't beat existing top-10).
- The `findOrInsertCapped` already caps group count when `sortAggIndex < 0`, but here `sortAggIndex >= 0` so it doesn't cap. Consider a **probabilistic early-stop**: after processing N docs, if the top-10 counts are all > remaining_docs/current_groups, new groups can't compete.

---

## Q38: `SELECT Title, COUNT(*) FROM hits WHERE CounterID=62 AND EventDate >= ... AND ... AND Title <> '' GROUP BY Title ORDER BY PageViews DESC LIMIT 10` — 2.98x

**Execution Path:** Plan is `LimitNode -> SortNode -> AggregationNode -> TableScanNode`. `extractAggFromSortedLimit()` extracts inner agg. `FusedGroupByAggregate.canFuse()` succeeds.

**Key Types:** Title=VARCHAR (single key) → `hasVarchar=true`, `keyInfos.size()==1` → routes to `executeSingleVarcharCountStar()` (line 1262) since it's single VARCHAR key with COUNT(*) only.

**Specific Path:** `executeSingleVarcharCountStar` is the ultra-fast ordinal-array path. For single-segment, uses `long[]` indexed by ordinal — zero HashMap overhead.

**Sort Optimization:** Sort by COUNT(*) DESC → top-N selection directly on ordinal count array (line 1269).

**Bottleneck:** The WHERE clause has multiple filters (`CounterID=62 AND EventDate >= ... AND DontCountHits=0 AND IsRefresh=0 AND Title <> ''`). This is a **heavily filtered** query — likely only a small fraction of 100M rows match. The Lucene query evaluation + Collector dispatch dominates.

**Potential Optimization:**
- For heavily filtered queries with small result sets, the Lucene query evaluation is the bottleneck, not the aggregation. Consider **pre-collecting doc IDs** into a `FixedBitSet`, then iterating the bitset with the fast ordinal-array path. This separates filter evaluation from aggregation.
- The `executeSingleVarcharCountStar` method already has a MatchAllDocsQuery fast path with `nextDoc()` iteration. Adding a **bitset iteration path** for filtered queries would give similar performance: iterate set bits, do `advanceExact(doc)` on the SortedSetDocValues.
- Since Title is VARCHAR and the filter is selective, the ordinal space is large but only a small subset of ordinals will be touched. A **sparse ordinal map** (HashMap<Long, Long> instead of long[ordinalCount]) could reduce memory for very selective filters.

---

## Q41: `SELECT URLHash, EventDate, COUNT(*) FROM hits WHERE CounterID=62 AND EventDate >= ... AND IsRefresh=0 AND TraficSourceID IN (-1,6) AND RefererHash=... GROUP BY URLHash, EventDate ORDER BY PageViews DESC OFFSET 100 LIMIT 10` — 3.28x

**Execution Path:** Same sorted-limit pattern. `extractAggFromSortedLimit()` → `FusedGroupByAggregate.canFuse()`.

**Key Types:** URLHash=numeric (BIGINT), EventDate=numeric (DATE) → `hasVarchar=false` → routes to `executeNumericOnly()` (line 2811).

**Specific Path:** `executeNumericOnly` handles all-numeric keys. This is a different code path from the varchar paths above.

**Sort Optimization:** Sort by COUNT(*) DESC → top-N with `sortAggIndex >= numGroupByCols`.

**Bottleneck:** 
1. **Heavily filtered** — same Collector overhead as Q38.
2. **OFFSET 100 LIMIT 10** — requires collecting top 110 groups, not just 10. The `topN = count + offset = 110`.
3. **`executeNumericOnly` path** — need to verify this path has the same flat-array optimization as the varchar path. Two numeric keys use a composite long key or similar hash map.
4. **ORDER BY PageViews** — but `PageViews` is not in the SELECT list as an alias for COUNT(*). This may cause a sort column resolution issue (the alias `c` or `PageViews` must map to the aggregate output column index).

**Potential Optimization:**
- Verify `executeNumericOnly` has equivalent flat-array optimization for 2-key numeric GROUP BY. If it falls back to `HashMap<CompositeKey, AccumulatorGroup>`, that's significantly slower than `FlatTwoKeyMap`.
- Same bitset-based pre-collection optimization as Q38.
- The OFFSET 100 means we need 110 groups minimum. For high-cardinality URLHash, this is trivially met, so the overhead is in hash table management, not in finding enough groups.

---

## Cross-Query Pattern: Filtered Queries Miss the Fast Iteration Path

**The #1 common bottleneck across Q31, Q32, Q38, Q41:** All have WHERE clauses, which means the query is not `MatchAllDocsQuery`. This forces the Collector-based code path in every fused method, which has:
- Per-doc virtual dispatch through `LeafCollector.collect()`
- Lucene's BulkScorer framework overhead
- No column-major iteration (must read all columns per doc in the collect callback)

**The MatchAllDocsQuery paths** (used by queries within 2x) get:
- Direct `for (doc = 0; doc < maxDoc; doc++)` loops
- Column-major iteration in `tryFlatArrayPath`
- `nextDoc()` sequential DV access instead of `advanceExact(doc)`

**Key Recommendation:** Implement a **bitset pre-collection strategy** for filtered fused paths:
1. Run `engineSearcher.search(query, bitsetCollector)` to collect matching doc IDs into a `FixedBitSet`
2. Iterate the bitset with the same tight loop structure as the MatchAll path
3. This converts the filtered path from Collector-based to iteration-based, eliminating virtual dispatch

This pattern already exists in the codebase — see line 1482 comment: "collect-then-sequential-scan for WHERE-filtered queries". Extending this to all fused GROUP BY paths would benefit Q31, Q32, Q38, Q41.

---

## Summary Table

| Query | Path | Fast Path? | Key Bottleneck | Est. Improvement |
|-------|------|-----------|----------------|-----------------|
| Q03 | FusedScanAggregate.tryFlatArrayPath | YES (best) | Column-major overhead for 2 cols | ~10-15% (row-major fusion) |
| Q31 | FusedGroupByAggregate.executeWithVarcharKeys (Collector) | PARTIAL | Collector dispatch for filtered query | ~20-30% (bitset pre-collect) |
| Q32 | Same as Q31 | PARTIAL | Same + high-cardinality hash table | ~15-25% (bitset + streaming top-N) |
| Q38 | FusedGroupByAggregate.executeSingleVarcharCountStar (Collector) | PARTIAL | Collector dispatch for heavily filtered | ~25-35% (bitset pre-collect) |
| Q41 | FusedGroupByAggregate.executeNumericOnly (Collector) | PARTIAL | Collector dispatch + verify flat optimization | ~20-30% (bitset + verify numeric path) |

## Key File Locations

- Dispatch logic: `TransportShardExecuteAction.java:191-620`
- Scalar agg fast path: `FusedScanAggregate.java:621` (tryFlatArrayPath)
- Scalar agg filtered: `FusedScanAggregate.java:676` (Collector path)
- GROUP BY varchar dispatch: `FusedGroupByAggregate.java:1045-1115`
- Two-key varchar flat path: `FusedGroupByAggregate.java:7042`
- Two-key varchar Collector: `FusedGroupByAggregate.java:7407`
- Single varchar COUNT(*): `FusedGroupByAggregate.java:1262`
- Numeric-only GROUP BY: `FusedGroupByAggregate.java:2811`
- Existing bitset pre-collect pattern: `TransportShardExecuteAction.java:1482`
