# Worst ClickBench Queries — Dispatch Path Analysis & Optimization Opportunities

## Query Summary

| Query | SQL | Ratio |
|-------|-----|-------|
| Q15 | `SELECT UserID, COUNT(*) FROM hits GROUP BY UserID ORDER BY COUNT(*) DESC LIMIT 10` | 32.89x |
| Q39 | `SELECT TraficSourceID, SearchEngineID, AdvEngineID, CASE WHEN ... END AS Src, URL AS Dst, COUNT(*) FROM hits WHERE CounterID=62 AND ... GROUP BY ... ORDER BY PageViews DESC OFFSET 1000 LIMIT 10` | 29.42x |
| Q18 | `SELECT UserID, extract(minute FROM EventTime) AS m, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, extract(minute FROM EventTime), SearchPhrase ORDER BY COUNT(*) DESC LIMIT 10` | 9.78x |
| Q13 | `SELECT SearchPhrase, COUNT(DISTINCT UserID) AS u FROM hits WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY u DESC LIMIT 10` | 7.78x |
| Q16 | `SELECT UserID, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, SearchPhrase ORDER BY COUNT(*) DESC LIMIT 10` | 7.08x |
| Q11 | `SELECT MobilePhone, MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM hits WHERE MobilePhoneModel <> '' GROUP BY MobilePhone, MobilePhoneModel ORDER BY u DESC LIMIT 10` | 6.88x |

---

## Per-Query Dispatch Path Analysis

### Q15 (32.89x) — GROUP BY UserID ORDER BY COUNT(*) DESC LIMIT 10

**Dispatch path:**
1. `executePlan` → unwraps ProjectNode → detects `LimitNode -> SortNode -> AggregationNode`
2. `extractAggFromSortedLimit` matches → `sortIndices.size()==1` and sort is on aggregate column
3. → `executeFusedGroupByAggregateWithTopN(innerAgg, req, sortAggIndex=0, sortAsc=false, topN=10)`
4. → `FusedGroupByAggregate.executeWithTopN` → `executeInternal`
5. → `executeNumericOnly` (UserID is numeric, no varchar keys)
6. → `executeSingleKeyNumeric` (single key, no expressions)
7. → `executeSingleKeyNumericFlat` (COUNT(*) is flat-compatible)
8. Inside flat path: parallel doc-range scan (all COUNT(*), matchAll, single bucket)
9. TopN: min-heap selection on `flatMap.accData` (heap of size 10 over ~4.4M groups)

**Key files:**
- `TransportShardExecuteAction.java:505-515` — TopN dispatch
- `FusedGroupByAggregate.java:5177-5482` — `executeSingleKeyNumericFlat`
- `FusedGroupByAggregate.java:5370-5450` — TopN heap selection

**Why it's slow (32.89x):**
- UserID has ~4.4M unique values → FlatSingleKeyMap with open-addressing hash map
- Every doc does a hash probe + accumulate into `accData[]`
- Parallel doc-range scan helps but hash map random access is cache-unfriendly for 4.4M entries
- TopN heap (size 10) over 4.4M slots is fast, but the aggregation itself is the bottleneck

**Optimization opportunities:**
1. **Segment-level TopN pruning**: Currently aggregates ALL groups across ALL segments, then selects top-10. Could maintain a running top-10 threshold during aggregation — if a group's count can't possibly exceed the current 10th-best, skip it. Hard for COUNT(*) since every doc increments.
2. **Two-pass approach**: First pass: approximate top-N candidates using sampling or per-segment top-N. Second pass: exact counts only for candidates. This is how ClickHouse handles high-cardinality GROUP BY + LIMIT.
3. **Partitioned parallel aggregation with early merge**: Instead of doc-range parallelism into shared FlatSingleKeyMap, use per-worker maps and merge only top-N from each worker. Avoids contention and reduces merge work.
4. **Columnar batch processing**: `loadNumericColumn` already loads into `long[]`. Could process in SIMD-friendly batches (e.g., 1024 docs at a time) with prefetching hints for hash map slots.
5. **Count-min sketch pre-filter**: Use a count-min sketch to identify likely top-N candidates, then do exact counting only for those. Trades accuracy for speed.

---

### Q39 (29.42x) — Filtered GROUP BY with CASE WHEN + OFFSET

**Dispatch path:**
1. `executePlan` → unwraps ProjectNode → detects `LimitNode -> SortNode -> AggregationNode`
2. AggregationNode has EvalNode child (CASE WHEN expression) → `canFuseWithExpressionKey` check
3. → `executeFusedGroupByAggregateWithTopN` or `executeFusedGroupByAggregate` depending on sort
4. → `executeInternal` → detects eval keys → `executeWithEvalKeys`
5. Inside `executeWithEvalKeys`: compiles CASE WHEN expression via `ExpressionCompiler`
6. Per-doc: evaluates CASE WHEN expression + reads 5 key columns (TraficSourceID, SearchEngineID, AdvEngineID, Src, Dst)
7. Uses `MergedGroupKey` HashMap (not flat path) due to eval keys + varchar keys

**Key files:**
- `TransportShardExecuteAction.java:440-451` — TopN dispatch with eval key detection
- `FusedGroupByAggregate.java:7202-8183` — `executeWithEvalKeys`
- `FusedGroupByAggregate.java:528-950` — `executeWithExpressionKeyImpl`

**Why it's slow (29.42x):**
- 5 GROUP BY keys (3 numeric + 2 varchar including CASE WHEN result)
- CASE WHEN expression evaluated per-document via Block-based expression evaluation
- Uses `MergedGroupKey` with Object[] arrays → heavy allocation + GC pressure
- HashMap<MergedGroupKey, AccumulatorGroup> → per-group object allocation
- CounterID=62 filter reduces docs but still scans all segments
- OFFSET 1000 means topN = 1010, still needs full aggregation

**Optimization opportunities:**
1. **Predicate pushdown to Lucene**: CounterID=62 AND EventDate range AND IsRefresh=0 should produce a very selective Lucene query. Verify this is being pushed down as a Lucene BooleanQuery rather than post-filtering.
2. **CASE WHEN specialization**: The CASE WHEN is `CASE WHEN (SearchEngineID=0 AND AdvEngineID=0) THEN Referer ELSE '' END`. This could be specialized: pre-compute a boolean flag per doc and branch on it, avoiding full Block-based expression evaluation.
3. **Flat N-key map for mixed types**: Currently uses `MergedGroupKey` HashMap. Could use a flat map with composite key encoding: pack 3 numeric keys into a single 192-bit key, use ordinals for varchar keys.
4. **URL column optimization**: URL (Dst) is a high-cardinality varchar key. Global ordinal map for URL could be very large. Consider using a hash of the URL as the key during aggregation, resolving only for top-N output.
5. **Filtered scan optimization**: Since CounterID=62 is very selective (~0.1% of rows), could use a PointRangeQuery or TermQuery to get matching doc IDs first, then iterate only those docs for DocValues.

---

### Q18 (9.78x) — GROUP BY UserID, extract(minute), SearchPhrase ORDER BY COUNT(*) LIMIT 10

**Dispatch path:**
1. `executePlan` → detects `LimitNode -> SortNode -> AggregationNode`
2. Sort is on aggregate column → `executeFusedGroupByAggregateWithTopN`
3. → `executeInternal` → has varchar key (SearchPhrase) → varchar path
4. → `executeWithVarcharKeys` → 3 keys (UserID numeric, minute=extract numeric, SearchPhrase varchar)
5. Multi-segment: falls through to `executeNKeyVarcharPath`
6. 3-key flat path check: has varchar key → `singleSegment || !anyVarcharKey` fails for multi-seg
7. Falls to `executeNKeyVarcharParallelDocRange` (parallel doc-range with MergedGroupKey maps)

**Key files:**
- `TransportShardExecuteAction.java:505-515` — TopN dispatch
- `FusedGroupByAggregate.java:10342-11523` — `executeNKeyVarcharPath`
- `FusedGroupByAggregate.java:11703-12027` — `executeNKeyVarcharParallelDocRange`

**Why it's slow (9.78x):**
- 3 keys with varchar → can't use FlatThreeKeyMap for multi-segment (ordinals are segment-local)
- Falls to `executeNKeyVarcharParallelDocRange` which uses `MergedGroupKey` HashMap
- `MergedGroupKey` stores Object[] with resolved Strings → heavy allocation
- extract(minute) computed per-doc
- Very high cardinality: UserID × minute × SearchPhrase → millions of groups

**Optimization opportunities:**
1. **Global ordinal map for 3-key flat path**: Currently `FlatThreeKeyMap` only works multi-segment when all keys are numeric. Could build a global ordinal map for SearchPhrase and use global ordinals as the third key in FlatThreeKeyMap. This would enable the flat path for multi-segment.
2. **extract(minute) pre-computation**: minute values are 0-59. Could encode as a 6-bit value packed with UserID into a single long key, reducing from 3 keys to effectively 2 keys (packed_UserID_minute + SearchPhrase_ordinal).
3. **Early termination with topN**: `executeNKeyVarcharParallelDocRange` already has `earlyTerminate` flag for LIMIT without ORDER BY. But Q18 has ORDER BY, so it can't early-terminate. Could use approximate top-N: maintain per-worker heaps and merge.
4. **Segment-local aggregation with deferred resolution**: Aggregate using segment-local ordinals per segment, select per-segment top-N, then resolve only those ordinals to strings for cross-segment merge.

---

### Q13 (7.78x) — GROUP BY SearchPhrase + COUNT(DISTINCT UserID)

**Dispatch path:**
1. `executePlan` → detects `LimitNode -> SortNode -> AggregationNode`
2. AggregationNode.Step == PARTIAL with 2 GROUP BY keys (SearchPhrase, UserID) — this is the COUNT(DISTINCT) decomposition
3. Matches the COUNT(DISTINCT) dedup path: `aggDedupNode.getGroupByKeys().size() >= 2`
4. 2-key path: key0=SearchPhrase (VARCHAR), key1=UserID (numeric)
5. → `executeVarcharCountDistinctWithHashSets` (VARCHAR key0 + numeric key1 pattern)
6. Uses global ordinals for SearchPhrase, per-ordinal `LongOpenHashSet` for UserID dedup

**Key files:**
- `TransportShardExecuteAction.java:2392-2800` — `executeVarcharCountDistinctWithHashSets`
- `TransportShardExecuteAction.java:330-340` — VARCHAR + numeric key dispatch

**Why it's slow (7.78x):**
- Per-group `LongOpenHashSet` for UserID dedup → many small hash sets allocated
- SearchPhrase has ~400K unique values → 400K LongOpenHashSet instances
- Each LongOpenHashSet grows dynamically → allocation + rehashing overhead
- Global ordinal map construction for SearchPhrase across segments

**Optimization opportunities:**
1. **HyperLogLog for approximate COUNT(DISTINCT)**: If approximate results are acceptable, use HLL instead of exact LongOpenHashSet. Reduces memory from O(distinct_values) to O(1) per group.
2. **Bitmap-based dedup**: UserID values are dense integers. Could use a RoaringBitmap per group instead of LongOpenHashSet — much more memory-efficient for dense value ranges.
3. **Two-level aggregation**: First pass: per-segment, collect (SearchPhrase_ordinal, UserID) pairs into a flat sorted array. Second pass: sort and count distinct by scanning the sorted array. Avoids per-group hash set allocation entirely.
4. **Ordinal-indexed array of sets**: Instead of HashMap<ordinal, LongOpenHashSet>, use a flat array indexed by global ordinal. Already partially done but could be optimized further with pre-sized sets based on estimated cardinality.
5. **Fused top-N with COUNT(DISTINCT)**: Currently aggregates all groups then selects top-N. Could maintain a running top-N and skip groups whose max possible count can't exceed the threshold.

---

### Q16 (7.08x) — GROUP BY UserID, SearchPhrase ORDER BY COUNT(*) LIMIT 10

**Dispatch path:**
1. `executePlan` → detects `LimitNode -> SortNode -> AggregationNode`
2. Sort on aggregate → `executeFusedGroupByAggregateWithTopN`
3. → `executeInternal` → has varchar key (SearchPhrase) → varchar path
4. → `executeWithVarcharKeys` → 2 keys (UserID numeric, SearchPhrase varchar)
5. Single-segment: tries `tryOrdinalIndexedTwoKeyCountStar` first
6. If single-segment + flat-compatible: uses `FlatTwoKeyMap` with ordinals
7. Multi-segment: → `executeMultiSegGlobalOrdFlatTwoKey` (global ordinals + FlatTwoKeyMap)

**Key files:**
- `FusedGroupByAggregate.java:8193-9289` — `executeWithVarcharKeys`
- `FusedGroupByAggregate.java:12038-12556` — `executeMultiSegGlobalOrdFlatTwoKey`
- `FusedGroupByAggregate.java:9303-9663` — `tryOrdinalIndexedTwoKeyCountStar`

**Why it's slow (7.08x):**
- UserID × SearchPhrase → very high cardinality (~millions of groups)
- FlatTwoKeyMap with open-addressing: cache-unfriendly for millions of entries
- Global ordinal map construction overhead for SearchPhrase
- TopN heap selection over millions of groups after full aggregation

**Optimization opportunities:**
1. **Per-segment top-N with merge**: Instead of aggregating all groups globally, maintain per-segment top-N heaps. For ORDER BY COUNT(*) DESC LIMIT 10, each segment contributes its local top-10. Coordinator merges segment-level top-Ns. This dramatically reduces the number of groups that need cross-segment resolution.
2. **Approximate pre-filtering**: Use count-min sketch to identify candidate groups, then exact-count only candidates.
3. **FlatTwoKeyMap capacity optimization**: For very high cardinality, the flat map's load factor causes many empty slots. Could use a more compact representation (e.g., Robin Hood hashing or Swiss table).
4. **Lazy ordinal resolution**: Currently resolves all varchar ordinals to strings. With top-N, only need to resolve the top-10 groups' ordinals.

---

### Q11 (6.88x) — GROUP BY MobilePhone, MobilePhoneModel + COUNT(DISTINCT UserID)

**Dispatch path:**
1. `executePlan` → detects COUNT(DISTINCT) decomposition
2. 3 GROUP BY keys: MobilePhone (VARCHAR), MobilePhoneModel (VARCHAR), UserID (numeric)
3. `aggDedupNode.getGroupByKeys().size() >= 2` matches
4. 3-key path: not all numeric → checks mixed-type path
5. Last key (UserID) is numeric → `executeMixedTypeCountDistinctWithHashSets`
6. Uses `ObjectArrayKey` (Object[] composite key) → HashMap<ObjectArrayKey, LongOpenHashSet>

**Key files:**
- `TransportShardExecuteAction.java:1631-2390` — `executeMixedTypeCountDistinctWithHashSets`
- `TransportShardExecuteAction.java:350-370` — mixed-type dispatch

**Why it's slow (6.88x):**
- `ObjectArrayKey` wraps Object[] → boxing, allocation, poor cache locality
- Per-group `LongOpenHashSet` for UserID dedup
- Two VARCHAR keys (MobilePhone, MobilePhoneModel) → string resolution per segment
- HashMap with Object keys → expensive equals/hashCode on every probe

**Optimization opportunities:**
1. **Ordinal-pair indexed flat array**: For single-segment, use (ord0, ord1) as a composite key into a flat array, avoiding Object[] allocation entirely. For multi-segment, build global ordinal maps for both varchar keys.
2. **Packed ordinal key**: Encode two ordinals into a single long (e.g., ord0 << 32 | ord1) and use FlatSingleKeyMap with LongOpenHashSet values. Eliminates ObjectArrayKey entirely.
3. **Pre-filter with MobilePhoneModel <> ''**: The WHERE clause filters empty MobilePhoneModel. This should be pushed to Lucene as an ExistsQuery or TermRangeQuery to reduce scanned docs.
4. **Bitmap dedup for UserID**: Same as Q13 — RoaringBitmap instead of LongOpenHashSet.

---

## Cross-Cutting Optimization Opportunities

### 1. Per-Segment Top-N with Deferred Resolution (Q15, Q16, Q18)
All three queries do `ORDER BY COUNT(*) DESC LIMIT 10`. Currently: aggregate all groups → heap-select top-10. Better: maintain per-segment top-N during aggregation, merge only top-N candidates across segments. For Q15 with 4.4M groups, this could reduce output from 4.4M to 10×num_segments.

### 2. Approximate Aggregation Mode
For queries with LIMIT 10 on high-cardinality GROUP BY, exact results require scanning all data. An approximate mode (count-min sketch + sampling) could provide 10-100x speedup with bounded error.

### 3. Flat Map for Mixed VARCHAR+Numeric Keys (Q11, Q39)
Currently falls back to HashMap<ObjectArrayKey/MergedGroupKey, ...> when varchar keys are present in multi-key GROUP BY. Could use global ordinals to convert varchar keys to longs, enabling flat map paths.

### 4. COUNT(DISTINCT) with Bitmaps (Q11, Q13)
Replace per-group `LongOpenHashSet` with `RoaringBitmap` for dense numeric dedup keys. RoaringBitmap is ~10x more memory-efficient for dense ranges and supports fast union operations.

### 5. CASE WHEN Expression Specialization (Q39)
The generic Block-based expression evaluation for CASE WHEN is expensive per-doc. Could generate specialized bytecode or use a simple branch instead of the full expression evaluation pipeline.

### 6. Predicate Selectivity-Aware Path Selection (Q39)
Q39's `CounterID=62` filter is extremely selective. Could detect high selectivity and switch to a "gather" strategy: collect matching doc IDs first (via Lucene), then batch-read DocValues only for those docs, avoiding full-column scans.
