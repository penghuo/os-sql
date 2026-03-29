# ClickBench Query Execution Path & Bottleneck Analysis

## Q02: `SELECT SUM(AdvEngineID), COUNT(*), AVG(ResolutionWidth) FROM hits`
**Type:** Scalar aggregation (no GROUP BY), MatchAll query

### Execution Path
1. **TransportShardExecuteAction.executePlan()** L186-192: `FusedScanAggregate.canFuse(aggNode)` returns true (empty groupByKeys, child is TableScanNode, all aggs match AGG_FUNCTION pattern)
2. **executeFusedScanAggregate()** L905-920 → **FusedScanAggregate.execute()**
3. Inside `execute()`: MatchAll → `tryFlatArrayPath()` (L390-540)
   - Eligibility: no DISTINCT, no double, no varchar → passes
   - Column-major iteration: reads each unique column (AdvEngineID, ResolutionWidth) once per segment via `nextDoc()` sequential scan
   - Parallel across segments via `ForkJoinPool` (THREADS_PER_SHARD workers)
   - Each worker accumulates `colSum[]`, `colCount[]` into packed `long[]`
   - Results merged into SumDirectAccumulator, CountStarDirectAccumulator, AvgDirectAccumulator

### Bottleneck
- **DocValues sequential read** is the dominant cost. Two columns × all docs.
- Parallelized across segments, so CPU-bound on DocValues decompression.
- **Well-optimized**: column-major nextDoc() avoids advanceExact() binary search overhead.

---

## Q29: `SELECT ... FROM hits WHERE ... (filtered scalar aggregation)`
**Type:** Filtered scalar aggregation (no GROUP BY), non-MatchAll query

### Execution Path
1. **TransportShardExecuteAction.compileOrCacheLuceneQuery()** L832-845: `dslFilter != null` → compiles DSL to Lucene Query (cached in LUCENE_QUERY_CACHE)
2. **FusedScanAggregate.canFuse()** returns true → **executeFusedScanAggregate()** → **FusedScanAggregate.execute()**
3. Inside `execute()`: query is NOT MatchAllDocsQuery
   - If all aggs are COUNT(*): uses `engineSearcher.count(query)` — O(1) via Lucene PointValues optimization (L460-470)
   - Otherwise: general Collector path (L475-500) — `engineSearcher.search(query, Collector)` with per-doc `acc.accumulate(doc)` using `advanceExact()`

### Bottleneck
- **Lucene query evaluation** (Scorer/DocIdSetIterator) + **advanceExact() per doc per column**
- For COUNT(*)-only filtered: nearly free via `IndexSearcher.count()` optimization
- For mixed aggs: advanceExact() is random-access (binary search in DocValues), ~2-3x slower than sequential nextDoc()
- **Optimization opportunity**: Could use collect-then-sequential-scan pattern (collect matching doc IDs into sorted array, then iterate DocValues sequentially) — already done in executeVarcharCountDistinctWithHashSets but not in the scalar agg path.

---

## Q14: `GROUP BY SearchEngineID, SearchPhrase WHERE SearchPhrase <> ''`
**Type:** Filtered 2-key GROUP BY (numeric + VARCHAR), non-MatchAll query

### Execution Path
1. **compileOrCacheLuceneQuery()**: `SearchPhrase <> ''` compiled to Lucene Query (NOT MatchAll)
2. **TransportShardExecuteAction.executePlan()** L358-375: `canFuseWithExpressionKey` check → false (no EvalNode expression key)
3. Falls through to L378-383: `FusedGroupByAggregate.canFuse()` → true (SearchEngineID is numeric, SearchPhrase is VARCHAR)
4. **executeFusedGroupByAggregate()** → **FusedGroupByAggregate.execute()** → **executeInternal()**
5. `hasVarchar=true`, `keyInfos.size()==2` → **executeWithVarcharKeys()** → single-segment 2-key path
6. Since filtered (not MatchAll): uses **bitset pre-collection** path if selectivity < 50%, otherwise Collector path
7. Uses **FlatTwoKeyMap** with ordinals for VARCHAR key (single-segment: ordinals are final)

### Bottleneck
- **Lucene Scorer evaluation** for WHERE filter (iterating matching docs)
- **advanceExact()** per doc for both key columns + aggregate columns (random access pattern)
- **Hash map operations**: FlatTwoKeyMap.findOrInsert() per matching doc
- SearchPhrase has ~16K unique ordinals, SearchEngineID has ~39 values → moderate cardinality
- **Key insight**: Filter reduces doc count significantly (SearchPhrase <> '' excludes empty strings), so the bottleneck shifts from DocValues read to Scorer iteration

---

## Q15: `GROUP BY UserID, COUNT(*) ORDER BY COUNT(*) DESC LIMIT 10`
**Type:** Single numeric key GROUP BY, high cardinality (17M unique UserIDs), with top-N

### Execution Path
1. **TransportShardExecuteAction.executePlan()** L290-340: Detects `LimitNode -> SortNode -> AggregationNode` pattern via `extractAggFromSortedLimit()`
2. `FusedGroupByAggregate.canFuse()` → true (UserID is numeric, COUNT(*) is supported)
3. **executeFusedGroupByAggregateWithTopN()** L1003-1040 → **FusedGroupByAggregate.executeWithTopN()** with sortAggIndex=0, sortAscending=false, topN=10
4. Inside `executeInternal()` → `executeNumericOnly()` → `executeSingleKeyNumeric()`
5. `canUseFlatAccumulators=true` (COUNT(*) only) → **executeSingleKeyNumericFlat()**
6. Estimates numBuckets: `totalDocs / FlatSingleKeyMap.MAX_CAPACITY` (MAX_CAPACITY=16M). For 100M docs → ~7 buckets
7. **Multi-bucket partitioned aggregation**: parallel across buckets via ForkJoinPool
8. Each bucket: `scanSegmentFlatSingleKey()` with hash-partition filter (`hash1(key) % numBuckets == bucket`)
9. Top-N selection via min-heap on flat accData array

### Bottleneck
- **Hash map size**: 17M unique UserIDs → exceeds FlatSingleKeyMap.MAX_CAPACITY (16M), triggers multi-bucket partitioning
- **Multi-pass overhead**: Each bucket pass reads ALL docs but only inserts keys matching its hash partition → ~7 full scans of DocValues
- **Hash map probe cost**: Open-addressing with 16M entries at 70% load factor → ~1.4 probes per lookup average
- **Memory pressure**: 16M entries × (8 bytes key + 8 bytes accData) = ~256MB per bucket map
- **Optimization opportunity**: Single-pass multi-bucket routing (already implemented as `executeSingleKeyNumericFlatMultiBucket`) would eliminate redundant DocValues reads. Check if it's being dispatched.

---

## Q28: `GROUP BY REGEXP_REPLACE(...)` — Expression-based GROUP BY
**Type:** Single expression key GROUP BY over VARCHAR column

### Execution Path
1. **TransportShardExecuteAction.executePlan()** L358-375: **canFuseWithExpressionKey()** check
   - Returns true: single group-by key, child is EvalNode → TableScanNode, key is not a physical column, expression references a single VARCHAR source column
2. **executeFusedExprGroupByAggregate()** L978-992 → **FusedGroupByAggregate.executeWithExpressionKey()**
3. Inside `executeWithExpressionKey()`:
   - Compiles REGEXP_REPLACE expression via ExpressionCompiler
   - Uses **ordinal-cached evaluation**: pre-computes expression for each unique ordinal in SortedSetDocValues
   - ~16K ordinals vs ~921K docs = **~58x reduction** in regex evaluations
   - Per-segment: builds `ordToGroupKey[]` array mapping ordinal → computed string
   - Pre-wires `ordGroups[]` array mapping ordinal → AccumulatorGroup
   - Per-doc: `dv.advanceExact(doc)` → `nextOrd()` → `ordGroups[ord]` → accumulate (no expression eval)

### Bottleneck
- **Expression pre-computation**: REGEXP_REPLACE evaluated ~16K times (once per ordinal) — one-time cost
- **Per-doc cost**: advanceExact() + nextOrd() + array lookup + accumulate — very fast
- **Cross-segment merge**: HashMap<String, AccumulatorGroup> for multi-segment indices
- **Key insight**: The ordinal caching is the critical optimization. Without it, REGEXP_REPLACE would be evaluated per-doc (~921K times), making regex the dominant bottleneck.
- **Optimization opportunity**: For single-segment indices, could use ordinal-indexed AccumulatorGroup[] array instead of HashMap (same as executeSingleVarcharGeneric path).

---

## Filter Dispatch: MatchAll vs WHERE clause

### How filtering works (TransportShardExecuteAction L832-845):
```java
private Query compileOrCacheLuceneQuery(String dslFilter, Map<String, String> fieldTypeMap) {
    if (dslFilter == null) {
        return new MatchAllDocsQuery();  // No WHERE clause
    }
    return LUCENE_QUERY_CACHE.computeIfAbsent(
        dslFilter, filter -> new LuceneQueryCompiler(fieldTypeMap).compile(filter));
}
```

### Impact on execution paths:
- **MatchAll (Q02, Q15)**: Uses `nextDoc()` sequential iteration — reads DocValues stream sequentially, ~2x faster than advanceExact()
- **Filtered (Q14, Q29)**: Uses `advanceExact(doc)` random access — binary search per doc per column, or Scorer-based iteration
- **FusedScanAggregate**: MatchAll → `tryFlatArrayPath()` (parallel column-major); Filtered → Collector with `acc.accumulate(doc)`
- **FusedGroupByAggregate**: MatchAll → direct doc iteration with nextDoc() lockstep; Filtered → Weight+Scorer per segment with early segment skip

### FlatSingleKeyMap.MAX_CAPACITY
- **Value**: 16,000,000 (all Flat*KeyMap variants)
- **Purpose**: Prevents OOM from unbounded hash map growth
- **Trigger**: When estimated totalDocs > MAX_CAPACITY, partitions into multiple buckets
- **Impact on Q15**: 17M unique UserIDs > 16M → multi-bucket partitioning with redundant full scans

---

## Summary Table

| Query | Handler Method | Key Structure | Bottleneck | Optimization |
|-------|---------------|---------------|------------|--------------|
| Q02 | FusedScanAggregate.tryFlatArrayPath() | N/A (scalar) | DocValues sequential read (parallelized) | Well-optimized |
| Q29 | FusedScanAggregate.execute() Collector | N/A (scalar) | Scorer eval + advanceExact() | Could use collect-then-sequential |
| Q14 | FusedGroupByAggregate.executeWithVarcharKeys() → FlatTwoKeyMap | FlatTwoKeyMap (ordinal+numeric) | Scorer + advanceExact() + hash probe | Bitset pre-collection helps |
| Q15 | executeSingleKeyNumericFlat() multi-bucket | FlatSingleKeyMap × N buckets | Multi-pass DocValues reads (7 passes) | Single-pass multi-bucket routing |
| Q28 | executeWithExpressionKey() | HashMap<String, AccumulatorGroup> | Expression pre-computation (one-time) | Ordinal-indexed array for single-seg |
