# DQE Execution Path Analysis for Borderline Queries

## Schema Reference
- UserID: long, AdvEngineID: short, SearchPhrase: keyword, ResolutionWidth: short
- WatchID: long, ClientIP: integer, IsRefresh: short

---

## Q04: `SELECT AVG(UserID) FROM hits` — 2.77x (0.307s vs 0.111s)

**Dispatch path:** `FusedScanAggregate.canFuse()` → `execute()` → `tryFlatArrayPath()`

**Analysis:** AVG(UserID) where UserID is `long` (BigintType). `tryFlatArrayPath` eligibility check:
- No DISTINCT ✓
- Not DoubleType ✓ (it's BigintType)
- Not VarcharType ✓
- No deleted docs ✓ (MatchAll)

**It DOES use the flat array path.** The flat path reads UserID column-major via `nextDoc()` sequential iteration, accumulates `colSum[c]` and `colCount[c]`, then maps back to `AvgDirectAccumulator.addLongSumCount()`.

**Bottleneck:** The flat path uses **segment-level parallelism** (`CompletableFuture.supplyAsync` with `numWorkers = availableProcessors / dqe.numLocalShards`). For a single column AVG, the work per doc is minimal (one `nextDoc()` + one `+=`). The overhead is:
1. **ForkJoinPool dispatch** — thread scheduling overhead for a trivially cheap per-doc operation
2. **Segment partitioning** — largest-first greedy assignment may not balance well for skewed segment sizes
3. **AvgDirectAccumulator uses `double doubleSum`** — even for long inputs, it accumulates as `doubleSum += dv.nextValue()` (line 1349-1365). The flat path accumulates as `long colSum`, then calls `addLongSumCount(long sum, long cnt)` which does `doubleSum += sum`. This long→double conversion is fine but the double accumulation in the non-flat path adds FP overhead.

**Optimization opportunity:** For a single-column scalar AVG on a dense long column, the parallel overhead may exceed the benefit. ClickHouse likely uses a tighter single-threaded loop. Consider: skip parallelism when `numCols == 1` and segment count is small.

---

## Q08: `SELECT AdvEngineID, COUNT(*) FROM hits WHERE AdvEngineID <> 0 GROUP BY AdvEngineID` — 2.46x (0.145s vs 0.059s)

**Dispatch path:** `FusedGroupByAggregate.canFuse()` → `executeInternal()` → `executeNumericOnly()` → `executeSingleKeyNumeric()` → `executeSingleKeyNumericFlat()`

**Analysis:** AdvEngineID is `short` (SmallintType) — numeric, not varchar. Single key, COUNT(*) only → flat accumulator path. The filter `AdvEngineID <> 0` means it's NOT MatchAll, so it takes the **filtered path** with `Weight+Scorer`.

**Key detail:** `canUseFlatAccumulators = true` (COUNT(*) only, no DISTINCT). Goes to `executeSingleKeyNumericFlat()` with `bucket=0, numBuckets=1`.

Inside `scanSegmentFlatSingleKey()`, since `!isMatchAll`:
- Uses `Scorer.iterator()` → `DocIdSetIterator` per segment
- For each matching doc: `advanceExact(doc)` on key DV → `findOrInsert(key)` → `accData[off]++`

**Bottleneck:** 
1. **advanceExact per doc** — filtered path uses random-access `advanceExact()` instead of sequential `nextDoc()`. For a selective filter (AdvEngineID <> 0 filters ~99.7% of rows in ClickBench), the Scorer efficiently skips non-matching docs, but each matching doc still requires `advanceExact` on the key column.
2. **No single-key fast path for filtered queries** — the MatchAll path has a special `allCountStar` ultra-fast path using `nextDoc()` on the key column only. The filtered path doesn't have this optimization.
3. **Very few groups (~39 distinct AdvEngineID values)** — hash map overhead is negligible, but the per-doc Scorer iteration + DV access dominates.

**Optimization opportunity:** For highly selective filters with very few groups, consider collecting matching doc IDs into a bitset first, then doing a single sequential pass over the key column's DocValues (similar to the `collect-then-sequential-scan` pattern used in `executeVarcharCountDistinctWithHashSets`).

---

## Q26: `SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY SearchPhrase LIMIT 10` — 2.09x (0.568s vs 0.272s)

**Dispatch path:** `extractSortedScanSpec()` matches → `executeSortedScan()`

**Analysis:** Pattern is `LimitNode → SortNode → FilterNode → TableScanNode`. The sorted scan uses `IndexSearcher.search(query, topN, Sort)` with `SortedSetSortField` for the keyword field. Lucene's TopFieldCollector handles early termination.

**Bottleneck:**
1. **Per-doc `readSingleDocValue` with `advanceExact`** — after TopFieldDocs returns the top 10 global doc IDs, each doc requires `ReaderUtil.subIndex()` to find the segment, then `advanceExact(segmentDocId)` on `SortedSetDocValues` + `lookupOrd()`. For only 10 docs this is trivial.
2. **The real cost is in `engineSearcher.search(luceneQuery, topN, luceneSort)`** — Lucene must evaluate the filter (`SearchPhrase <> ''`) across all segments and maintain a priority queue of top-10 results. The filter compilation via `LuceneQueryCompiler` and the `SortedSetSortField` comparison are the dominant costs.
3. **8 shards × 6 segments each** — each shard independently runs the sorted search. The coordinator merges 8 partial results.

**Optimization opportunity:** The 2.09x gap is relatively small. The main cost is Lucene's internal sorted search, which is already well-optimized. Potential: pre-sort the index on SearchPhrase (it's already in the sort.field list but as 6th priority). Or use a more efficient filter representation for `<> ''` (e.g., `ExistsQuery` + `DocValuesFieldExistsQuery`).

---

## Q30: `SELECT SUM(ResolutionWidth), SUM(ResolutionWidth + 1), ... FROM hits` — 2.75x (0.264s vs 0.096s)

**Dispatch path:** `FusedScanAggregate.canFuse()` returns **false** (child is EvalNode, not TableScanNode). Then `canFuseWithEval()` is checked.

**`canFuseWithEval` check:**
- groupByKeys empty ✓
- Child is EvalNode → TableScanNode ✓
- All aggs are SUM (non-distinct) ✓
- Eval expressions: `ResolutionWidth + 1`, `ResolutionWidth + 2`, etc. → matches `COL_PLUS_CONST` pattern ✓

**It DOES use the fused eval-aggregate path** (`executeWithEval`). This uses the algebraic identity: `SUM(col + k) = SUM(col) + k * COUNT(col)`. Reads ResolutionWidth once, derives all results.

**Inside `executeWithEval`:** For MatchAll with no deletes, uses parallel segment scanning (same `CompletableFuture.supplyAsync` pattern). Each worker reads the single unique column (`ResolutionWidth`) across its assigned segments.

**Bottleneck:**
1. **ResolutionWidth is `short` (SmallintType)** — stored as SortedNumericDocValues. The `nextDoc()` + `nextValue()` loop is the same as Q04.
2. **Parallel overhead** — same issue as Q04. For a single physical column, the parallelism overhead (thread dispatch, segment partitioning, result merging) may not pay off.
3. **Many SUM outputs (~90 expressions in Q30)** — but the algebraic shortcut means only 1 column is read. The derivation `SUM(col) + k * COUNT` is O(numAggs) at the end, negligible.

**Optimization opportunity:** Same as Q04 — for single-column reads, consider skipping parallelism. The 2.75x gap suggests the per-doc loop itself is slower than ClickHouse's vectorized column scan.

---

## Q32: `SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits WHERE SearchPhrase <> '' GROUP BY WatchID, ClientIP` — 2.13x (2.336s vs 1.095s)

**Dispatch path:** `FusedGroupByAggregate.canFuse()` → `executeInternal()` → `executeNumericOnly()` (WatchID=long, ClientIP=integer, both numeric) → `executeTwoKeyNumeric()`

**AVG decomposition:** AVG(ResolutionWidth) is decomposed by the planner into SUM(ResolutionWidth) + COUNT(ResolutionWidth). So the aggs are: COUNT(*), SUM(IsRefresh), SUM(ResolutionWidth), COUNT(ResolutionWidth) — all flat-compatible.

**`canUseFlatAccumulators = true`** → `executeTwoKeyNumericFlat()` with bucket partitioning if needed.

**Filter:** `SearchPhrase <> ''` means NOT MatchAll. Uses `Weight+Scorer` filtered path inside `scanSegmentFlatTwoKey()`.

**Bottleneck:**
1. **Filtered path with `advanceExact` per doc per column** — for each matching doc: read WatchID (advanceExact), read ClientIP (advanceExact), hash2(k0,k1), probe FlatTwoKeyMap, then advanceExact on IsRefresh and ResolutionWidth. That's 4 `advanceExact` calls per doc.
2. **DV deduplication** — the code detects when multiple aggs reference the same column (`hasDvDuplicates`). SUM(ResolutionWidth) and COUNT(ResolutionWidth) share the same DV reader, saving 1 advanceExact per doc. But SUM(IsRefresh) needs its own.
3. **High cardinality** — WatchID × ClientIP produces many unique groups. The FlatTwoKeyMap may need multiple resizes.
4. **Parallel segment scanning** — uses `THREADS_PER_SHARD` workers with per-worker FlatTwoKeyMaps, then merges. The merge step iterates all slots of each worker map.

**Optimization opportunity:** The 2.13x gap is the smallest among these queries. The filtered path's `advanceExact` overhead is inherent. Consider: for the filtered case, collect matching doc IDs first (bitset), then do a single sequential pass reading all columns via `nextDoc()` with doc-ID matching (similar to the varchar count-distinct pattern).

---

## Q33: `SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits GROUP BY WatchID, ClientIP` — 2.79x (12.530s vs 4.493s)

**Dispatch path:** Same as Q32 but **no filter** → MatchAll path.

**Inside `scanSegmentFlatTwoKey` with `isMatchAll=true`:**
- Uses `advanceExact(doc)` for all 4 columns (WatchID, ClientIP, IsRefresh, ResolutionWidth) per doc
- DV deduplication applies (ResolutionWidth read once for both SUM and COUNT)

**Bottleneck — this is the worst ratio (2.79x):**
1. **100M rows, no filter** — every single doc is processed. 4 `advanceExact` calls × 100M = 400M random DV accesses.
2. **No `nextDoc()` sequential path for 2-key MatchAll** — unlike the single-key flat path which has a `nextDoc()` lockstep optimization for dense columns, the 2-key path always uses `advanceExact`. This is because the code doesn't attempt lockstep iteration for 2+ keys.
3. **Massive hash map** — WatchID × ClientIP on 100M rows produces potentially millions of unique groups. The FlatTwoKeyMap resizes multiple times, and bucket partitioning kicks in (`numBuckets = ceil(totalDocs / MAX_CAPACITY)`). With MAX_CAPACITY=8M and ~100M docs, that's ~13 buckets, each requiring a full scan of all docs.
4. **Bucket partitioning = 13 full passes** — each bucket pass scans ALL docs but only inserts groups whose hash falls in that bucket. This means 13 × 100M = 1.3B doc iterations total, with hash computation + bucket check per doc even when skipped.
5. **System.gc() call** — after `executeFusedGroupByAggregateWithTopN` returns >10K rows, an explicit `System.gc()` is triggered, adding ~50-100ms pause.

**This is the #1 optimization target.** The bucket partitioning strategy is O(numBuckets × totalDocs), which is catastrophic for high-cardinality 2-key GROUP BY on large tables.

**Optimization opportunities:**
1. **Add `nextDoc()` lockstep for 2-key MatchAll** — like the single-key path, advance all DV iterators sequentially instead of `advanceExact` per doc. This alone could save ~2x on DV access.
2. **Single-pass partitioned aggregation** — instead of N passes over all docs, do a single pass and route each doc to its bucket's map. Requires concurrent maps or per-thread maps with a single merge.
3. **Increase MAX_CAPACITY** — 8M is conservative. For 100M rows with high-cardinality keys, 16M or 32M would reduce bucket count and eliminate multi-pass overhead.
4. **Remove System.gc()** — let G1GC handle collection naturally. The explicit GC adds latency between queries.

---

## Summary of Optimization Priorities

| Query | Ratio | Primary Bottleneck | Top Optimization |
|-------|-------|--------------------|------------------|
| Q33 | 2.79x | Multi-pass bucket partitioning (13 passes × 100M docs) | Single-pass + nextDoc lockstep for 2-key MatchAll |
| Q04 | 2.77x | Parallel overhead for single-column scalar agg | Skip parallelism for 1-column reads |
| Q30 | 2.75x | Same as Q04 (single physical column after algebraic shortcut) | Skip parallelism for 1-column reads |
| Q08 | 2.46x | advanceExact per doc in filtered path | Bitset-then-sequential-scan for selective filters |
| Q32 | 2.13x | advanceExact per doc × 4 columns (filtered) | Bitset-then-sequential for filtered 2-key |
| Q26 | 2.09x | Lucene sorted search internals | Limited room — already uses TopFieldCollector |
