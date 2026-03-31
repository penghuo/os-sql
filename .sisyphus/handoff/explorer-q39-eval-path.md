# Q39 executeWithEvalKeys Analysis

## Method Location
- **Definition**: `FusedGroupByAggregate.java:7279`
- **Call site**: `FusedGroupByAggregate.java:1430` (dispatched when `hasEvalKey == true`)
- **Caller**: `TransportShardExecuteAction.java:2910` via `executeFusedGroupByAggregateWithTopN`

## topN Computation
- `topN = limitNode.getCount() + limitNode.getOffset()` (line 459 of TransportShardExecuteAction.java)
- For Q39: `topN = 10 + 1000 = 1010`
- The OFFSET does NOT cause materializing all groups. The heap selects top-1010 during output.

## Execution Flow for Q39

### Phase 0: Expression Compilation & Inline CASE WHEN Detection (lines 7291-7530)
The method detects the CASE WHEN pattern:
```
CASE WHEN (SearchEngineID=0 AND AdvEngineID=0) THEN Referer ELSE '' END
```
This is recognized as an **inline CASE WHEN** (line ~7440-7510). The key optimization:
- `inlineEvalKey[k] = true` for the Src column
- `isEvalKey[k] = false` — removed from Block-based eval
- Condition checks reference non-eval GROUP BY keys (SearchEngineID, AdvEngineID)
- Result column = Referer (read from DocValues only when condition is true)

The `URL AS Dst` expression is a simple **ColumnReference** and gets converted to a regular non-eval key (line ~7370). So after optimization:
- **isEvalKey**: all false (no Block-based eval remains)
- **inlineEvalKey**: true for Src column only
- **keyIsVarchar**: true for URL and Referer-derived keys
- **hasRemainingEvalKeys**: false → no Block materialization at all

### Phase 1: Global Ordinal Map Building (lines 7640-7680)
For multi-segment indices, builds `OrdinalMap` for each VARCHAR key:
```java
globalOrdMaps[k] = buildGlobalOrdinalMap(allLeaves, fieldName);  // line 7659
```
- Called for URL and Referer fields
- `buildGlobalOrdinalMap` (line 12789) has a **ConcurrentHashMap cache** (`ORDINAL_MAP_CACHE`)
- First call: `OrdinalMap.build(null, subs, PackedInts.DEFAULT)` — expensive for high-cardinality fields
- Subsequent calls: cache hit (keyed by reader identity + field name)
- **URL and Referer are high-cardinality** → OrdinalMap.build iterates all unique terms across all segments

### Phase 2: Doc Collection via Scorer (lines 7700-7720)
```java
Scorer scorer = cachedWeight.scorer(leafCtx);  // line 7714
DocIdSetIterator disi = scorer.iterator();
// Collects matching docIds into int[] array
```
- Uses Lucene Scorer (not Collector) for filtered queries
- Collects 722K matching doc IDs into a primitive int array
- This is efficient — just iterating the posting list

### Phase 3: Hot Loop — Micro-batch Grouping (lines 7779-8050)
**EVAL_BATCH_SIZE = 256** but since `hasRemainingEvalKeys = false`, no Block building occurs.

The inner loop per doc (lines 7860-7960):
```java
for (int d = 0; d < batchCount; d++) {
    int docId = segDocIds[batchStart + d];
    // For each non-eval VARCHAR key: advanceExact + nextOrd + global ord mapping
    // For each numeric key: advanceExact + nextValue
    // For inline CASE WHEN: check condition, then advanceExact on Referer if true
}
```

**Per-doc cost for Q39 (5 keys):**
1. `TraficSourceID` — numeric: `advanceExact(docId)` + `nextValue()`
2. `SearchEngineID` — numeric: `advanceExact(docId)` + `nextValue()`
3. `AdvEngineID` — numeric: `advanceExact(docId)` + `nextValue()`
4. `Src` (inline CASE WHEN) — check if SearchEngineID==0 && AdvEngineID==0, then `advanceExact` on Referer DocValues + `nextOrd()` + global ord mapping
5. `URL` (varchar) — `advanceExact(docId)` + `nextOrd()` + `curSegGlobalMaps[k].get(ord)`

**Total per doc: 5× advanceExact + hash lookup in flat long map**

### Phase 3b: Flat Long Map (lines 7970-8040)
When `useFlatLongMap = true` (all keys resolve to longs via ordinals):
- Open-addressing hash map with `long[flatCap * numKeys]` keys
- Hash: `h = h * 0x9E3779B97F4A7C15L + probeLongKeys[k]` for each of 5 keys
- Linear probing with 5-key comparison per slot
- Initial capacity 32768, load factor 0.65
- **For 722K docs with potentially ~100K+ unique groups, this resizes multiple times**

### Phase 4: Top-N Heap Selection (lines 8050-8120)
- Min-heap of size 1010 (topN = OFFSET 1000 + LIMIT 10)
- Iterates all `flatSize` groups, maintaining heap
- This is O(flatSize × log(1010)) — fast relative to Phase 3

## Where the 2 Seconds Go — Bottleneck Analysis

### Likely Bottleneck #1: OrdinalMap.build for URL and Referer (cold path)
- `OrdinalMap.build()` merges per-segment ordinal spaces into a global ordinal space
- For URL (millions of unique values) and Referer (millions of unique values), this is O(total_unique_terms × num_segments)
- **On first execution**: could be 500ms-1s per field
- **On subsequent executions**: cached, near-zero cost
- Check: if Q39 is slow only on first run, this is the bottleneck

### Likely Bottleneck #2: advanceExact on SortedSetDocValues for URL/Referer
- `advanceExact(docId)` on SortedSetDocValues does a binary search in the ordinals index
- For 722K docs × 2 VARCHAR fields (URL + Referer) = 1.4M advanceExact calls
- Each advanceExact on high-cardinality SortedSetDocValues is O(log N) where N = unique terms
- **This is likely the dominant cost on warm runs**

### Likely Bottleneck #3: Hash map operations with 5 keys
- 722K hash lookups with 5-key comparison
- Flat long map resizes: 32K → 64K → 128K → 256K (if >100K unique groups)
- Each resize rehashes all existing entries

### Likely Bottleneck #4: Global ordinal mapping per doc
- `curSegGlobalMaps[k].get(ord)` for URL and Referer on every doc
- This is a PackedInts lookup — O(1) but with cache misses for random access patterns

## Diagnostic Instrumentation Points

Add timing around these sections:
1. **Line ~7659**: `buildGlobalOrdinalMap` calls — measure OrdinalMap build time
2. **Line ~7714**: Scorer doc collection — measure filter evaluation time
3. **Line ~7779**: The micro-batch loop — measure total grouping time
4. **Inside the per-doc loop (~7860)**: Count advanceExact calls and measure total DV access time
5. **Line ~8050**: Top-N heap — measure selection time

```java
// Example instrumentation at line 7640:
long t0 = System.nanoTime();
// ... buildGlobalOrdinalMap calls ...
long ordMapTime = System.nanoTime() - t0;
System.out.println("[Q39-DIAG] OrdinalMap build: " + (ordMapTime / 1_000_000) + "ms");

// At line 7700:
long t1 = System.nanoTime();
// ... scorer doc collection ...
long scorerTime = System.nanoTime() - t1;
System.out.println("[Q39-DIAG] Doc collection: " + (scorerTime / 1_000_000) + "ms, docs=" + segDocCount);

// At line 7779:
long t2 = System.nanoTime();
// ... micro-batch loop ...
long groupTime = System.nanoTime() - t2;
System.out.println("[Q39-DIAG] Grouping loop: " + (groupTime / 1_000_000) + "ms, groups=" + flatSize);
```

## Key Observations

1. **OFFSET 1000 is NOT the problem** — topN=1010 just means a slightly larger heap (1010 vs 10). All groups must be materialized regardless because GROUP BY requires full aggregation.

2. **The inline CASE WHEN optimization is working** — no Block materialization for the CASE WHEN expression. It's evaluated inline using already-resolved key values.

3. **URL AS Dst is optimized** — converted from eval to direct DocValues read (ColumnReference optimization at line ~7370).

4. **The flat long map is used** — all 5 keys resolve to longs (3 numeric + 2 VARCHAR ordinals), so the zero-allocation open-addressing path is taken.

5. **ClickHouse advantage**: ClickHouse uses columnar storage with dictionary encoding and vectorized hash aggregation. It processes columns in batches of 65K rows. DQE processes row-by-row with per-doc DocValues seeks.
