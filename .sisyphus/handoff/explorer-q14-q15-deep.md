# Q14 & Q15 Deep-Dive: FusedGroupByAggregate.java Optimization Analysis

## Q15: GROUP BY UserID ORDER BY COUNT(*) LIMIT 10

### Execution Path
1. **Entry**: `executeSingleKeyNumericFlat()` at line 5145
2. **Signature** (line 5145-5163): Takes `shard, query, keyInfos, specs, columnTypeMap, groupByKeys, numAggs, isCountStar, accType, sortAggIndex, sortAscending, topN, bucket, numBuckets`
3. **Parallelization**: YES — two modes:
   - **Doc-range parallel** (line 5199-5250): Only for `allCountStar && isMatchAll && numBuckets <= 1`. Uses `ConcurrentFlatSingleKeyMap` with `AtomicLongArray` (lock-free CAS). Splits docs across `THREADS_PER_SHARD` workers.
   - **Segment-parallel** (line 5253-5325): For filtered or non-COUNT(*) queries. Each worker gets own `FlatSingleKeyMap`, merged after.
   - **Sequential** (line 5327-5338): Fallback when `leaves.size() <= 1` or parallelism off.

4. **Top-N**: Done AFTER full aggregation (line 5347-5403)
   - Iterates ALL `flatMap.capacity` slots (up to 4M pre-sized) checking for non-empty keys
   - Uses min-heap of size N=10 for top-N selection
   - **This is post-aggregation, not during scan**

### Q15 Optimization Opportunities

**OPT-Q15-1: Top-N iterates sparse hash map capacity, not size** (line 5352)
```java
for (int slot = 0; slot < flatMap.capacity; slot++) {  // iterates 4M slots
    if (flatMap.keys[slot] == FlatSingleKeyMap.EMPTY_KEY) continue;
```
With `initialCapacity=4_000_000` (line 5294) but only ~25K unique UserIDs, this scans 4M slots to find 25K entries. Fix: maintain a dense `int[] usedSlots` array during insertion, iterate only populated slots.

**OPT-Q15-2: ConcurrentFlatSingleKeyMap uses AtomicLongArray — high CAS contention** (line 13626-13666)
- `findOrInsert` uses CAS on `keys` array (line 13656)
- `accData.addAndGet` for every doc (line 14349)
- For ~25K groups with millions of docs, hot keys cause heavy contention
- Fix: Use thread-local maps + merge (like segment-parallel path) even for doc-range parallelism

**OPT-Q15-3: toFlatSingleKeyMap copies entire capacity** (line 13672-13682)
```java
for (int i = 0; i < capacity; i++) result.keys[i] = keys.get(i);
for (int i = 0; i < capacity * slotsPerGroup; i++) result.accData[i] = accData.get(i);
```
Copies up to 32M AtomicLong reads. Fix: Only copy non-empty slots.

**OPT-Q15-4: FlatSingleKeyMap pre-sized to 4M for ~25K groups** (line 5294)
```java
int initCap = (int) Math.min(myDocCount * 10 / 7 + 1, 4_000_000);
```
Wastes memory and makes top-N scan slow. Could estimate cardinality from PointValues or use smaller initial size with resize.

---

## Q14: Filtered 2-key COUNT(*) with ordinal indexing

### Execution Path
1. **Entry**: Two-key GROUP BY path, called from line 8276
2. **Guard**: Only for `singleSegment && keyInfos.size() == 2` (line 8270)
3. **Method**: `tryOrdinalIndexedTwoKeyCountStar()` at line 9311
4. **Signature** (line 9311-9321): Takes `engineSearcher, query, leafCtx, keyInfos, numAggs, isCountStar, groupByKeys, sortAggIndex, sortAscending, topN`
5. **NOT parallelized** — single-segment only, no thread pool usage

### Key Data Structure
- `KEYS_PER_ORD = 64` (line 9364): Fixed-size array per varchar ordinal
- `numericKeys[numOrds * 64]` + `counts[numOrds * 64]` + `numKeysPerOrd[numOrds]`
- Ordinal limit: `valueCount <= 500_000` (line 9360)
- Memory limit: `numOrds * KEYS_PER_ORD * 16 <= 32_000_000` (line 9388)

### Filter Path (Q14 uses filtered query)
- **Bitset path** (line 9461-9500): Used when `estCount < maxDoc / 2` (selective filter)
  - Builds `FixedBitSet matchingDocs` from scorer
  - Then iterates `matchingDocs.nextSetBit()` sequentially
  - Already has bitset optimization ✓
- **Collector path** (line 9503-9545): Used for broad filters (>50% docs)
  - Uses Lucene Collector with virtual dispatch per doc

### Inner Loop Bottleneck (line 9484-9497)
```java
int n = numKeysPerOrd[(int) ord];
int idx = -1;
for (int j = 0; j < n; j++) {           // LINEAR SCAN up to 64
    if (numericKeys[base + j] == nk) {
        idx = j; break;
    }
}
```
This is O(KEYS_PER_ORD) = O(64) per document. For Q14 with millions of filtered docs, this linear scan is the hot loop.

### Q14 Optimization Opportunities

**OPT-Q14-1: Linear scan per ordinal is O(64) per doc** (lines 9407, 9434, 9484, 9525)
The inner loop scans up to 64 entries per ordinal to find the numeric key. With millions of docs, this is significant.
Fix options:
- Use a small hash set per ordinal (open-addressing with 64 slots → O(1) amortized)
- Sort numericKeys per ordinal and use binary search (O(6) vs O(32) avg)
- If numeric key range is small (checked at line 9376-9385), use direct-indexed array

**OPT-Q14-2: Single-segment only — no parallelism** (line 8270)
```java
if (singleSegment && keyInfos.size() == 2) {
```
This path is only taken for single-segment indices. Multi-segment indices fall through to slower generic path. No intra-segment parallelism either.
Fix: Split the single segment's doc range across threads (like doc-range parallel in Q15 path).

**OPT-Q14-3: Bitset construction is a separate pass** (line 9465-9472)
```java
FixedBitSet matchingDocs = new FixedBitSet(maxDoc);
Scorer scorer = weight.scorer(leafCtx);
// ... iterate scorer to build bitset
// ... then iterate bitset to aggregate
```
Two passes: (1) build bitset from scorer, (2) iterate bitset for aggregation.
Fix: Fuse filter evaluation with aggregation — iterate scorer directly and aggregate in one pass (skip bitset for forward-only DocValues since scorer already produces sorted doc IDs).

**OPT-Q14-4: advanceExact called twice per doc** (line 9483-9485)
```java
if (!varcharDv.advanceExact(doc)) continue;
long ord = varcharDv.nextOrd();
if (!numericDv.advanceExact(doc)) continue;
```
Two `advanceExact` calls per doc. If the data is dense (most docs have values), the `advanceExact` overhead is wasted.
Fix: For dense columns, use `advance()` + check, or pre-load into columnar arrays like the Q15 path does with `loadNumericColumn()`.

**OPT-Q14-5: No columnar cache for filtered path**
The Q15 path uses `loadNumericColumn()` (line 14358) to pre-load key values into a flat `long[]` for sequential access. The Q14 ordinal path doesn't do this — it uses DocValues `advanceExact()` per doc.
Fix: Pre-load both varchar ordinals and numeric values into arrays, then iterate the bitset against arrays.

---

## No System.gc() or Thread.sleep() Found
Searched entire file — zero matches for `System.gc()` or `Thread.sleep()`.

---

## Summary: Priority-Ranked Optimizations

| Priority | ID | Query | Est. Impact | Description |
|----------|-----|-------|-------------|-------------|
| 1 | OPT-Q14-3 | Q14 | ~5-8% | Fuse filter+aggregation into single pass (eliminate bitset) |
| 2 | OPT-Q14-1 | Q14 | ~5-10% | Replace linear scan with hash lookup per ordinal |
| 3 | OPT-Q14-5 | Q14 | ~3-5% | Pre-load columns into arrays for filtered path |
| 4 | OPT-Q15-1 | Q15 | ~2-5% | Dense slot tracking for top-N scan |
| 5 | OPT-Q15-2 | Q15 | ~5-10% | Thread-local maps instead of CAS contention |
| 6 | OPT-Q14-2 | Q14 | ~10-15% | Add intra-segment parallelism for single-segment |
| 7 | OPT-Q15-4 | Q15 | ~1-2% | Better initial capacity estimation |
| 8 | OPT-Q14-4 | Q14 | ~2-3% | Eliminate advanceExact for dense columns |
