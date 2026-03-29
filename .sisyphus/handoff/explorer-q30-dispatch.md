# Q30/Q31/Q32 Execution Path Through DQE Engine

## 1. Dispatch Path in TransportShardExecuteAction.executePlan()

**Pattern matched**: `LimitNode -> [ProjectNode] -> SortNode -> AggregationNode -> TableScanNode`

**Method**: `extractAggFromSortedLimit()` at line 2438 extracts the AggregationNode.

**Condition check** (line 397): `FusedGroupByAggregate.canFuse(innerAgg, colTypeMap)` — returns true for these queries.

**Sort analysis** (lines 418-493):
- `sortIndices.size() == 1` and `sortIndices.get(0) >= numGroupByCols` → the sort key is `c` (COUNT(*)), which is an aggregate column (index 2, past the 2 group-by keys).
- This triggers the **single-sort-key top-N fast path** at line 487-495:
  ```
  sortAggIndex = sortIndices.get(0) - numGroupByCols  // = 0 (first agg = COUNT(*))
  executeFusedGroupByAggregateWithTopN(innerAgg, req, sortAggIndex=0, sortAsc=false, topN=10)
  ```
- The pages come back **already top-N filtered** — no SortOperator needed.

**For Q30/Q31** (with WHERE): `SearchPhrase <> ''` is compiled to a Lucene query via `compileOrCacheLuceneQuery()` (line 836). The DSL filter is compiled by `LuceneQueryCompiler` and cached.

**For Q32** (no WHERE): `dslFilter == null` → `MatchAllDocsQuery`.

## 2. FusedGroupByAggregate Dispatch Chain

```
executeWithTopN() [line 907]
  → executeInternal() [line 938]
    → classifies keys: SearchEngineID/WatchID = numeric, ClientIP = numeric → hasVarchar=false
    → executeNumericOnly() [line 2812]
      → keyInfos.size() == 2 && !anyTrunc → executeTwoKeyNumeric() [line 5071]
        → canUseFlatAccumulators = true (COUNT*=accType0, SUM_long=accType1, AVG_long=accType2)
        → executeTwoKeyNumericFlat() [line 5435]
```

## 3. Data Structures

### FlatTwoKeyMap (inner class, line 12184)
- **Open-addressing hash map** with linear probing
- `long[] keys0` — first group-by key (SearchEngineID or WatchID)
- `long[] keys1` — second group-by key (ClientIP)
- `boolean[] occupied` — slot occupancy
- `long[] accData` — contiguous accumulator array, slot i's data at `[i*slotsPerGroup .. (i+1)*slotsPerGroup)`
- Initial capacity: 8192, load factor: 0.7, max capacity: 8M
- Hash function: `TwoKeyHashMap.hash2(key0, key1)` (line 12215)
- **No per-group object allocation** — all data in primitive arrays

### Accumulator Layout in accData (per group)
For these 3 aggregations: COUNT(*), SUM(IsRefresh), AVG(ResolutionWidth):
- `accType[0] = 0` (COUNT*) → 1 slot: count
- `accType[1] = 1` (SUM long) → 1 slot: sum
- `accType[2] = 2` (AVG long) → 2 slots: sum + count
- **Total slotsPerGroup = 4**
- Layout: `[count_star, sum_isrefresh, avg_sum_reswidth, avg_count_reswidth]`

## 4. Per-Group Accumulation (scanSegmentFlatTwoKey, line 5733)

The hot loop per document:
```java
int slot = flatMap.findOrInsert(key0, key1);
int base = slot * slotsPerGroup;
for (int i = 0; i < numAggs; i++) {
    int off = base + accOffset[i];
    if (isCountStar[i]) { flatMap.accData[off]++; continue; }
    // advanceExact or lockstep nextDoc to get value
    switch (accType[i]) {
        case 0: flatMap.accData[off]++; break;           // COUNT
        case 1: flatMap.accData[off] += rawVal; break;    // SUM
        case 2: flatMap.accData[off] += rawVal;           // AVG: sum
               flatMap.accData[off + 1]++; break;         // AVG: count
    }
}
```

**DV deduplication**: IsRefresh and ResolutionWidth are different columns, so `hasDvDuplicates=false` for these queries. Each agg gets its own SortedNumericDocValues reader.

**Dense column optimization**: When all DV columns are dense (no nulls) and MatchAll, uses `nextDoc()` lockstep instead of `advanceExact()` — avoids binary search per doc.

## 5. Shard-Level Top-N for Multi-Key Queries

**YES, there IS a shard-level top-N** in `executeTwoKeyNumericFlat` (line 5611):
```java
if (sortAggIndex >= 0 && topN > 0 && topN < flatMap.size) {
    // Min-heap of size topN=10, keyed on COUNT(*) value
    int sortAccOff = accOffset[sortAggIndex]; // offset of COUNT(*) in accData
    int n = (int) Math.min(topN, flatMap.size);
    int[] heap = new int[n]; // heap of slot indices
    // ... standard min-heap selection for DESC sort
}
```
This scans all occupied slots in the FlatTwoKeyMap and keeps only the top-10 by COUNT(*). Only those 10 rows are materialized into the output Page.

**Critical for Q31/Q32**: With ~100M docs and potentially millions of unique (WatchID, ClientIP) groups, this avoids building BlockBuilder entries for all groups.

## 6. Filter Application (Q30/Q31 with WHERE SearchPhrase <> '')

**NOT a bitset** — the bitset path is **disabled** (line 5935):
```java
boolean useBitsetLockstep = false; // Disabled: causes EOFException on some DocValues
```

**Actual path**: Scorer-based iteration (line 6075):
```java
DocIdSetIterator docIt = scorer.iterator();
while ((doc = docIt.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
    collectFlatTwoKeyDoc(doc, dv0, dv1, flatMap, ...);
}
```
Each matching doc uses `advanceExact(doc)` on the key and agg DocValues readers. This is the **slower path** compared to the MatchAll lockstep — no nextDoc() optimization possible since docs are sparse.

**For Q32 (no filter)**: Uses MatchAll path with dense nextDoc() lockstep (line 5830-5870) — significantly faster.

## 7. Coordinator Merge Path

**Dispatch** in `ResultMerger.mergeAggregationAndSort()` (line 71):
- `canUseFastNumericMerge()` returns true (all keys are numeric, all aggs are standard)
- `numGroupByCols == 2` → falls through to `mergeAggregationFastNumericWithSort()` (line 574)

**Merge data structure**: Open-addressing hash map with `long[][] mapKeys`, `long[][] mapLongAggs`, `double[][] mapDoubleAggs` (line 608-615). Each group has a `long[2]` key array and `long[numAggCols]` agg array.

**AVG merge**: Weighted merge — accumulates `avg * count` as weightedSum across shards, divides by total count at output.

**Top-N selection**: After building the hash map from all shard results, scans all slots with a bounded int heap of size `limit=10` (line 739+). Only 10 rows are materialized into the output Page.

## 8. Partitioned Aggregation (for Q31/Q32 with high cardinality)

When estimated docs > `FlatTwoKeyMap.MAX_CAPACITY` (8M), the key space is **hash-partitioned** into multiple buckets (line 5143-5152):
```java
numBuckets = Math.max(1, (int) Math.ceil((double) totalDocs / FlatTwoKeyMap.MAX_CAPACITY));
```
For 100M docs / 4 shards = ~25M docs/shard → `numBuckets = ceil(25M / 8M) = 4`.

Each bucket runs `executeTwoKeyNumericFlat` with a hash-partition filter:
```java
int docBucket = (TwoKeyHashMap.hash2(key0, key1) & 0x7FFFFFFF) % numBuckets;
if (docBucket != bucket) continue;
```
Buckets are parallelized across `THREADS_PER_SHARD` workers, then merged via `mergePartitionedPages()`.

**This is a major bottleneck for Q31/Q32**: Each bucket re-scans ALL segments, reading ALL DocValues, just to skip ~75% of docs via the hash filter. With 4 buckets × ~30 segments, that's 120 segment scans instead of 30.

## 9. Key Bottlenecks Identified

### Bottleneck 1: Hash-Partitioned Multi-Pass for High Cardinality (Q31/Q32)
- 100M docs → 4 buckets → 4× full DocValues scan per shard
- Each pass reads ALL 5 DocValues columns (2 keys + 3 agg args) even for skipped docs
- **Impact**: ~4× slower than single-pass

### Bottleneck 2: No Dense Lockstep for Filtered Queries (Q30/Q31)
- WHERE filter forces Scorer-based iteration with `advanceExact()` per doc per DV column
- MatchAll path uses `nextDoc()` lockstep which is ~2-3× faster
- Bitset lockstep path is disabled due to EOFException bug

### Bottleneck 3: FlatTwoKeyMap MAX_CAPACITY = 8M
- Q31/Q32 with (WatchID, ClientIP) could have tens of millions of unique groups
- Forces partitioning even when physical memory would suffice
- Each partition's top-N is local — must be re-merged

### Bottleneck 4: Coordinator Merge Uses long[][] (Object Arrays)
- `mergeAggregationFastNumericWithSort` uses `long[][] mapKeys` — each group allocates a `long[2]` array
- For millions of groups from 4 shards, this creates millions of small arrays
- Could use flat parallel arrays like the shard-level FlatTwoKeyMap

## 10. Existing Optimized Paths (Not Applicable Here)

- `executeMultiSegGlobalOrdFlatTwoKey` (line 11060): For (numeric + varchar) key pairs using global ordinals. Not applicable — both keys are numeric.
- `executeSingleKeyNumeric`: Single-key fast path with `FlatSingleKeyMap`. Not applicable — 2 keys.
- Single-sort-key top-N in TransportShardExecuteAction (line 487): **IS used** — dispatches to `executeWithTopN` since sort is on single aggregate column.

## Summary: Execution Flow

```
TransportShardExecuteAction.executePlan()
  → extractAggFromSortedLimit() matches LimitNode→SortNode→AggNode
  → sort on single agg column (COUNT*) → executeFusedGroupByAggregateWithTopN()
    → FusedGroupByAggregate.executeWithTopN(sortAggIndex=0, sortAsc=false, topN=10)
      → executeInternal() → executeNumericOnly() → executeTwoKeyNumeric()
        → canUseFlatAccumulators=true (COUNT+SUM+AVG all long)
        → estimate docs: 25M/shard > 8M MAX_CAPACITY → numBuckets=4
        → parallel executeTwoKeyNumericFlat() × 4 buckets
          → per bucket: scanSegmentFlatTwoKey() per segment
            → Q30/Q31: Scorer iterator + advanceExact per doc
            → Q32: MatchAll + nextDoc lockstep
          → FlatTwoKeyMap: open-addressing, keys0[]/keys1[]/accData[] (4 slots/group)
          → top-N heap selection on COUNT(*) slot
        → mergePartitionedPages() across buckets
  → Coordinator: mergeAggregationFastNumericWithSort()
    → open-addressing long[][] hash map
    → AVG weighted merge
    → top-N heap selection → output 10 rows
```
