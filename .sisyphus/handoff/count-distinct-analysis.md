# FusedGroupByAggregate COUNT(DISTINCT) Analysis

## 1. COUNT(DISTINCT) / Dedup / HashSet Accumulation

### CountDistinctAccum (inner class) — Line 14516
```java
private static class CountDistinctAccum implements MergeableAccumulator {
    final Type argType;
    final boolean usePrimitiveLong;
    final LongOpenHashSet longDistinctValues;    // for numeric non-double
    final Set<Object> objectDistinctValues;       // for varchar/double
}
```
- **usePrimitiveLong**: true when argType is NOT VarcharType and NOT DoubleType
- **longDistinctValues**: `LongOpenHashSet` (custom primitive set) — used for numeric COUNT(DISTINCT)
- **objectDistinctValues**: `HashSet<Object>` — used for varchar/double COUNT(DISTINCT)
- Initial capacity: 16 (small, resizes dynamically)

### accType == 5 dispatching
COUNT(DISTINCT) is encoded as `accType[i] = 5` throughout the file. Found at ~20 locations in switch statements. Pattern:
```java
case 5: // COUNT(DISTINCT)
    CountDistinctAccum cda = (CountDistinctAccum) acc;
    if (cda.usePrimitiveLong) cda.longDistinctValues.add(rawVal);
    else cda.objectDistinctValues.add(Double.longBitsToDouble(rawVal));
```
For varchar args, the pattern is:
```java
case 5:
    ((CountDistinctAccum) acc).objectDistinctValues.add(val); // val is String
```

### Key locations of accType=5 handling:
- Line 713, 759, 772: executeWithExpressionKeyImpl (Collector path)
- Line 1089, 1133, 1146, 1202: processExprKeySegment (parallel expr-key)
- Line 3732: executeSingleKeyNumeric (sequential lockstep)
- Line 4042: collectSingleKeyNumericDoc
- Line 9367, 9425: executeWithVarcharKeys (N-key varchar, inline accum)
- Line 10867, 10925: executeNKeyVarcharPath (single-segment N-key)
- Line 11297, 11355: executeNKeyVarcharPath (multi-segment global-ord parallel)
- Line 11693, 11751: executeNKeyVarcharPath (multi-segment sequential)
- Line 12179, 12237: executeNKeyVarcharParallelDocRange
- Line 14078: accumulateDocNumeric helper

### Merge logic (CountDistinctAccum.merge) — Line ~14540
```java
public void merge(MergeableAccumulator other) {
    CountDistinctAccum o = (CountDistinctAccum) other;
    if (usePrimitiveLong) longDistinctValues.addAll(o.longDistinctValues);
    else objectDistinctValues.addAll(o.objectDistinctValues);
}
```

### writeTo — Line ~14548
```java
public void writeTo(BlockBuilder builder) {
    long count = usePrimitiveLong ? longDistinctValues.size() : objectDistinctValues.size();
    BigintType.BIGINT.writeLong(builder, count);
}
```

### getSortValue — Line ~14554
Returns `longDistinctValues.size()` or `objectDistinctValues.size()`.

### createAccumulator dispatch — Line ~14480
```java
case "COUNT":
    if (spec.isDistinct) return new CountDistinctAccum(spec.argType);
```

### canUseFlatAccumulators — COUNT(DISTINCT) BLOCKS flat path
In executeSingleKeyNumeric (line ~3540), executeTwoKeyNumeric (line ~6140), executeDerivedSingleKeyNumeric (line ~4090):
```java
case "COUNT":
    if (spec.isDistinct) { accType[i] = 5; canUseFlatAccumulators = false; }
```
COUNT(DISTINCT) forces the object-based AccumulatorGroup path (not FlatSingleKeyMap/FlatTwoKeyMap).

## 2. LongOpenHashSet Usage

**File**: `dqe/src/main/java/org/opensearch/sql/dqe/operator/LongOpenHashSet.java`

- Open-addressing hash set for primitive longs (avoids Long boxing)
- Sentinel-based: `EMPTY = Long.MIN_VALUE`, separate `hasZero`/`hasSentinel` flags
- Load factor: 0.65, Murmur3 hash
- Key methods:
  - `add(long)` — line 62: insert with linear probing
  - `addAll(LongOpenHashSet)` — line 186: bulk merge for cross-segment
  - `addAllBatched(long[], int, int)` — line 107: prefetch-batched bulk add
  - `contains(long)` — line 137: probe lookup
  - `size()` — line 152
  - `ensureCapacity(int)` — line 175: pre-allocate before merge

**Used exclusively by** `CountDistinctAccum.longDistinctValues` for numeric non-double COUNT(DISTINCT).

## 3. executeNKey* Methods

### executeNKeyVarcharPath — Line 10644
```java
private static List<Page> executeNKeyVarcharPath(Searcher, Query, boolean singleSegment,
    List<KeyInfo>, List<AggSpec>, int numAggs, boolean[] isCountStar, boolean[] isDoubleArg,
    boolean[] isVarcharArg, int[] accType, String[] truncUnits, String[] arithUnits,
    List<String> groupByKeys, int sortAggIndex, boolean sortAscending, long topN,
    Map<String,Type> columnTypeMap)
```
- Handles 3+ key GROUP BY with at least one VARCHAR key
- Dispatches to: executeThreeKeyFlat (3 numeric keys), single-segment SegmentGroupKey path, global-ordinals multi-segment path, parallel doc-range path, or sequential multi-segment merge path
- COUNT(DISTINCT) handled via inline switch at lines 10867, 10925, 11297, 11355, 11693, 11751

### executeNKeyVarcharParallelDocRange — Line 12007
```java
private static List<Page> executeNKeyVarcharParallelDocRange(Searcher, Query,
    List<KeyInfo>, List<AggSpec>, int numAggs, boolean[] isCountStar, boolean[] isDoubleArg,
    boolean[] isVarcharArg, int[] accType, String[] truncUnits, String[] arithUnits,
    List<String> groupByKeys, int sortAggIndex, boolean sortAscending, long topN,
    Map<String,Type> columnTypeMap)
```
- Doc-range parallel for N-key varchar: collects matching doc IDs per segment, splits across ForkJoinPool workers
- Each worker builds local SegmentGroupKey→AccumulatorGroup map, resolves ordinals, merges into worker's MergedGroupKey map
- COUNT(DISTINCT) at lines 12179, 12237

## 4. scanDocRange* Methods and Parallel Variants

### scanDocRangeFlatTwoKey — Line 7202
```java
private static void scanDocRangeFlatTwoKey(LeafReaderContext, int startDoc, int endDoc,
    FlatTwoKeyMap, List<KeyInfo>, List<AggSpec>, int numAggs, boolean[] isCountStar,
    int[] accType, int[] accOffset, int slotsPerGroup)
```
- Scans [startDoc, endDoc) within a single segment for flat 2-key numeric GROUP BY
- Used by doc-range parallel execution for single-segment shards
- Only supports flat accumulators (COUNT/SUM/AVG) — **no COUNT(DISTINCT)**

### scanDocRangeFlatSingleKeyCountStar — Line 14630
```java
private static void scanDocRangeFlatSingleKeyCountStar(long[] keyValues, int startDoc,
    int endDoc, Bits liveDocs, FlatSingleKeyMap flatMap, int accOffset0, int slotsPerGroup)
```
- Ultra-optimized: pre-loaded columnar key array, prefetch-batched hash probing
- Run-length optimization for sorted data (consecutive identical keys)
- **COUNT(*) only** — no COUNT(DISTINCT)

### Parallel variants (called from):
- `executeSingleKeyNumericFlat` (line 5234): spawns CompletableFuture workers calling `scanDocRangeFlatSingleKeyCountStar`
- `executeTwoKeyNumericFlat` (line 6444): spawns workers calling `scanDocRangeFlatTwoKey`
- `executeDerivedSingleKeyNumeric` (line 4073): spawns workers calling `scanDocRangeFlatSingleKeyCountStar`
- `executeSingleKeyNumericFlatMultiBucket` (line 4802): multi-bucket variant calling `scanSegmentFlatSingleKeyMultiBucket`

### Other segment-scan helpers:
- `scanSegmentFlatSingleKey` — Line 5707: per-segment scan for flat 1-key
- `scanSegmentFlatTwoKey` — Line 6788: per-segment scan for flat 2-key
- `scanSegmentFlatThreeKey` — Line 10456: per-segment scan for flat 3-key
- `scanSegmentDerivedSingleKeyFlat` — Line 4544: per-segment for derived single-key
- `scanSegmentFlatSingleKeyMultiBucket` — Line 5022: multi-bucket routing per segment
- `processExprKeySegment` — Line 957: per-segment for expression-key GROUP BY

## Key Architectural Insight for COUNT(DISTINCT) Optimization

**Current limitation**: COUNT(DISTINCT) forces `canUseFlatAccumulators = false`, meaning it always uses the object-based `AccumulatorGroup` + `CountDistinctAccum` path with per-group `LongOpenHashSet` allocation. The flat paths (`FlatSingleKeyMap`, `FlatTwoKeyMap`, `FlatThreeKeyMap`) that store accumulators in contiguous `long[]` arrays cannot represent the variable-size HashSet state.

**Potential optimization vectors**:
1. For low-cardinality DISTINCT columns: use ordinal-based bitset instead of LongOpenHashSet
2. For single-segment + single VARCHAR DISTINCT arg: use ordinal dedup (ordinals are already unique per value)
3. Pre-size LongOpenHashSet based on field's PointValues min/max range
4. Batch-add optimization: LongOpenHashSet.addAllBatched exists but isn't used in the GROUP BY hot paths
