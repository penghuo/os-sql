# TopN Heap Push-Down & FlatSingleKeyMap Code Sections

## 1. TopN Heap in executeSingleVarcharCountStar (lines ~1707-1780)

Uses ordinal-indexed `ordCounts[]` array. Heap stores ordinal indices + count values.

```java
// Min-heap (for DESC) or max-heap (for ASC) of ordinal indices
int[] heap = new int[n];
long[] heapVals = new long[n];
int heapSize = 0;

for (int i = 0; i < ordCounts.length; i++) {
  if (ordCounts[i] == 0) continue;
  long cnt = ordCounts[i];
  if (heapSize < n) {
    heap[heapSize] = i;
    heapVals[heapSize] = cnt;
    heapSize++;
    // Sift up
    int k = heapSize - 1;
    while (k > 0) {
      int parent = (k - 1) >>> 1;
      boolean swap = sortAscending ? (heapVals[k] > heapVals[parent]) : (heapVals[k] < heapVals[parent]);
      if (swap) { /* swap heap[k] <-> heap[parent], heapVals[k] <-> heapVals[parent] */ k = parent; }
      else break;
    }
  } else {
    boolean better = sortAscending ? (cnt < heapVals[0]) : (cnt > heapVals[0]);
    if (better) {
      heap[0] = i;
      heapVals[0] = cnt;
      // Sift down from root
      // ... standard binary heap sift-down ...
    }
  }
}
```

Key pattern: For ASC sort, heap is a **max-heap** (evicts largest, keeps smallest N). For DESC sort, heap is a **min-heap** (evicts smallest, keeps largest N).

---

## 2. TopN Heap Already in executeSingleKeyNumericFlat (lines ~5395-5460)

This method **already has** a TopN heap implementation at the materialization stage. It's triggered by:
```java
if (sortAggIndex >= 0 && topN > 0 && topN < flatMap.size) {
```

The heap iterates `flatMap.capacity` slots, skipping `EMPTY_KEY` slots:
```java
for (int slot = 0; slot < flatMap.capacity; slot++) {
  if (flatMap.keys[slot] == FlatSingleKeyMap.EMPTY_KEY) continue;
  long val = flatMap.accData[slot * slotsPerGroup + sortAccOff];
  // ... heap insert/replace logic identical to varchar version ...
}
outputSlots = heap;  // heap contains slot indices into flatMap
outputCount = heapSize;
```

The heap stores **slot indices** (not ordinals) and reads sort values from `flatMap.accData[slot * slotsPerGroup + sortAccOff]`.

---

## 3. executeSingleKeyNumericFlat Full Structure (lines 5184-5800)

### Signature
```java
private static List<Page> executeSingleKeyNumericFlat(
    IndexShard shard, Query query, List<KeyInfo> keyInfos, List<AggSpec> specs,
    Map<String, Type> columnTypeMap, List<String> groupByKeys,
    int numAggs, boolean[] isCountStar, int[] accType,
    int sortAggIndex, boolean sortAscending, long topN,
    int bucket, int numBuckets) throws Exception
```

### Layout
1. **Lines 5198-5215**: Compute `accOffset[]` and `slotsPerGroup` (COUNT/SUM=1 slot, AVG=2 slots)
2. **Lines 5216**: Create `FlatSingleKeyMap flatMap = new FlatSingleKeyMap(slotsPerGroup)`
3. **Lines 5218-5390**: Scanning phase (3 paths):
   - **Doc-range parallel**: COUNT(*)-only + MatchAll + no bucketing → split doc ranges across workers
   - **Segment-parallel**: Partition segments across `THREADS_PER_SHARD` workers, each with local `FlatSingleKeyMap`, then `mergeFrom()`
   - **Sequential**: Single-threaded scan through all segments
4. **Lines 5392-5460**: TopN selection via heap (already exists!)
5. **Lines 5462-5510**: Materialization — build `BlockBuilder[]` from `outputSlots`, write keys via `writeNumericKeyValue()`, write agg values based on `accType`

### Scanning delegates to:
- `scanSegmentFlatSingleKey()` — per-segment scan, handles MatchAll vs filtered
- `scanDocRangeFlatSingleKeyCountStar()` — optimized COUNT(*) doc-range scan
- `collectFlatSingleKeyDoc()` — per-doc accumulation
- `collectFlatSingleKeyDocWithLength()` — per-doc with VARCHAR LENGTH() support

---

## 4. executeSingleKeyNumeric Dispatch (lines 3516-3700)

This is the **entry point** that decides flat vs object-based path:

```java
// Determines accType[] and canUseFlatAccumulators flag
// Flat path: COUNT(*), SUM long, AVG long only
// Object path: COUNT DISTINCT, SUM/AVG double, MIN, MAX, VARCHAR args

if (canUseFlatAccumulators) {
  // Estimate doc count to determine numBuckets
  numBuckets = Math.max(1, ceil(totalDocs / FlatSingleKeyMap.MAX_CAPACITY));
  
  if (numBuckets <= 1) {
    return executeSingleKeyNumericFlat(..., bucket=0, numBuckets=1);
  } else {
    // Multi-bucket: parallel or sequential passes
    // Each pass filters keys by hash bucket
    // Results merged via mergePartitionedPages()
  }
} else {
  // Falls through to SingleKeyHashMap (object-based) path
}
```

---

## 5. FlatSingleKeyMap API (line 13464)

```java
private static final class FlatSingleKeyMap {
  static final long EMPTY_KEY = Long.MIN_VALUE;
  static final int MAX_CAPACITY = 16_000_000;
  
  long[] keys;           // hash table keys, EMPTY_KEY = unused slot
  long[] accData;        // contiguous: slot i's data at [i*slotsPerGroup .. (i+1)*slotsPerGroup)
  int size;              // number of occupied slots
  int capacity;          // current hash table capacity (power of 2)
  final int slotsPerGroup;
  
  int findOrInsert(long key);     // returns slot index, auto-resizes
  void mergeFrom(FlatSingleKeyMap other);  // element-wise add of accData
}
```

**Key access pattern**: 
- Iterate: `for (slot = 0; slot < flatMap.capacity; slot++) { if (keys[slot] != EMPTY_KEY) ... }`
- Read key: `flatMap.keys[slot]`
- Read agg value: `flatMap.accData[slot * slotsPerGroup + accOffset[aggIndex]]`

---

## 6. Key Findings for TopN Integration

1. **TopN already exists in executeSingleKeyNumericFlat** at the materialization stage (post-scan). No new insertion needed there.
2. The varchar version's TopN operates on `ordCounts[]` (dense ordinal array), while the flat numeric version operates on `flatMap` (sparse hash table with `EMPTY_KEY` sentinel).
3. Both use identical heap logic — the only difference is how values are read:
   - Varchar: `ordCounts[i]` 
   - Flat numeric: `flatMap.accData[slot * slotsPerGroup + sortAccOff]`
4. The `outputSlots` array from TopN feeds directly into the materialization loop.
5. For multi-bucket partitioned aggregation, TopN is applied per-bucket, then `mergePartitionedPages()` handles cross-bucket merging.
