# Doc-Range Parallel Pattern Analysis

Source: `FusedGroupByAggregate.java`

---

## 1. Doc-Range Computation (Lines 5222-5237)

The doc-range pattern splits segments into work units of `docsPerWorker` docs each:

```java
// Line 5222-5237
flatMap = new FlatSingleKeyMap(slotsPerGroup, 4_000_000);
int numWorkers = THREADS_PER_SHARD;
long docsPerWorker = Math.max(1, (totalDocs + numWorkers - 1) / numWorkers);
java.util.List<int[]> workUnits = new java.util.ArrayList<>();
for (int s = 0; s < leaves.size(); s++) {
    int maxDoc = segKeyArrays.get(s).length;
    if (maxDoc == 0) continue;
    for (int start = 0; start < maxDoc; start += (int) docsPerWorker) {
        int end = (int) Math.min(start + docsPerWorker, maxDoc);
        workUnits.add(new int[]{s, start, end});  // {segmentIndex, startDoc, endDoc}
    }
}
```

Each work unit is `int[]{segmentIndex, startDoc, endDoc}`.

---

## 2. Worker Submission to PARALLEL_POOL (Lines 5239-5260)

```java
// Line 5239-5260
int actualWorkers = workUnits.size();
@SuppressWarnings("unchecked")
java.util.concurrent.CompletableFuture<FlatSingleKeyMap>[] futures =
    new java.util.concurrent.CompletableFuture[actualWorkers];

for (int w = 0; w < actualWorkers; w++) {
    final int[] unit = workUnits.get(w);
    final long[] keyValues = segKeyArrays.get(unit[0]);
    final Bits liveDocs = segLiveDocs.get(unit[0]);
    final int startDoc = unit[1];
    final int endDoc = unit[2];
    futures[w] =
        java.util.concurrent.CompletableFuture.supplyAsync(
            () -> {
                int docCount = endDoc - startDoc;
                int initCap = (int) Math.min((long) docCount * 10 / 7 + 1, 4_000_000);
                FlatSingleKeyMap localMap = new FlatSingleKeyMap(slotsPerGroup, initCap);
                scanDocRangeFlatSingleKeyCountStar(
                    keyValues, startDoc, endDoc, liveDocs,
                    localMap, accOffset[0], slotsPerGroup);
                return localMap;
            },
            PARALLEL_POOL);
}
```

---

## 3. FlatSingleKeyMap Per-Worker Creation (Lines 5253-5254)

Each worker creates its own map, pre-sized based on doc count:

```java
int docCount = endDoc - startDoc;
int initCap = (int) Math.min((long) docCount * 10 / 7 + 1, 4_000_000);
FlatSingleKeyMap localMap = new FlatSingleKeyMap(slotsPerGroup, initCap);
```

---

## 4. Merge via mergeFrom() (Lines 5262-5266)

```java
// Line 5262-5266
java.util.concurrent.CompletableFuture.allOf(futures).join();
for (int fi = 0; fi < futures.length; fi++) {
    FlatSingleKeyMap workerMap = futures[fi].join();
    if (workerMap.size > 0) flatMap.mergeFrom(workerMap);
    futures[fi] = null;  // release worker map for GC
}
```

---

## 5. scanDocRangeFlatSingleKeyCountStar (Lines 14278-14295)

```java
// Line 14278-14295
private static void scanDocRangeFlatSingleKeyCountStar(
    long[] keyValues, int startDoc, int endDoc, Bits liveDocs,
    FlatSingleKeyMap flatMap, int accOffset0, int slotsPerGroup) {
  if (liveDocs == null) {
    for (int doc = startDoc; doc < endDoc; doc++) {
      int slot = flatMap.findOrInsert(keyValues[doc]);
      flatMap.accData[slot * slotsPerGroup + accOffset0]++;
    }
  } else {
    for (int doc = startDoc; doc < endDoc; doc++) {
      if (!liveDocs.get(doc)) continue;
      int slot = flatMap.findOrInsert(keyValues[doc]);
      flatMap.accData[slot * slotsPerGroup + accOffset0]++;
    }
  }
}
```

---

## 6. `leaves.size() > 1` Guard in executeSingleKeyNumericFlat (Line 5188)

```java
// Line 5188
boolean canParallelize =
    !"off".equals(PARALLELISM_MODE) && THREADS_PER_SHARD > 1 && leaves.size() > 1;
```

This is the guard that **prevents** doc-range parallelism for single-segment shards. The doc-range path at line 5197 is nested inside `if (canParallelize)`, so single-segment shards fall through to the sequential path.

**Key issue**: The doc-range parallel path (lines 5197-5266) is ONLY reachable when `leaves.size() > 1`. For single-segment shards, the code falls to the sequential path at line 5268.

---

## 7. `!anyVarcharKey` Guard in executeThreeKeyFlat (Lines 9976-9980)

```java
// Lines 9976-9980 (inside executeThreeKeyFlat)
boolean anyVarcharKey =
    keyInfos.get(0).isVarchar || keyInfos.get(1).isVarchar || keyInfos.get(2).isVarchar;
// Parallel only when all keys are numeric (ordinals are segment-local for varchar)
boolean canParallelize =
    !"off".equals(PARALLELISM_MODE)
        && THREADS_PER_SHARD > 1
        && leaves.size() > 1
        && !anyVarcharKey;
```

This guard prevents parallelism when ANY key is varchar because ordinals are segment-local.

Also at line 10445 in executeNKeyVarcharPath:
```java
if (singleSegment || !anyVarcharKey) {
```
This allows the 3-key flat path for single-segment (varchar ordinals are valid within one segment) OR all-numeric keys (no ordinal issue).

---

## 8. FlatTwoKeyMap Constructor and mergeFrom (Lines 13268-13397)

**Constructor** (line 13273):
```java
FlatTwoKeyMap(int slotsPerGroup, int initialCapacity) {
    this.slotsPerGroup = slotsPerGroup;
    int cap = Integer.highestOneBit(Math.max(initialCapacity, INITIAL_CAPACITY) - 1) << 1;
    if (cap < INITIAL_CAPACITY) cap = INITIAL_CAPACITY;
    this.capacity = cap;
    this.keys0 = new long[capacity];
    Arrays.fill(keys0, EMPTY_KEY);
    this.keys1 = new long[capacity];
    this.accData = new long[capacity * slotsPerGroup];
    this.size = 0;
    this.threshold = (int) (capacity * LOAD_FACTOR);
}
```

**mergeFrom** (line 13387):
```java
void mergeFrom(FlatTwoKeyMap other) {
    for (int s = 0; s < other.capacity; s++) {
        if (other.keys0[s] == EMPTY_KEY) continue;
        int slot = findOrInsert(other.keys0[s], other.keys1[s]);
        int dstBase = slot * slotsPerGroup;
        int srcBase = s * other.slotsPerGroup;
        for (int j = 0; j < slotsPerGroup; j++) {
            accData[dstBase + j] += other.accData[srcBase + j];
        }
    }
}
```

---

## 8b. FlatThreeKeyMap Constructor and mergeFrom (Lines 13424-13520)

**Constructor** (line 13424):
```java
FlatThreeKeyMap(int slotsPerGroup) {
    this.slotsPerGroup = slotsPerGroup;
    this.capacity = INITIAL_CAPACITY;  // 8192
    this.keys0 = new long[capacity];
    Arrays.fill(keys0, EMPTY_KEY);
    this.keys1 = new long[capacity];
    this.keys2 = new long[capacity];
    this.accData = new long[capacity * slotsPerGroup];
    this.size = 0;
    this.threshold = (int) (capacity * LOAD_FACTOR);
}
```

**mergeFrom** (line 13508):
```java
void mergeFrom(FlatThreeKeyMap other) {
    for (int s = 0; s < other.capacity; s++) {
        if (other.keys0[s] == EMPTY_KEY) continue;
        int slot = findOrInsert(other.keys0[s], other.keys1[s], other.keys2[s]);
        int dstBase = slot * slotsPerGroup;
        int srcBase = s * other.slotsPerGroup;
        for (int j = 0; j < slotsPerGroup; j++) {
            accData[dstBase + j] += other.accData[srcBase + j];
        }
    }
}
```

---

## Existing Segment-Parallel Pattern (for reference)

The segment-parallel pattern used in executeTwoKeyNumericFlat (lines 6405-6450) and executeThreeKeyFlat (lines 9982-10030) follows this structure:

```java
// 1. Partition segments across workers (largest-first greedy)
int numWorkers = Math.min(THREADS_PER_SHARD, leaves.size());
List<LeafReaderContext>[] workerSegments = new List[numWorkers];
long[] workerDocCounts = new long[numWorkers];
for (int i = 0; i < numWorkers; i++) workerSegments[i] = new ArrayList<>();
List<LeafReaderContext> sortedLeaves = new ArrayList<>(leaves);
sortedLeaves.sort((a, b) -> Integer.compare(b.reader().maxDoc(), a.reader().maxDoc()));
for (LeafReaderContext leaf : sortedLeaves) {
    int lightest = 0;
    for (int i = 1; i < numWorkers; i++) {
        if (workerDocCounts[i] < workerDocCounts[lightest]) lightest = i;
    }
    workerSegments[lightest].add(leaf);
    workerDocCounts[lightest] += leaf.reader().maxDoc();
}

// 2. Submit workers
CompletableFuture<FlatXxxKeyMap>[] futures = new CompletableFuture[numWorkers];
for (int w = 0; w < numWorkers; w++) {
    final List<LeafReaderContext> mySegments = workerSegments[w];
    futures[w] = CompletableFuture.supplyAsync(() -> {
        FlatXxxKeyMap localMap = new FlatXxxKeyMap(slotsPerGroup);
        for (LeafReaderContext leafCtx : mySegments) {
            scanSegmentFlatXxxKey(leafCtx, weight, isMatchAll, localMap, ...);
        }
        return localMap;
    }, PARALLEL_POOL);
}

// 3. Merge
CompletableFuture.allOf(futures).join();
for (int fi = 0; fi < futures.length; fi++) {
    FlatXxxKeyMap workerMap = futures[fi].join();
    if (workerMap.size > 0) flatMap.mergeFrom(workerMap);
    futures[fi] = null;
}
```

---

## Summary of What Needs to Change

### For `executeSingleKeyNumericFlat` (single-segment fix):
- **Line 5188**: Change `leaves.size() > 1` to allow single-segment doc-range parallelism
- The doc-range path at lines 5197-5266 already handles single-segment correctly (it iterates `segKeyArrays` which has 1 entry for single-segment), but the `canParallelize` guard blocks it

### For `executeTwoKeyNumericFlat`:
- Currently only has segment-parallel (lines 6405-6450), no doc-range path
- Need to add doc-range parallel similar to lines 5197-5266 but using `FlatTwoKeyMap` and a `scanDocRangeFlatTwoKeyCountStar` helper

### For `executeThreeKeyFlat`:
- Currently only has segment-parallel (lines 9982-10030), no doc-range path
- The `!anyVarcharKey` guard (line 9980) already prevents parallelism for varchar keys
- Need to add doc-range parallel similar to lines 5197-5266 but using `FlatThreeKeyMap` and a `scanDocRangeFlatThreeKeyCountStar` helper
- For single-segment with varchar keys: need doc-range within the single segment (ordinals are valid within one segment)
