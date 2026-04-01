# Q35 Parallel Code — Current Implementation

File: `dqe/src/main/java/org/opensearch/sql/dqe/shard/source/FusedGroupByAggregate.java`

## 1. FlatSingleKeyMap (line 13891)

```java
private static final class FlatSingleKeyMap {
    private static final int INITIAL_CAPACITY = 4096;
    private static final float LOAD_FACTOR = 0.7f;
    private static final int MAX_CAPACITY = 32_000_000;
    static final long EMPTY_KEY = Long.MIN_VALUE;

    long[] keys;
    long[] accData; // contiguous: slot i's data at [i*slotsPerGroup .. (i+1)*slotsPerGroup)
    int size;
    int capacity;
    int threshold;
    final int slotsPerGroup;

    FlatSingleKeyMap(int slotsPerGroup) {
      this(slotsPerGroup, INITIAL_CAPACITY);
    }

    FlatSingleKeyMap(int slotsPerGroup, int initialCapacity) {
      this.slotsPerGroup = slotsPerGroup;
      int cap = Integer.highestOneBit(Math.max(initialCapacity, INITIAL_CAPACITY) - 1) << 1;
      if (cap < INITIAL_CAPACITY) cap = INITIAL_CAPACITY;
      this.capacity = cap;
      this.keys = new long[capacity];
      Arrays.fill(keys, EMPTY_KEY);
      this.accData = new long[capacity * slotsPerGroup];
      this.size = 0;
      this.threshold = (int) (capacity * LOAD_FACTOR);
    }

    // --- findOrInsert (line 13925) ---
    int findOrInsert(long key) {
      int mask = capacity - 1;
      int h = SingleKeyHashMap.hash1(key) & mask;
      while (keys[h] != EMPTY_KEY) {
        if (keys[h] == key) return h;
        h = (h + 1) & mask;
      }
      keys[h] = key;
      size++;
      if (size > threshold) {
        resize();
        return findExisting(key);
      }
      return h;
    }

    // --- findOrInsertFromSlot (line 13988) ---
    // Used by prefetch-batched scan loops where hash slot is pre-computed
    int findOrInsertFromSlot(long key, int startSlot) {
      int mask = capacity - 1;
      int h = startSlot;
      while (keys[h] != EMPTY_KEY) {
        if (keys[h] == key) return h;
        h = (h + 1) & mask;
      }
      keys[h] = key;
      size++;
      if (size > threshold) {
        resize();
        return findExisting(key);
      }
      return h;
    }

    // --- mergeFrom (line 14005) ---
    // Prefetch-batched merge: compute target hash slots ahead of time to warm cache lines
    void mergeFrom(FlatSingleKeyMap other) {
      final int BATCH = 32;
      int[] batchSrcSlots = new int[BATCH];
      int[] batchTargetSlots = new int[BATCH];
      int batchLen = 0;
      long prefetchSink = 0;
      int mask = capacity - 1;

      for (int s = 0; s < other.capacity; s++) {
        if (other.keys[s] == EMPTY_KEY) continue;
        batchSrcSlots[batchLen] = s;
        batchTargetSlots[batchLen] = SingleKeyHashMap.hash1(other.keys[s]) & mask;
        batchLen++;
        if (batchLen == BATCH) {
          // Phase 1: prefetch target hash slots
          for (int j = 0; j < BATCH; j++) {
            prefetchSink += keys[batchTargetSlots[j]];
            prefetchSink += accData[batchTargetSlots[j] * slotsPerGroup];
          }
          // Phase 2: actual merge
          for (int j = 0; j < BATCH; j++) {
            int srcSlot = batchSrcSlots[j];
            int slot = findOrInsertFromSlot(other.keys[srcSlot], batchTargetSlots[j]);
            int dstBase = slot * slotsPerGroup;
            int srcBase = srcSlot * other.slotsPerGroup;
            for (int k = 0; k < slotsPerGroup; k++) {
              accData[dstBase + k] += other.accData[srcBase + k];
            }
          }
          mask = capacity - 1; // update in case resize happened
          batchLen = 0;
        }
      }
      // Remainder
      for (int j = 0; j < batchLen; j++) {
        int srcSlot = batchSrcSlots[j];
        int slot = findOrInsertFromSlot(other.keys[srcSlot], batchTargetSlots[j]);
        int dstBase = slot * slotsPerGroup;
        int srcBase = srcSlot * other.slotsPerGroup;
        for (int k = 0; k < slotsPerGroup; k++) {
          accData[dstBase + k] += other.accData[srcBase + k];
        }
      }
      if (prefetchSink == Long.MIN_VALUE) System.nanoTime(); // prevent dead code elimination
    }

    // --- resize (line ~13960) ---
    private void resize() {
      if (size > MAX_CAPACITY) {
        throw new RuntimeException("GROUP BY exceeded memory limit (" + size
            + " unique groups, max " + MAX_CAPACITY + ").");
      }
      int newCap = capacity * 2;
      long[] nk = new long[newCap];
      Arrays.fill(nk, EMPTY_KEY);
      long[] nacc = new long[newCap * slotsPerGroup];
      int nm = newCap - 1;
      for (int s = 0; s < capacity; s++) {
        if (keys[s] != EMPTY_KEY) {
          int nh = SingleKeyHashMap.hash1(keys[s]) & nm;
          while (nk[nh] != EMPTY_KEY) nh = (nh + 1) & nm;
          nk[nh] = keys[s];
          System.arraycopy(accData, s * slotsPerGroup, nacc, nh * slotsPerGroup, slotsPerGroup);
        }
      }
      this.keys = nk;
      this.accData = nacc;
      this.capacity = newCap;
      this.threshold = (int) (newCap * LOAD_FACTOR);
    }

    private int findExisting(long key) {
      int mask = capacity - 1;
      int h = SingleKeyHashMap.hash1(key) & mask;
      while (keys[h] != EMPTY_KEY) {
        if (keys[h] == key) return h;
        h = (h + 1) & mask;
      }
      throw new IllegalStateException("Key not found after resize");
    }
}
```

## 2. Parallel Doc-Range Path in executeDerivedSingleKeyNumeric (line 4279)

This is the `allCountStar && isMatchAll` fast path. Steps:
1. Load all segment key columns into flat `long[]` arrays (parallel across segments)
2. Split total docs across `THREADS_PER_SHARD` workers
3. Each worker builds a local `FlatSingleKeyMap` and calls `scanDocRangeFlatSingleKeyCountStar`
4. Find the largest worker map, merge all others into it

```java
if (allCountStar && isMatchAll) {
    // Step 1: Load columnar key arrays (parallel across segments)
    int numSegs = leaves.size();
    long[][] segKeyArrays = new long[numSegs][];
    Bits[] segLiveDocs = new Bits[numSegs];
    if (numSegs > 1 && canParallelize) {
      CompletableFuture<Void>[] loadFutures = new CompletableFuture[numSegs];
      for (int s = 0; s < numSegs; s++) {
        final int segIdx = s;
        final LeafReaderContext leafCtx = leaves.get(s);
        loadFutures[s] = CompletableFuture.runAsync(() -> {
          try {
            segKeyArrays[segIdx] = loadNumericColumn(leafCtx, sourceCol);
            segLiveDocs[segIdx] = leafCtx.reader().getLiveDocs();
          } catch (Exception e) { throw new RuntimeException(e); }
        }, PARALLEL_POOL);
      }
      CompletableFuture.allOf(loadFutures).join();
    } else {
      for (int s = 0; s < numSegs; s++) {
        segKeyArrays[s] = loadNumericColumn(leaves.get(s), sourceCol);
        segLiveDocs[s] = leaves.get(s).reader().getLiveDocs();
      }
    }
    long totalDocs = 0;
    for (long[] arr : segKeyArrays) totalDocs += arr.length;

    if (canParallelize && leaves.size() > 1 && totalDocs <= 200_000_000) {
      // Step 2: Split docs into work units
      int numWorkers = THREADS_PER_SHARD;
      long docsPerWorker = Math.max(1, (totalDocs + numWorkers - 1) / numWorkers);
      List<int[]> workUnits = new ArrayList<>();
      for (int s = 0; s < leaves.size(); s++) {
        int maxDoc = segKeyArrays[s].length;
        if (maxDoc == 0) continue;
        for (int start = 0; start < maxDoc; start += (int) docsPerWorker) {
          int end = (int) Math.min(start + docsPerWorker, maxDoc);
          workUnits.add(new int[]{s, start, end});
        }
      }

      // Step 3: Each worker builds local FlatSingleKeyMap
      int actualWorkers = workUnits.size();
      CompletableFuture<FlatSingleKeyMap>[] futures = new CompletableFuture[actualWorkers];
      for (int w = 0; w < actualWorkers; w++) {
        final int[] unit = workUnits.get(w);
        final long[] keyValues = segKeyArrays[unit[0]];
        final Bits liveDocs = segLiveDocs[unit[0]];
        final int startDoc = unit[1];
        final int endDoc = unit[2];
        futures[w] = CompletableFuture.supplyAsync(() -> {
          int docCount = endDoc - startDoc;
          int initCap = (int) Math.min((long) docCount * 10 / 7 + 1, 16_000_000);
          FlatSingleKeyMap localMap = new FlatSingleKeyMap(slotsPerGroup, initCap);
          scanDocRangeFlatSingleKeyCountStar(
              keyValues, startDoc, endDoc, liveDocs,
              localMap, accOffset[0], slotsPerGroup);
          return localMap;
        }, PARALLEL_POOL);
      }
      CompletableFuture.allOf(futures).join();

      // Step 4: Merge — use largest as base, merge others into it
      FlatSingleKeyMap[] workerMaps = new FlatSingleKeyMap[actualWorkers];
      int largestIdx = 0;
      for (int fi = 0; fi < actualWorkers; fi++) {
        workerMaps[fi] = futures[fi].join();
        if (workerMaps[fi].size > workerMaps[largestIdx].size) largestIdx = fi;
      }
      flatMap = workerMaps[largestIdx];
      for (int fi = 0; fi < actualWorkers; fi++) {
        if (fi != largestIdx && workerMaps[fi].size > 0) flatMap.mergeFrom(workerMaps[fi]);
        workerMaps[fi] = null;
      }
    } else {
      // Sequential fallback
      flatMap = new FlatSingleKeyMap(slotsPerGroup, 8_000_000);
      for (int s = 0; s < segKeyArrays.length; s++) {
        scanDocRangeFlatSingleKeyCountStar(
            segKeyArrays[s], 0, segKeyArrays[s].length,
            segLiveDocs[s], flatMap, accOffset[0], slotsPerGroup);
      }
    }
}
```

## 3. scanDocRangeFlatSingleKeyCountStar (line 14699)

The inner scan loop. Uses run-length optimization + prefetch batching:

```java
private static void scanDocRangeFlatSingleKeyCountStar(
    long[] keyValues, int startDoc, int endDoc, Bits liveDocs,
    FlatSingleKeyMap flatMap, int accOffset0, int slotsPerGroup) {

  // liveDocs path: simple run-length cached
  if (liveDocs != null) {
    long lastKey = FlatSingleKeyMap.EMPTY_KEY;
    int lastSlot = -1;
    for (int doc = startDoc; doc < endDoc; doc++) {
      if (!liveDocs.get(doc)) continue;
      long key = keyValues[doc];
      if (key == lastKey) {
        flatMap.accData[lastSlot * slotsPerGroup + accOffset0]++;
      } else {
        lastSlot = flatMap.findOrInsert(key);
        flatMap.accData[lastSlot * slotsPerGroup + accOffset0]++;
        lastKey = key;
      }
    }
    return;
  }

  // No-liveDocs path: transition-batched with prefetch
  long lastKey = FlatSingleKeyMap.EMPTY_KEY;
  int lastSlot = -1;
  int doc = startDoc;
  final int BATCH = 32;
  int[] transitionDocs = new int[BATCH];

  for (; doc < endDoc; ) {
    int numTransitions = 0;
    int scanEnd = Math.min(doc + BATCH * 8, endDoc);
    int d = doc;
    // Phase 1: find key transitions (unique-key boundaries)
    for (; d < scanEnd && numTransitions < BATCH; d++) {
      long key = keyValues[d];
      if (key != lastKey) {
        transitionDocs[numTransitions++] = d;
        lastKey = key;
      }
    }
    if (numTransitions == 0) {
      if (lastSlot >= 0) flatMap.accData[lastSlot * slotsPerGroup + accOffset0] += (d - doc);
      doc = d;
      continue;
    }
    // Count docs before first transition
    if (lastSlot >= 0 && transitionDocs[0] > doc) {
      flatMap.accData[lastSlot * slotsPerGroup + accOffset0] += (transitionDocs[0] - doc);
    }
    // Prefetch target slots
    long[] mapKeys = flatMap.keys;
    long[] mapAcc = flatMap.accData;
    int mask = flatMap.capacity - 1;
    long prefetchSink = 0;
    for (int t = 0; t < numTransitions; t++) {
      int slot = SingleKeyHashMap.hash1(keyValues[transitionDocs[t]]) & mask;
      prefetchSink += mapKeys[slot];
      prefetchSink += mapAcc[slot * slotsPerGroup];
    }
    // Phase 2: probe and accumulate run lengths
    for (int t = 0; t < numTransitions; t++) {
      int tDoc = transitionDocs[t];
      int nextDoc = (t + 1 < numTransitions) ? transitionDocs[t + 1] : d;
      int runLen = nextDoc - tDoc;
      lastSlot = flatMap.findOrInsert(keyValues[tDoc]);
      flatMap.accData[lastSlot * slotsPerGroup + accOffset0] += runLen;
    }
    lastKey = keyValues[transitionDocs[numTransitions - 1]];
    if (prefetchSink == Long.MIN_VALUE) System.nanoTime();
    doc = d;
  }
}
```

## 4. Constants & Infrastructure

```java
// Line 118-130
private static final String PARALLELISM_MODE = System.getProperty("dqe.parallelism", "docrange");
static final int THREADS_PER_SHARD =
    Math.max(1, Runtime.getRuntime().availableProcessors()
        / Integer.getInteger("dqe.numLocalShards", 4));

private static final ForkJoinPool PARALLEL_POOL =
    new ForkJoinPool(
        Runtime.getRuntime().availableProcessors(),
        ForkJoinPool.defaultForkJoinWorkerThreadFactory,
        null, true); // asyncMode=true for work-stealing
```

## 5. Key Observations for Hash-Partitioned Approach

**Current bottleneck (merge):**
- Each worker builds a full FlatSingleKeyMap with up to 2.5M entries
- Map memory: `capacity * 8` (keys) + `capacity * slotsPerGroup * 8` (accData)
- With 2.5M entries at 70% load → capacity ~4M → keys=32MB + accData=32MB = ~64MB per worker
- `mergeFrom` iterates all `other.capacity` slots, doing hash probes into target
- With N workers, merge does (N-1) full passes over ~4M-slot maps

**What hash-partitioning would change:**
- Instead of each worker seeing all keys, partition keys by `hash(key) % N`
- Worker i only inserts keys where `hash(key) % N == i`
- Each worker's map has ~2.5M/N entries → much smaller, fits in cache
- No merge needed — each partition is authoritative for its key range
- Final step: concatenate partition results (no hash probing)

**Existing bucket-partitioned code (line ~4870):**
- There's already a multi-bucket path (`scanSegmentFlatSingleKeyMultiBucket`) that distributes keys across `numBuckets` maps
- But it's used for a different purpose (multi-bucket aggregation), not for parallelism
- The merge still happens: `bucketMaps[b].mergeFrom(workerMaps[b])` per bucket per worker
