# Extracted Code: FusedGroupByAggregate.java — Flat Single-Key Path

Source: `dqe/src/main/java/org/opensearch/sql/dqe/shard/source/FusedGroupByAggregate.java`

---

## 1. executeSingleKeyNumericFlat (L5143–L5476)

```java
// L5143
  private static List<Page> executeSingleKeyNumericFlat(
      IndexShard shard, Query query, List<KeyInfo> keyInfos, List<AggSpec> specs,
      Map<String, Type> columnTypeMap, List<String> groupByKeys, int numAggs,
      boolean[] isCountStar, int[] accType, int sortAggIndex, boolean sortAscending,
      long topN, int bucket, int numBuckets) throws Exception {

    // L5159 — Compute flat layout
    final int[] accOffset = new int[numAggs];
    int totalSlots = 0;
    for (int i = 0; i < numAggs; i++) {
      accOffset[i] = totalSlots;
      switch (accType[i]) {
        case 0: case 1: totalSlots += 1; break;
        case 2: totalSlots += 2; break;
        default: throw new IllegalStateException("Flat path used with unsupported accType: " + accType[i]);
      }
    }
    final int slotsPerGroup = totalSlots;
    final FlatSingleKeyMap flatMap;

    try (Engine.Searcher engineSearcher = shard.acquireSearcher("dqe-fused-groupby-numeric-1key-flat")) {
      List<LeafReaderContext> leaves = engineSearcher.getIndexReader().leaves();
      boolean isMatchAll = query instanceof MatchAllDocsQuery;
      boolean canParallelize = !"off".equals(PARALLELISM_MODE) && THREADS_PER_SHARD > 1 && leaves.size() > 1;

      if (canParallelize) {
        // L5193 — Check if doc-range parallelism applies: COUNT(*)-only, matchAll, no bucketing
        boolean allCountStar = true;
        for (int i = 0; i < numAggs; i++) { if (!isCountStar[i]) { allCountStar = false; break; } }

        if (allCountStar && isMatchAll && numBuckets <= 1) {
          // L5200 — Load columnar cache per segment
          List<long[]> segKeyArrays = new ArrayList<>();
          List<Bits> segLiveDocs = new ArrayList<>();
          long totalDocs = 0;
          for (LeafReaderContext leafCtx : leaves) {
            long[] keyValues = loadNumericColumn(leafCtx, keyInfos.get(0).name());
            segKeyArrays.add(keyValues);
            segLiveDocs.add(leafCtx.reader().getLiveDocs());
            totalDocs += keyValues.length;
          }

          // L5210 — SEQUENTIAL FALLBACK (totalDocs > 10M)
          if (totalDocs > 10_000_000) {
            flatMap = new FlatSingleKeyMap(slotsPerGroup, 8_000_000);
            for (int s = 0; s < segKeyArrays.size(); s++) {
              scanDocRangeFlatSingleKeyCountStar(
                  segKeyArrays.get(s), 0, segKeyArrays.get(s).length,
                  segLiveDocs.get(s), flatMap, accOffset[0], slotsPerGroup);
            }
          } else {
          // L5222 — PARALLEL PATH (doc-range parallel for lower-cardinality)
          flatMap = new FlatSingleKeyMap(slotsPerGroup, 4_000_000);
          int numWorkers = THREADS_PER_SHARD;
          long docsPerWorker = Math.max(1, (totalDocs + numWorkers - 1) / numWorkers);
          List<int[]> workUnits = new ArrayList<>();
          for (int s = 0; s < leaves.size(); s++) {
            int maxDoc = segKeyArrays.get(s).length;
            if (maxDoc == 0) continue;
            for (int start = 0; start < maxDoc; start += (int) docsPerWorker) {
              int end = (int) Math.min(start + docsPerWorker, maxDoc);
              workUnits.add(new int[]{s, start, end});
            }
          }
          int actualWorkers = workUnits.size();
          CompletableFuture<FlatSingleKeyMap>[] futures = new CompletableFuture[actualWorkers];
          for (int w = 0; w < actualWorkers; w++) {
            final int[] unit = workUnits.get(w);
            final long[] keyValues = segKeyArrays.get(unit[0]);
            final Bits liveDocs = segLiveDocs.get(unit[0]);
            final int startDoc = unit[1]; final int endDoc = unit[2];
            futures[w] = CompletableFuture.supplyAsync(() -> {
              int docCount = endDoc - startDoc;
              int initCap = (int) Math.min((long) docCount * 10 / 7 + 1, 4_000_000);
              FlatSingleKeyMap localMap = new FlatSingleKeyMap(slotsPerGroup, initCap);
              scanDocRangeFlatSingleKeyCountStar(keyValues, startDoc, endDoc, liveDocs, localMap, accOffset[0], slotsPerGroup);
              return localMap;
            }, PARALLEL_POOL);
          }
          CompletableFuture.allOf(futures).join();
          for (int fi = 0; fi < futures.length; fi++) {
            FlatSingleKeyMap workerMap = futures[fi].join();
            if (workerMap.size > 0) flatMap.mergeFrom(workerMap);
            futures[fi] = null;
          }
          } // end parallel path

        } else {
        // L5270 — SEGMENT-PARALLEL PATH (non-COUNT(*) or filtered)
        int mainInitCap = 4_000_000;
        flatMap = new FlatSingleKeyMap(slotsPerGroup, mainInitCap);
        int numWorkers = Math.min(THREADS_PER_SHARD, leaves.size());
        List<LeafReaderContext>[] workerSegments = new List[numWorkers];
        long[] workerDocCounts = new long[numWorkers];
        for (int i = 0; i < numWorkers; i++) workerSegments[i] = new ArrayList<>();
        List<LeafReaderContext> sortedLeaves = new ArrayList<>(leaves);
        sortedLeaves.sort((a, b) -> Integer.compare(b.reader().maxDoc(), a.reader().maxDoc()));
        for (LeafReaderContext leaf : sortedLeaves) {
          int lightest = 0;
          for (int i = 1; i < numWorkers; i++) { if (workerDocCounts[i] < workerDocCounts[lightest]) lightest = i; }
          workerSegments[lightest].add(leaf);
          workerDocCounts[lightest] += leaf.reader().maxDoc();
        }
        final Weight weight;
        if (!isMatchAll) {
          IndexSearcher luceneSearcher = new IndexSearcher(engineSearcher.getIndexReader());
          weight = luceneSearcher.createWeight(luceneSearcher.rewrite(query), ScoreMode.COMPLETE_NO_SCORES, 1.0f);
        } else { weight = null; }
        CompletableFuture<FlatSingleKeyMap>[] futures = new CompletableFuture[numWorkers];
        for (int w = 0; w < numWorkers; w++) {
          final List<LeafReaderContext> mySegments = workerSegments[w];
          final long myDocCount = workerDocCounts[w];
          futures[w] = CompletableFuture.supplyAsync(() -> {
            int initCap = (int) Math.min(myDocCount * 10 / 7 + 1, 4_000_000);
            FlatSingleKeyMap localMap = new FlatSingleKeyMap(slotsPerGroup, initCap);
            try {
              for (LeafReaderContext leafCtx : mySegments) {
                scanSegmentFlatSingleKey(leafCtx, weight, isMatchAll, localMap, keyInfos, specs,
                    numAggs, isCountStar, accType, accOffset, slotsPerGroup, bucket, numBuckets);
              }
            } catch (Exception e) { throw new RuntimeException(e); }
            return localMap;
          }, PARALLEL_POOL);
        }
        CompletableFuture.allOf(futures).join();
        for (int fi = 0; fi < futures.length; fi++) {
          FlatSingleKeyMap workerMap = futures[fi].join();
          if (workerMap.size > 0) flatMap.mergeFrom(workerMap);
          futures[fi] = null;
        }
        } // end segment-parallel

      } else {
        // L5338 — SEQUENTIAL PATH (no parallelism)
        flatMap = new FlatSingleKeyMap(slotsPerGroup, 4_000_000);
        Weight weight = null;
        if (!isMatchAll) {
          IndexSearcher luceneSearcher = new IndexSearcher(engineSearcher.getIndexReader());
          weight = luceneSearcher.createWeight(luceneSearcher.rewrite(query), ScoreMode.COMPLETE_NO_SCORES, 1.0f);
        }
        for (LeafReaderContext leafCtx : leaves) {
          scanSegmentFlatSingleKey(leafCtx, weight, isMatchAll, flatMap, keyInfos, specs,
              numAggs, isCountStar, accType, accOffset, slotsPerGroup, bucket, numBuckets);
        }
      }
    }

    // L5355 — Output page construction (top-N heap sort + block building)
    if (flatMap.size == 0) return List.of();
    // ... heap-based top-N selection and BlockBuilder output (L5355–L5476)
    // See file for full output construction code
  }
```

## 2. scanDocRangeFlatSingleKeyCountStar (L14278–L14293)

```java
// L14278
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

## 3. FlatSingleKeyMap (L13531–L13636)

```java
// L13531
  private static final class FlatSingleKeyMap {
    private static final int INITIAL_CAPACITY = 4096;
    private static final float LOAD_FACTOR = 0.7f;
    private static final int MAX_CAPACITY = 16_000_000;
    static final long EMPTY_KEY = Long.MIN_VALUE;

    long[] keys;
    long[] accData; // contiguous: slot i's data at [i*slotsPerGroup .. (i+1)*slotsPerGroup)
    int size;
    int capacity;
    int threshold;
    final int slotsPerGroup;

    // L13546 — Constructors
    FlatSingleKeyMap(int slotsPerGroup) { this(slotsPerGroup, INITIAL_CAPACITY); }
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

    // L13569 — findOrInsert
    int findOrInsert(long key) {
      int mask = capacity - 1;
      int h = SingleKeyHashMap.hash1(key) & mask;
      while (keys[h] != EMPTY_KEY) {
        if (keys[h] == key) return h;
        h = (h + 1) & mask;
      }
      keys[h] = key;
      size++;
      if (size > threshold) { resize(); return findExisting(key); }
      return h;
    }

    // L13584 — findExisting (post-resize lookup)
    private int findExisting(long key) {
      int mask = capacity - 1;
      int h = SingleKeyHashMap.hash1(key) & mask;
      while (keys[h] != EMPTY_KEY) {
        if (keys[h] == key) return h;
        h = (h + 1) & mask;
      }
      throw new IllegalStateException("Key not found after resize");
    }

    // L13593 — resize
    private void resize() {
      if (size > MAX_CAPACITY) throw new RuntimeException("GROUP BY exceeded memory limit...");
      int newCap = capacity * 2;
      long[] nk = new long[newCap]; Arrays.fill(nk, EMPTY_KEY);
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
      this.keys = nk; this.accData = nacc;
      this.capacity = newCap; this.threshold = (int) (newCap * LOAD_FACTOR);
    }

    // L13625 — mergeFrom
    void mergeFrom(FlatSingleKeyMap other) {
      for (int s = 0; s < other.capacity; s++) {
        if (other.keys[s] == EMPTY_KEY) continue;
        int slot = findOrInsert(other.keys[s]);
        int dstBase = slot * slotsPerGroup;
        int srcBase = s * other.slotsPerGroup;
        for (int j = 0; j < slotsPerGroup; j++) {
          accData[dstBase + j] += other.accData[srcBase + j];
        }
      }
    }
  }
```

## Key Decision Points

| Line | Branch | Condition |
|------|--------|-----------|
| L5189 | canParallelize | `PARALLELISM_MODE != "off" && THREADS_PER_SHARD > 1 && leaves.size() > 1` |
| L5197 | allCountStar + matchAll | `allCountStar && isMatchAll && numBuckets <= 1` |
| L5210 | **Sequential fallback** | `totalDocs > 10_000_000` |
| L5222 | Doc-range parallel | `totalDocs <= 10_000_000` (else branch) |
| L5270 | Segment-parallel | Non-COUNT(*) or filtered queries |
| L5338 | Full sequential | `!canParallelize` |
