# FusedGroupByAggregate.java - Extracted Source Code

Source: `dqe/src/main/java/org/opensearch/sql/dqe/shard/source/FusedGroupByAggregate.java`

---

## 1. FlatSingleKeyMap (line 12529)

```java
  private static final class FlatSingleKeyMap {
    private static final int INITIAL_CAPACITY = 4096;
    private static final float LOAD_FACTOR = 0.7f;
    private static final int MAX_CAPACITY = 32_000_000;

    long[] keys;
    boolean[] occupied;
    long[] accData; // contiguous: slot i's data at [i*slotsPerGroup .. (i+1)*slotsPerGroup)
    int size;
    int capacity;
    int threshold;
    final int slotsPerGroup;

    FlatSingleKeyMap(int slotsPerGroup) {
      this.slotsPerGroup = slotsPerGroup;
      this.capacity = INITIAL_CAPACITY;
      this.keys = new long[capacity];
      this.occupied = new boolean[capacity];
      this.accData = new long[capacity * slotsPerGroup];
      this.size = 0;
      this.threshold = (int) (capacity * LOAD_FACTOR);
    }

    /**
     * Find existing slot or insert new entry for the given key. Returns the slot index. The caller
     * accumulates into accData at slot*slotsPerGroup.
     */
    int findOrInsert(long key) {
      int mask = capacity - 1;
      int h = SingleKeyHashMap.hash1(key) & mask;
      while (occupied[h]) {
        if (keys[h] == key) {
          return h;
        }
        h = (h + 1) & mask;
      }
      // New entry
      keys[h] = key;
      occupied[h] = true;
      // accData[h*slotsPerGroup..] is already 0 from array init
      size++;
      if (size > threshold) {
        resize();
        return findExisting(key);
      }
      return h;
    }

    private int findExisting(long key) {
      int mask = capacity - 1;
      int h = SingleKeyHashMap.hash1(key) & mask;
      while (occupied[h]) {
        if (keys[h] == key) return h;
        h = (h + 1) & mask;
      }
      throw new IllegalStateException("Key not found after resize");
    }

    private void resize() {
      if (size > MAX_CAPACITY) {
        throw new RuntimeException(
            "GROUP BY exceeded memory limit ("
                + size
                + " unique groups, max "
                + MAX_CAPACITY
                + "). "
                + "Add a WHERE clause to reduce cardinality.");
      }
      int newCap = capacity * 2;
      long[] nk = new long[newCap];
      boolean[] nocc = new boolean[newCap];
      long[] nacc = new long[newCap * slotsPerGroup];
      int nm = newCap - 1;
      for (int s = 0; s < capacity; s++) {
        if (occupied[s]) {
          int nh = SingleKeyHashMap.hash1(keys[s]) & nm;
          while (nocc[nh]) nh = (nh + 1) & nm;
          nk[nh] = keys[s];
          nocc[nh] = true;
          System.arraycopy(accData, s * slotsPerGroup, nacc, nh * slotsPerGroup, slotsPerGroup);
        }
      }
      this.keys = nk;
      this.occupied = nocc;
      this.accData = nacc;
      this.capacity = newCap;
      this.threshold = (int) (newCap * LOAD_FACTOR);
    }

    /** Merge all entries from another FlatSingleKeyMap into this one, adding accData element-wise. */
    void mergeFrom(FlatSingleKeyMap other) {
      for (int s = 0; s < other.capacity; s++) {
        if (!other.occupied[s]) continue;
        int slot = findOrInsert(other.keys[s]);
        int dstBase = slot * slotsPerGroup;
        int srcBase = s * other.slotsPerGroup;
        for (int j = 0; j < slotsPerGroup; j++) {
          accData[dstBase + j] += other.accData[srcBase + j];
        }
      }
    }
  }

  // =========================================================================
  // Accumulator infrastructure
  // =========================================================================

  /** Mergeable accumulator for cross-segment aggregation. */
  private interface MergeableAccumulator {
    void merge(MergeableAccumulator other);

    void writeTo(BlockBuilder builder);

    /**
     * Return a long value suitable for top-N sorting. For COUNT/SUM this is the accumulated value.
     * For AVG this is the count (as a proxy for significance). Default returns 0.
     */
    default long getSortValue() {
      return 0;
    }
  }

```

---

## 2. executeTwoKeyNumeric (line 5139)

```java
  private static List<Page> executeTwoKeyNumeric(
      IndexShard shard,
      Query query,
      List<KeyInfo> keyInfos,
      List<AggSpec> specs,
      Map<String, Type> columnTypeMap,
      List<String> groupByKeys,
      int sortAggIndex,
      boolean sortAscending,
      long topN)
      throws Exception {

    // Pre-compute per-aggregate dispatch flags
    final int numAggs = specs.size();
    final boolean[] isCountStar = new boolean[numAggs];
    final boolean[] isDoubleArg = new boolean[numAggs];
    // Accumulator type: 0=CountStar, 1=SumLong, 2=AvgLong, 3=Min, 4=Max, 5=CountDistinct,
    //                    6=SumDouble, 7=AvgDouble
    final int[] accType = new int[numAggs];
    boolean canUseFlatAccumulators = true;

    for (int i = 0; i < numAggs; i++) {
      AggSpec spec = specs.get(i);
      isCountStar[i] = "*".equals(spec.arg);
      isDoubleArg[i] = spec.argType instanceof DoubleType;
      if (isCountStar[i]) {
        accType[i] = 0;
      } else {
        switch (spec.funcName) {
          case "COUNT":
            if (spec.isDistinct) {
              accType[i] = 5;
              canUseFlatAccumulators = false;
            } else {
              accType[i] = 0;
            }
            break;
          case "SUM":
            if (isDoubleArg[i]) {
              accType[i] = 6;
              canUseFlatAccumulators = false;
            } else {
              accType[i] = 1;
            }
            break;
          case "AVG":
            if (isDoubleArg[i]) {
              accType[i] = 7;
              canUseFlatAccumulators = false;
            } else {
              accType[i] = 2;
            }
            break;
          case "MIN":
            accType[i] = 3;
            canUseFlatAccumulators = false;
            break;
          case "MAX":
            accType[i] = 4;
            canUseFlatAccumulators = false;
            break;
        }
        // VARCHAR args can't use flat accumulators
        if (spec.argType instanceof VarcharType) {
          canUseFlatAccumulators = false;
        }
      }
    }

    // === Flat accumulator path for two-key numeric ===
    if (canUseFlatAccumulators) {
      // Estimate number of docs to decide if hash-partitioned aggregation is needed.
      // Use totalDocs/4 as heuristic for unique group count (assumes ≥4x duplication).
      int numBuckets;
      try (org.opensearch.index.engine.Engine.Searcher estSearcher =
          shard.acquireSearcher("dqe-fused-groupby-estimate")) {
        long totalDocs = 0;
        for (LeafReaderContext leaf : estSearcher.getIndexReader().leaves()) {
          totalDocs += leaf.reader().maxDoc();
        }
        long estimatedGroups = Math.max(1, totalDocs / 4);
        numBuckets = Math.max(1, (int) Math.ceil((double) estimatedGroups / FlatTwoKeyMap.MAX_CAPACITY));
      }

      if (numBuckets <= 1) {
        return executeTwoKeyNumericFlat(
            shard,
            query,
            keyInfos,
            specs,
            columnTypeMap,
            groupByKeys,
            numAggs,
            isCountStar,
            accType,
            sortAggIndex,
            sortAscending,
            topN,
            0,
            1);
      } else {
        // Multi-bucket partitioned aggregation: each pass aggregates only groups
        // whose hash falls into the current bucket, keeping map size under MAX_CAPACITY.
        // Parallelize across buckets when possible.
        int parallelBuckets = Math.min(numBuckets, THREADS_PER_SHARD);
        if (parallelBuckets > 1 && !"off".equals(PARALLELISM_MODE)) {
          @SuppressWarnings("unchecked")
          java.util.concurrent.CompletableFuture<List<Page>>[] futures =
              new java.util.concurrent.CompletableFuture[numBuckets];
          for (int b = 0; b < numBuckets; b++) {
            final int bkt = b;
            futures[b] =
                java.util.concurrent.CompletableFuture.supplyAsync(
                    () -> {
                      try {
                        return executeTwoKeyNumericFlat(
                            shard, query, keyInfos, specs, columnTypeMap, groupByKeys,
                            numAggs, isCountStar, accType, sortAggIndex, sortAscending,
                            topN, bkt, numBuckets);
                      } catch (Exception e) {
                        throw new RuntimeException(e);
                      }
                    },
                    PARALLEL_POOL);
          }
          java.util.concurrent.CompletableFuture.allOf(futures).join();
          List<Page> allBucketResults = new ArrayList<>();
          for (var future : futures) {
            allBucketResults.addAll(future.join());
          }
          if (allBucketResults.isEmpty()) return List.of();
          if (allBucketResults.size() == 1) return allBucketResults;
          return mergePartitionedPages(
              allBucketResults, keyInfos, specs, columnTypeMap, groupByKeys,
              numAggs, accType, sortAggIndex, sortAscending, topN);
        }
        // Sequential fallback
        List<Page> allBucketResults = new ArrayList<>();
        for (int bucket = 0; bucket < numBuckets; bucket++) {
          List<Page> bucketPages =
              executeTwoKeyNumericFlat(
                  shard,
                  query,
                  keyInfos,
                  specs,
                  columnTypeMap,
                  groupByKeys,
                  numAggs,
                  isCountStar,
                  accType,
                  sortAggIndex,
                  sortAscending,
                  topN,
                  bucket,
                  numBuckets);
          allBucketResults.addAll(bucketPages);
        }
        if (allBucketResults.isEmpty()) return List.of();
        if (allBucketResults.size() == 1) return allBucketResults;
        return mergePartitionedPages(
            allBucketResults,
            keyInfos,
            specs,
            columnTypeMap,
            groupByKeys,
            numAggs,
            accType,
            sortAggIndex,
            sortAscending,
            topN);
      }
    }

    // Use TwoKeyHashMap with AccumulatorGroup objects and pre-computed dispatch
    final TwoKeyHashMap twoKeyMap = new TwoKeyHashMap(specs);

    try (org.opensearch.index.engine.Engine.Searcher engineSearcher =
        shard.acquireSearcher("dqe-fused-groupby-numeric-2key")) {

      if (query instanceof MatchAllDocsQuery) {
        // MatchAll: use Collector path
        engineSearcher.search(
            query,
            new Collector() {
              @Override
              public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
                SortedNumericDocValues dv0 =
                    context.reader().getSortedNumericDocValues(keyInfos.get(0).name());
                SortedNumericDocValues dv1 =
                    context.reader().getSortedNumericDocValues(keyInfos.get(1).name());
                final SortedNumericDocValues[] numericAggDvs = new SortedNumericDocValues[numAggs];
                for (int i = 0; i < numAggs; i++) {
                  if (!isCountStar[i]) {
                    AggSpec spec = specs.get(i);
                    if (!(spec.argType instanceof VarcharType)) {
                      numericAggDvs[i] = context.reader().getSortedNumericDocValues(spec.arg);
                    }
                  }
                }
                return new LeafCollector() {
                  @Override
                  public void setScorer(Scorable scorer) {}

                  @Override
                  public void collect(int doc) throws IOException {
                    long key0 = 0, key1 = 0;
                    if (dv0 != null && dv0.advanceExact(doc)) key0 = dv0.nextValue();
                    if (dv1 != null && dv1.advanceExact(doc)) key1 = dv1.nextValue();
                    AccumulatorGroup accGroup = twoKeyMap.getOrCreate(key0, key1);
                    accumulateDocNumeric(
                        doc, accGroup, numAggs, isCountStar, isDoubleArg, accType, numericAggDvs);
                  }
                };
              }

              @Override
              public ScoreMode scoreMode() {
                return ScoreMode.COMPLETE_NO_SCORES;
              }
            });
      } else {
        // Filtered path: manual Weight+Scorer loop to skip empty segments early
        IndexSearcher luceneSearcher = new IndexSearcher(engineSearcher.getIndexReader());
        Weight weight =
            luceneSearcher.createWeight(
                luceneSearcher.rewrite(query), ScoreMode.COMPLETE_NO_SCORES, 1.0f);

        for (LeafReaderContext leafCtx : engineSearcher.getIndexReader().leaves()) {
          Scorer scorer = weight.scorer(leafCtx);
          if (scorer == null) continue;

          LeafReader reader = leafCtx.reader();
          SortedNumericDocValues dv0 = reader.getSortedNumericDocValues(keyInfos.get(0).name());
          SortedNumericDocValues dv1 = reader.getSortedNumericDocValues(keyInfos.get(1).name());
          final SortedNumericDocValues[] numericAggDvs = new SortedNumericDocValues[numAggs];
          for (int i = 0; i < numAggs; i++) {
            if (!isCountStar[i]) {
              AggSpec spec = specs.get(i);
              if (!(spec.argType instanceof VarcharType)) {
                numericAggDvs[i] = reader.getSortedNumericDocValues(spec.arg);
              }
            }
          }

          DocIdSetIterator docIt = scorer.iterator();
          int doc;
          while ((doc = docIt.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
            long key0 = 0, key1 = 0;
            if (dv0 != null && dv0.advanceExact(doc)) key0 = dv0.nextValue();
            if (dv1 != null && dv1.advanceExact(doc)) key1 = dv1.nextValue();
            AccumulatorGroup accGroup = twoKeyMap.getOrCreate(key0, key1);
            accumulateDocNumeric(
                doc, accGroup, numAggs, isCountStar, isDoubleArg, accType, numericAggDvs);
          }
        }
      }
    }

    if (twoKeyMap.size == 0) return List.of();

    int numGroupKeys = groupByKeys.size();
    int totalColumns = numGroupKeys + numAggs;

    // Determine which slots to output (with optional top-N selection)
    int[] outputSlots;
    int outputCount;
    if (sortAggIndex >= 0 && topN > 0 && topN < twoKeyMap.size) {
      // Top-N using MergeableAccumulator.getSortValue() on the sort aggregate
      int n = (int) Math.min(topN, twoKeyMap.size);
      int[] heap = new int[n];
      int heapSize = 0;
      for (int slot = 0; slot < twoKeyMap.capacity; slot++) {
        if (!twoKeyMap.occupied[slot]) continue;
        long val = twoKeyMap.groups[slot].accumulators[sortAggIndex].getSortValue();
        if (heapSize < n) {
          heap[heapSize++] = slot;
          int k = heapSize - 1;
          while (k > 0) {
            int parent = (k - 1) >>> 1;
            long pVal = twoKeyMap.groups[heap[parent]].accumulators[sortAggIndex].getSortValue();
            long kVal = twoKeyMap.groups[heap[k]].accumulators[sortAggIndex].getSortValue();
            boolean swap = sortAscending ? (kVal > pVal) : (kVal < pVal);
            if (swap) {
              int tmp = heap[parent];
              heap[parent] = heap[k];
              heap[k] = tmp;
              k = parent;
            } else break;
          }
        } else {
          long rootVal = twoKeyMap.groups[heap[0]].accumulators[sortAggIndex].getSortValue();
          boolean better = sortAscending ? (val < rootVal) : (val > rootVal);
          if (better) {
            heap[0] = slot;
            int k = 0;
            while (true) {
              int left = 2 * k + 1;
              if (left >= heapSize) break;
              int right = left + 1;
              int target = left;
              if (right < heapSize) {
                long lv = twoKeyMap.groups[heap[left]].accumulators[sortAggIndex].getSortValue();
                long rv = twoKeyMap.groups[heap[right]].accumulators[sortAggIndex].getSortValue();
                boolean pickRight = sortAscending ? (rv > lv) : (rv < lv);
                if (pickRight) target = right;
              }
              long kVal = twoKeyMap.groups[heap[k]].accumulators[sortAggIndex].getSortValue();
              long tVal = twoKeyMap.groups[heap[target]].accumulators[sortAggIndex].getSortValue();
              boolean swap = sortAscending ? (tVal > kVal) : (tVal < kVal);
              if (swap) {
                int tmp = heap[k];
                heap[k] = heap[target];
                heap[target] = tmp;
                k = target;
              } else break;
            }
          }
        }
      }
      outputSlots = heap;
      outputCount = heapSize;
    } else if (topN > 0 && topN < twoKeyMap.size) {
      int n = (int) Math.min(topN, twoKeyMap.size);
      outputSlots = new int[n];
      int idx = 0;
      for (int slot = 0; slot < twoKeyMap.capacity && idx < n; slot++) {
        if (twoKeyMap.occupied[slot]) outputSlots[idx++] = slot;
      }
      outputCount = idx;
    } else {
      outputSlots = new int[twoKeyMap.size];
      int idx = 0;
      for (int slot = 0; slot < twoKeyMap.capacity; slot++) {
        if (twoKeyMap.occupied[slot]) outputSlots[idx++] = slot;
      }
      outputCount = idx;
    }

    BlockBuilder[] builders = new BlockBuilder[totalColumns];
    for (int i = 0; i < numGroupKeys; i++) {
      builders[i] = keyInfos.get(i).type.createBlockBuilder(null, outputCount);
    }
    for (int i = 0; i < numAggs; i++) {
      builders[numGroupKeys + i] =
          resolveAggOutputType(specs.get(i), columnTypeMap).createBlockBuilder(null, outputCount);
    }
    for (int o = 0; o < outputCount; o++) {
      int slot = outputSlots[o];
      writeNumericKeyValue(builders[0], keyInfos.get(0), twoKeyMap.keys0[slot]);
      writeNumericKeyValue(builders[1], keyInfos.get(1), twoKeyMap.keys1[slot]);
      AccumulatorGroup accGroup = twoKeyMap.groups[slot];
      for (int a = 0; a < numAggs; a++) {
        accGroup.accumulators[a].writeTo(builders[numGroupKeys + a]);
      }
    }
    Block[] blocks = new Block[totalColumns];
    for (int i = 0; i < totalColumns; i++) blocks[i] = builders[i].build();
    return List.of(new Page(blocks));
  }

  /**
   * Flat accumulator path for two-key numeric GROUP BY. Same concept as {@link
```

---

## 3. TwoKeyHashMap (line 12023)

```java
  private static final class TwoKeyHashMap {
    private static final int INITIAL_CAPACITY = 8192;
    private static final float LOAD_FACTOR = 0.7f;
    private static final int MAX_CAPACITY = 32_000_000;

    long[] keys0;
    long[] keys1;
    AccumulatorGroup[] groups;
    boolean[] occupied;
    int size;
    int capacity;
    private int threshold;
    private final List<AggSpec> specs;

    @SuppressWarnings("unchecked")
    TwoKeyHashMap(List<AggSpec> specs) {
      this.specs = specs;
      this.capacity = INITIAL_CAPACITY;
      this.keys0 = new long[capacity];
      this.keys1 = new long[capacity];
      this.groups = new AccumulatorGroup[capacity];
      this.occupied = new boolean[capacity];
      this.size = 0;
      this.threshold = (int) (capacity * LOAD_FACTOR);
    }

    AccumulatorGroup getOrCreate(long key0, long key1) {
      int mask = capacity - 1;
      int slot = hash2(key0, key1) & mask;
      while (occupied[slot]) {
        if (keys0[slot] == key0 && keys1[slot] == key1) {
          return groups[slot];
        }
        slot = (slot + 1) & mask;
      }
      // New entry
      AccumulatorGroup accGroup = createAccumulatorGroup(specs);
      keys0[slot] = key0;
      keys1[slot] = key1;
      groups[slot] = accGroup;
      occupied[slot] = true;
      size++;
      if (size > threshold) {
        resize();
        // After resize, return from the new slot location
        return getExisting(key0, key1);
      }
      return accGroup;
    }

    private AccumulatorGroup getExisting(long key0, long key1) {
      int mask = capacity - 1;
      int slot = hash2(key0, key1) & mask;
      while (occupied[slot]) {
        if (keys0[slot] == key0 && keys1[slot] == key1) {
          return groups[slot];
        }
        slot = (slot + 1) & mask;
      }
      throw new IllegalStateException("Key not found after resize");
    }

    private void resize() {
      if (size > MAX_CAPACITY) {
        throw new RuntimeException(
            "GROUP BY exceeded memory limit ("
                + size
                + " unique groups, max "
                + MAX_CAPACITY
                + "). "
                + "Add a WHERE clause to reduce cardinality.");
      }
      int newCapacity = capacity * 2;
      long[] newKeys0 = new long[newCapacity];
      long[] newKeys1 = new long[newCapacity];
      AccumulatorGroup[] newGroups = new AccumulatorGroup[newCapacity];
      boolean[] newOccupied = new boolean[newCapacity];
      int newMask = newCapacity - 1;

      for (int i = 0; i < capacity; i++) {
        if (occupied[i]) {
          int slot = hash2(keys0[i], keys1[i]) & newMask;
          while (newOccupied[slot]) {
            slot = (slot + 1) & newMask;
          }
          newKeys0[slot] = keys0[i];
          newKeys1[slot] = keys1[i];
          newGroups[slot] = groups[i];
          newOccupied[slot] = true;
        }
      }

      this.keys0 = newKeys0;
      this.keys1 = newKeys1;
      this.groups = newGroups;
      this.occupied = newOccupied;
      this.capacity = newCapacity;
      this.threshold = (int) (newCapacity * LOAD_FACTOR);
    }

    /** Combine two long keys into a single hash using Murmur3-inspired mixing. */
    private static int hash2(long k0, long k1) {
      long h = k0 * 0x9E3779B97F4A7C15L + k1;
      h ^= h >>> 33;
      h *= 0xff51afd7ed558ccdL;
      h ^= h >>> 33;
      return (int) h;
    }
  }

  /**
   * Open-addressing hash map with a single primitive long key. Uses linear probing with
   * power-of-two capacity for fast modulo via bitmask. Stores keys in a long array and
   * AccumulatorGroups in a parallel array, eliminating HashMap.Entry, SegmentGroupKey, and
   * NumericProbeKey allocation per group. Hash function uses Murmur3 finalizer for good
   * distribution of sequential integer keys (e.g., RegionID).
```

---

## 4. scanSegmentFlatSingleKey (line 4803)

```java
  private static void scanSegmentFlatSingleKey(
      LeafReaderContext leafCtx,
      Weight weight,
      boolean isMatchAll,
      FlatSingleKeyMap flatMap,
      List<KeyInfo> keyInfos,
      List<AggSpec> specs,
      int numAggs,
      boolean[] isCountStar,
      int[] accType,
      int[] accOffset,
      int slotsPerGroup,
      int bucket,
      int numBuckets)
      throws Exception {
    LeafReader reader = leafCtx.reader();
    SortedNumericDocValues dv0 = reader.getSortedNumericDocValues(keyInfos.get(0).name());

    final SortedNumericDocValues[] numericAggDvs = new SortedNumericDocValues[numAggs];
    final SortedSetDocValues[] lengthVarcharDvs = new SortedSetDocValues[numAggs];
    final int[][] ordLengthMaps = new int[numAggs][];
    final boolean[] aggApplyLength = new boolean[numAggs];
    for (int i = 0; i < numAggs; i++) {
      if (!isCountStar[i]) {
        if (specs.get(i).applyLength) {
          SortedSetDocValues varcharDv = reader.getSortedSetDocValues(specs.get(i).arg);
          lengthVarcharDvs[i] = varcharDv;
          aggApplyLength[i] = true;
          if (varcharDv != null) {
            int ordCount = (int) Math.min(varcharDv.getValueCount(), 10_000_000);
            int[] ordLengths = new int[ordCount];
            for (int ord = 0; ord < ordCount; ord++) {
              ordLengths[ord] = varcharDv.lookupOrd(ord).length;
            }
            ordLengthMaps[i] = ordLengths;
          }
        } else {
          numericAggDvs[i] = reader.getSortedNumericDocValues(specs.get(i).arg);
        }
      }
    }

    if (isMatchAll) {
      int maxDoc = reader.maxDoc();
      Bits liveDocs = reader.getLiveDocs();

      if (liveDocs == null && dv0 != null) {
        boolean allCountStar = true;
        boolean hasApplyLen = false;
        for (int i = 0; i < numAggs; i++) {
          if (!isCountStar[i]) allCountStar = false;
          if (aggApplyLength[i]) hasApplyLen = true;
        }

        if (allCountStar) {
          // Ultra-fast path: only COUNT(*), use nextDoc() on key column
          if (numBuckets <= 1) {
            // Columnar cache path: load key column into flat array for faster sequential access
            long[] keyValues = loadNumericColumn(leafCtx, keyInfos.get(0).name());
            for (int doc = 0; doc < maxDoc; doc++) {
              long key0 = keyValues[doc];
              int slot = flatMap.findOrInsert(key0);
              flatMap.accData[slot * slotsPerGroup + accOffset[0]]++;
            }
          } else {
            int doc = dv0.nextDoc();
            while (doc != DocIdSetIterator.NO_MORE_DOCS) {
              long key0 = dv0.nextValue();
              if ((SingleKeyHashMap.hash1(key0) & 0x7FFFFFFF) % numBuckets == bucket) {
                int slot = flatMap.findOrInsert(key0);
                flatMap.accData[slot * slotsPerGroup + accOffset[0]]++;
              }
              doc = dv0.nextDoc();
            }
          }
        } else if (!hasApplyLen) {
          // Fast path: numeric aggs, use nextDoc() lockstep iteration.
          boolean allAggDense = true;
          for (int i = 0; i < numAggs; i++) {
            if (!isCountStar[i] && numericAggDvs[i] == null) {
              allAggDense = false;
              break;
            }
          }
          if (allAggDense) {
            int[] aggDocPos = new int[numAggs];
            for (int i = 0; i < numAggs; i++) {
              if (!isCountStar[i]) {
                aggDocPos[i] = numericAggDvs[i].nextDoc();
              }
            }
            int keyDoc = dv0.nextDoc();
            for (int doc = 0; doc < maxDoc; doc++) {
              long key0;
              if (keyDoc == doc) {
                key0 = dv0.nextValue();
                keyDoc = dv0.nextDoc();
              } else {
                key0 = 0;
              }
              // Hash-partition filter: skip docs not in this bucket
              if (numBuckets > 1
                  && (SingleKeyHashMap.hash1(key0) & 0x7FFFFFFF) % numBuckets != bucket) {
                // Still need to advance agg DocValues iterators to stay in sync
                for (int i = 0; i < numAggs; i++) {
                  if (!isCountStar[i] && aggDocPos[i] == doc) {
                    numericAggDvs[i].nextValue();
                    aggDocPos[i] = numericAggDvs[i].nextDoc();
                  }
                }
                continue;
              }
              int slot = flatMap.findOrInsert(key0);
              int base = slot * slotsPerGroup;
              for (int i = 0; i < numAggs; i++) {
                int off = base + accOffset[i];
                if (isCountStar[i]) {
                  flatMap.accData[off]++;
                  continue;
                }
                if (aggDocPos[i] == doc) {
                  long rawVal = numericAggDvs[i].nextValue();
                  aggDocPos[i] = numericAggDvs[i].nextDoc();
                  switch (accType[i]) {
                    case 0:
                      flatMap.accData[off]++;
                      break;
                    case 1:
                      flatMap.accData[off] += rawVal;
                      break;
                    case 2:
                      flatMap.accData[off] += rawVal;
                      flatMap.accData[off + 1]++;
                      break;
                  }
                }
              }
            }
          } else {
            for (int doc = 0; doc < maxDoc; doc++) {
              collectFlatSingleKeyDocWithLength(
                  doc, dv0, flatMap, slotsPerGroup, numAggs, isCountStar,
                  accType, accOffset, numericAggDvs, lengthVarcharDvs,
                  ordLengthMaps, aggApplyLength, bucket, numBuckets);
            }
          }
        } else {
          // hasApplyLen path: check if we can use sequential nextDoc() lockstep
          boolean allAggDense = true;
          for (int i = 0; i < numAggs; i++) {
            if (!isCountStar[i]) {
              if (aggApplyLength[i] && lengthVarcharDvs[i] == null) { allAggDense = false; break; }
              if (!aggApplyLength[i] && numericAggDvs[i] == null) { allAggDense = false; break; }
            }
          }
          if (allAggDense && numBuckets <= 1) {
            // Sequential lockstep for MatchAll + LENGTH aggs
            int keyDoc = dv0.nextDoc();
            int[] aggDocPos = new int[numAggs];
            int[] lenDocPos = new int[numAggs];
            for (int i = 0; i < numAggs; i++) {
              if (isCountStar[i]) continue;
              if (aggApplyLength[i]) {
                lenDocPos[i] = lengthVarcharDvs[i].nextDoc();
              } else {
                aggDocPos[i] = numericAggDvs[i].nextDoc();
              }
            }
            for (int doc = 0; doc < maxDoc; doc++) {
              long key0 = 0;
              if (keyDoc == doc) {
                key0 = dv0.nextValue();
                keyDoc = dv0.nextDoc();
              }
              int slot = flatMap.findOrInsert(key0);
              int base = slot * slotsPerGroup;
              for (int i = 0; i < numAggs; i++) {
                int off = base + accOffset[i];
                if (isCountStar[i]) {
                  flatMap.accData[off]++;
                  continue;
                }
                if (aggApplyLength[i]) {
                  if (lenDocPos[i] == doc) {
                    int ord = (int) lengthVarcharDvs[i].nextOrd();
                    long rawVal = (ordLengthMaps[i] != null && ord < ordLengthMaps[i].length)
                        ? ordLengthMaps[i][ord]
                        : lengthVarcharDvs[i].lookupOrd(ord).length;
                    lenDocPos[i] = lengthVarcharDvs[i].nextDoc();
                    switch (accType[i]) {
                      case 0: flatMap.accData[off]++; break;
                      case 1: flatMap.accData[off] += rawVal; break;
                      case 2: flatMap.accData[off] += rawVal; flatMap.accData[off + 1]++; break;
                    }
                  }
                } else {
                  if (aggDocPos[i] == doc) {
                    long rawVal = numericAggDvs[i].nextValue();
                    aggDocPos[i] = numericAggDvs[i].nextDoc();
                    switch (accType[i]) {
                      case 0: flatMap.accData[off]++; break;
                      case 1: flatMap.accData[off] += rawVal; break;
                      case 2: flatMap.accData[off] += rawVal; flatMap.accData[off + 1]++; break;
                    }
                  }
                }
              }
            }
          } else {
            for (int doc = 0; doc < maxDoc; doc++) {
              collectFlatSingleKeyDocWithLength(
                  doc, dv0, flatMap, slotsPerGroup, numAggs, isCountStar,
                  accType, accOffset, numericAggDvs, lengthVarcharDvs,
                  ordLengthMaps, aggApplyLength, bucket, numBuckets);
            }
          }
        }
      } else if (liveDocs == null) {
        for (int doc = 0; doc < maxDoc; doc++) {
          collectFlatSingleKeyDocWithLength(
              doc, dv0, flatMap, slotsPerGroup, numAggs, isCountStar,
              accType, accOffset, numericAggDvs, lengthVarcharDvs,
              ordLengthMaps, aggApplyLength, bucket, numBuckets);
        }
      } else {
        for (int doc = 0; doc < maxDoc; doc++) {
          if (liveDocs.get(doc)) {
            collectFlatSingleKeyDocWithLength(
                doc, dv0, flatMap, slotsPerGroup, numAggs, isCountStar,
                accType, accOffset, numericAggDvs, lengthVarcharDvs,
                ordLengthMaps, aggApplyLength, bucket, numBuckets);
          }
        }
      }
    } else {
      // Filtered path
      Scorer scorer = weight.scorer(leafCtx);
      if (scorer == null) return;

      // Check selectivity: if filter matches <50% of docs, use bitset+nextDoc lockstep
      int maxDoc = reader.maxDoc();
      int estCount = weight.count(leafCtx);
      boolean hasApplyLen = false;
      for (int i = 0; i < numAggs; i++) {
        if (aggApplyLength[i]) { hasApplyLen = true; break; }
      }
      boolean useBitsetLockstep = false; // Disabled: needs more testing

      if (useBitsetLockstep) {
        // Collect matching doc IDs into bitset
        FixedBitSet matchingDocs = new FixedBitSet(maxDoc);
        DocIdSetIterator disi = scorer.iterator();
        for (int d = disi.nextDoc(); d != DocIdSetIterator.NO_MORE_DOCS; d = disi.nextDoc()) {
          matchingDocs.set(d);
        }
        boolean allAggDense = true;
        for (int i = 0; i < numAggs; i++) {
          if (!isCountStar[i] && numericAggDvs[i] == null) { allAggDense = false; break; }
        }
        if (allAggDense) {
          int keyDoc = dv0.nextDoc();
          int[] aggDocPos = new int[numAggs];
          for (int i = 0; i < numAggs; i++) {
            aggDocPos[i] = isCountStar[i] ? DocIdSetIterator.NO_MORE_DOCS : numericAggDvs[i].nextDoc();
          }
          for (int doc = matchingDocs.nextSetBit(0); doc >= 0;
              doc = matchingDocs.nextSetBit(doc + 1)) {
            while (keyDoc != DocIdSetIterator.NO_MORE_DOCS && keyDoc < doc) { keyDoc = dv0.nextDoc(); }
            long key0 = 0;
            if (keyDoc == doc) { key0 = dv0.nextValue(); keyDoc = dv0.nextDoc(); }
            if (numBuckets > 1
                && (SingleKeyHashMap.hash1(key0) & 0x7FFFFFFF) % numBuckets != bucket) {
              continue;
            }
            int slot = flatMap.findOrInsert(key0);
            int base = slot * slotsPerGroup;
            for (int i = 0; i < numAggs; i++) {
              int off = base + accOffset[i];
              if (isCountStar[i]) { flatMap.accData[off]++; continue; }
              while (aggDocPos[i] != DocIdSetIterator.NO_MORE_DOCS && aggDocPos[i] < doc) {
                aggDocPos[i] = numericAggDvs[i].nextDoc();
              }
              if (aggDocPos[i] == doc) {
                long rawVal = numericAggDvs[i].nextValue();
                aggDocPos[i] = numericAggDvs[i].nextDoc();
                switch (accType[i]) {
                  case 0: flatMap.accData[off]++; break;
                  case 1: flatMap.accData[off] += rawVal; break;
                  case 2: flatMap.accData[off] += rawVal; flatMap.accData[off + 1]++; break;
                }
              }
            }
          }
        } else {
          for (int doc = matchingDocs.nextSetBit(0); doc >= 0;
              doc = matchingDocs.nextSetBit(doc + 1)) {
            collectFlatSingleKeyDocWithLength(
                doc, dv0, flatMap, slotsPerGroup, numAggs, isCountStar,
                accType, accOffset, numericAggDvs, lengthVarcharDvs,
                ordLengthMaps, aggApplyLength, bucket, numBuckets);
          }
        }
      } else {
        DocIdSetIterator docIt = scorer.iterator();
        int doc;
        while ((doc = docIt.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
          collectFlatSingleKeyDocWithLength(
              doc, dv0, flatMap, slotsPerGroup, numAggs, isCountStar,
              accType, accOffset, numericAggDvs, lengthVarcharDvs,
              ordLengthMaps, aggApplyLength, bucket, numBuckets);
        }
      }
    }
  }
```

---

## 5. executeSingleKeyNumericFlat (line 4443)

```java
  private static List<Page> executeSingleKeyNumericFlat(
      IndexShard shard,
      Query query,
      List<KeyInfo> keyInfos,
      List<AggSpec> specs,
      Map<String, Type> columnTypeMap,
      List<String> groupByKeys,
      int numAggs,
      boolean[] isCountStar,
      int[] accType,
      int sortAggIndex,
      boolean sortAscending,
      long topN,
      int bucket,
      int numBuckets)
      throws Exception {

    // Compute flat layout: offset and slots per aggregate
    final int[] accOffset = new int[numAggs];
    int totalSlots = 0;
    for (int i = 0; i < numAggs; i++) {
      accOffset[i] = totalSlots;
      switch (accType[i]) {
        case 0: // COUNT
        case 1: // SUM long
          totalSlots += 1;
          break;
        case 2: // AVG long (sum + count)
          totalSlots += 2;
          break;
        default:
          throw new IllegalStateException("Flat path used with unsupported accType: " + accType[i]);
      }
    }

    final int slotsPerGroup = totalSlots;
    final FlatSingleKeyMap flatMap = new FlatSingleKeyMap(slotsPerGroup);

    try (org.opensearch.index.engine.Engine.Searcher engineSearcher =
        shard.acquireSearcher("dqe-fused-groupby-numeric-1key-flat")) {

      java.util.List<LeafReaderContext> leaves = engineSearcher.getIndexReader().leaves();
      boolean isMatchAll = query instanceof MatchAllDocsQuery;
      boolean canParallelize =
          !"off".equals(PARALLELISM_MODE) && THREADS_PER_SHARD > 1 && leaves.size() > 1;

      if (canParallelize) {
        // Parallel path: partition segments across workers, each with own FlatSingleKeyMap
        int numWorkers = Math.min(THREADS_PER_SHARD, leaves.size());
        @SuppressWarnings("unchecked")
        java.util.List<LeafReaderContext>[] workerSegments = new java.util.List[numWorkers];
        long[] workerDocCounts = new long[numWorkers];
        for (int i = 0; i < numWorkers; i++)
          workerSegments[i] = new java.util.ArrayList<>();
        // Largest-first greedy assignment for balanced load
        java.util.List<LeafReaderContext> sortedLeaves = new java.util.ArrayList<>(leaves);
        sortedLeaves.sort(
            (a, b) -> Integer.compare(b.reader().maxDoc(), a.reader().maxDoc()));
        for (LeafReaderContext leaf : sortedLeaves) {
          int lightest = 0;
          for (int i = 1; i < numWorkers; i++) {
            if (workerDocCounts[i] < workerDocCounts[lightest]) lightest = i;
          }
          workerSegments[lightest].add(leaf);
          workerDocCounts[lightest] += leaf.reader().maxDoc();
        }

        final Weight weight;
        if (!isMatchAll) {
          IndexSearcher luceneSearcher = new IndexSearcher(engineSearcher.getIndexReader());
          weight =
              luceneSearcher.createWeight(
                  luceneSearcher.rewrite(query), ScoreMode.COMPLETE_NO_SCORES, 1.0f);
        } else {
          weight = null;
        }

        @SuppressWarnings("unchecked")
        java.util.concurrent.CompletableFuture<FlatSingleKeyMap>[] futures =
            new java.util.concurrent.CompletableFuture[numWorkers];

        for (int w = 0; w < numWorkers; w++) {
          final java.util.List<LeafReaderContext> mySegments = workerSegments[w];
          futures[w] =
              java.util.concurrent.CompletableFuture.supplyAsync(
                  () -> {
                    FlatSingleKeyMap localMap = new FlatSingleKeyMap(slotsPerGroup);
                    try {
                      for (LeafReaderContext leafCtx : mySegments) {
                        scanSegmentFlatSingleKey(
                            leafCtx, weight, isMatchAll, localMap, keyInfos, specs,
                            numAggs, isCountStar, accType, accOffset, slotsPerGroup,
                            bucket, numBuckets);
                      }
                    } catch (Exception e) {
                      throw new RuntimeException(e);
                    }
                    return localMap;
                  },
                  PARALLEL_POOL);
        }

        java.util.concurrent.CompletableFuture.allOf(futures).join();
        for (var future : futures) {
          FlatSingleKeyMap workerMap = future.join();
          if (workerMap.size > 0) flatMap.mergeFrom(workerMap);
        }
      } else {
        // Sequential path: single FlatSingleKeyMap for all segments
        Weight weight = null;
        if (!isMatchAll) {
          IndexSearcher luceneSearcher = new IndexSearcher(engineSearcher.getIndexReader());
          weight =
              luceneSearcher.createWeight(
                  luceneSearcher.rewrite(query), ScoreMode.COMPLETE_NO_SCORES, 1.0f);
        }
        for (LeafReaderContext leafCtx : leaves) {
          scanSegmentFlatSingleKey(
              leafCtx, weight, isMatchAll, flatMap, keyInfos, specs,
              numAggs, isCountStar, accType, accOffset, slotsPerGroup,
              bucket, numBuckets);
        }
      }
    }

    if (flatMap.size == 0) return List.of();

    int numGroupKeys = groupByKeys.size();
    int totalColumns = numGroupKeys + numAggs;

    // Determine which slots to output (with optional top-N selection)
    int[] outputSlots;
    int outputCount;

    if (sortAggIndex >= 0 && topN > 0 && topN < flatMap.size) {
      int sortAccOff = accOffset[sortAggIndex];
      int n = (int) Math.min(topN, flatMap.size);
      int[] heap = new int[n];
      int heapSize = 0;
      for (int slot = 0; slot < flatMap.capacity; slot++) {
        if (!flatMap.occupied[slot]) continue;
        long val = flatMap.accData[slot * slotsPerGroup + sortAccOff];
        if (heapSize < n) {
          heap[heapSize] = slot;
          heapSize++;
          int k = heapSize - 1;
          while (k > 0) {
            int parent = (k - 1) >>> 1;
            long pVal = flatMap.accData[heap[parent] * slotsPerGroup + sortAccOff];
            long kVal = flatMap.accData[heap[k] * slotsPerGroup + sortAccOff];
            boolean swap = sortAscending ? (kVal > pVal) : (kVal < pVal);
            if (swap) {
              int tmp = heap[parent];
              heap[parent] = heap[k];
              heap[k] = tmp;
              k = parent;
            } else break;
          }
        } else {
          long rootVal = flatMap.accData[heap[0] * slotsPerGroup + sortAccOff];
          boolean better = sortAscending ? (val < rootVal) : (val > rootVal);
          if (better) {
            heap[0] = slot;
            int k = 0;
            while (true) {
              int left = 2 * k + 1;
              if (left >= heapSize) break;
              int right = left + 1;
              int target = left;
              if (right < heapSize) {
                long lv = flatMap.accData[heap[left] * slotsPerGroup + sortAccOff];
                long rv = flatMap.accData[heap[right] * slotsPerGroup + sortAccOff];
                boolean pickRight = sortAscending ? (rv > lv) : (rv < lv);
                if (pickRight) target = right;
              }
              long kVal = flatMap.accData[heap[k] * slotsPerGroup + sortAccOff];
              long tVal = flatMap.accData[heap[target] * slotsPerGroup + sortAccOff];
              boolean swap = sortAscending ? (tVal > kVal) : (tVal < kVal);
              if (swap) {
                int tmp = heap[k];
                heap[k] = heap[target];
                heap[target] = tmp;
                k = target;
              } else break;
            }
          }
        }
      }
      outputSlots = heap;
      outputCount = heapSize;
    } else if (topN > 0 && topN < flatMap.size) {
      int n = (int) Math.min(topN, flatMap.size);
      outputSlots = new int[n];
      int idx = 0;
      for (int slot = 0; slot < flatMap.capacity && idx < n; slot++) {
        if (flatMap.occupied[slot]) outputSlots[idx++] = slot;
      }
      outputCount = idx;
    } else {
      outputSlots = new int[flatMap.size];
      int idx = 0;
      for (int slot = 0; slot < flatMap.capacity; slot++) {
        if (flatMap.occupied[slot]) outputSlots[idx++] = slot;
      }
      outputCount = idx;
    }

    BlockBuilder[] builders = new BlockBuilder[totalColumns];
    builders[0] = keyInfos.get(0).type.createBlockBuilder(null, outputCount);
    for (int i = 0; i < numAggs; i++) {
      builders[numGroupKeys + i] =
          resolveAggOutputType(specs.get(i), columnTypeMap).createBlockBuilder(null, outputCount);
    }
    for (int o = 0; o < outputCount; o++) {
      int slot = outputSlots[o];
      writeNumericKeyValue(builders[0], keyInfos.get(0), flatMap.keys[slot]);
      int base = slot * slotsPerGroup;
      for (int a = 0; a < numAggs; a++) {
        int off = base + accOffset[a];
        switch (accType[a]) {
          case 0: // COUNT
          case 1: // SUM long
            BigintType.BIGINT.writeLong(builders[numGroupKeys + a], flatMap.accData[off]);
            break;
          case 2: // AVG long
            long sum = flatMap.accData[off];
            long cnt = flatMap.accData[off + 1];
            if (cnt == 0) {
              builders[numGroupKeys + a].appendNull();
            } else {
              DoubleType.DOUBLE.writeDouble(builders[numGroupKeys + a], (double) sum / cnt);
            }
            break;
        }
      }
    }
    Block[] blocks = new Block[totalColumns];
    for (int i = 0; i < totalColumns; i++) blocks[i] = builders[i].build();
    return List.of(new Page(blocks));
  }

```

---

## 6. SingleKeyHashMap (line 12140) - referenced by FlatSingleKeyMap

```java
  private static final class SingleKeyHashMap {
    private static final int INITIAL_CAPACITY = 4096;
    private static final float LOAD_FACTOR = 0.7f;
    private static final int MAX_CAPACITY = 32_000_000;

    long[] keys;
    AccumulatorGroup[] groups;
    boolean[] occupied;
    int size;
    int capacity;
    private int threshold;
    private final List<AggSpec> specs;

    SingleKeyHashMap(List<AggSpec> specs) {
      this.specs = specs;
      this.capacity = INITIAL_CAPACITY;
      this.keys = new long[capacity];
      this.groups = new AccumulatorGroup[capacity];
      this.occupied = new boolean[capacity];
      this.size = 0;
      this.threshold = (int) (capacity * LOAD_FACTOR);
    }

    AccumulatorGroup getOrCreate(long key) {
      int mask = capacity - 1;
      int slot = hash1(key) & mask;
      while (occupied[slot]) {
        if (keys[slot] == key) {
          return groups[slot];
        }
        slot = (slot + 1) & mask;
      }
      // New entry
      AccumulatorGroup accGroup = createAccumulatorGroup(specs);
      keys[slot] = key;
      groups[slot] = accGroup;
      occupied[slot] = true;
      size++;
      if (size > threshold) {
        resize();
        return getExisting(key);
      }
      return accGroup;
    }

    private AccumulatorGroup getExisting(long key) {
      int mask = capacity - 1;
      int slot = hash1(key) & mask;
      while (occupied[slot]) {
        if (keys[slot] == key) {
          return groups[slot];
        }
        slot = (slot + 1) & mask;
      }
      throw new IllegalStateException("Key not found after resize");
    }

    private void resize() {
      if (size > MAX_CAPACITY) {
        throw new RuntimeException(
            "GROUP BY exceeded memory limit ("
                + size
                + " unique groups, max "
                + MAX_CAPACITY
                + "). "
                + "Add a WHERE clause to reduce cardinality.");
      }
      int newCapacity = capacity * 2;
      long[] newKeys = new long[newCapacity];
      AccumulatorGroup[] newGroups = new AccumulatorGroup[newCapacity];
      boolean[] newOccupied = new boolean[newCapacity];
      int newMask = newCapacity - 1;

      for (int i = 0; i < capacity; i++) {
        if (occupied[i]) {
          int slot = hash1(keys[i]) & newMask;
          while (newOccupied[slot]) {
            slot = (slot + 1) & newMask;
          }
          newKeys[slot] = keys[i];
          newGroups[slot] = groups[i];
          newOccupied[slot] = true;
        }
      }

      this.keys = newKeys;
      this.groups = newGroups;
      this.occupied = newOccupied;
      this.capacity = newCapacity;
      this.threshold = (int) (newCapacity * LOAD_FACTOR);
    }

    /** Hash a single long key using Murmur3 finalizer for good distribution. */
    private static int hash1(long key) {
      key ^= key >>> 33;
      key *= 0xff51afd7ed558ccdL;
      key ^= key >>> 33;
      key *= 0xc4ceb9fe1a85ec53L;
      key ^= key >>> 33;
      return (int) key;
    }
  }

  /**
   * Open-addressing hash map with two long keys and contiguous flat accumulator storage. All
   * accumulator data is stored in a single {@code long[]} array at offset {@code slot *
   * slotsPerGroup}, eliminating per-group object allocation entirely. On miss, only the slot's
   * range within the contiguous array is initialized (default 0 from Java array initialization
   * handles most cases). Resize copies accumulator data using {@code System.arraycopy} for
   * efficiency.
```

---

## 7. FlatTwoKeyMap (line 12251) - used by executeTwoKeyNumericFlat

```java
  private static final class FlatTwoKeyMap {
    private static final int INITIAL_CAPACITY = 8192;
    private static final float LOAD_FACTOR = 0.7f;
    private static final int MAX_CAPACITY = 32_000_000;

    long[] keys0;
    long[] keys1;
    boolean[] occupied;
    long[] accData; // contiguous: slot i's data at [i*slotsPerGroup .. (i+1)*slotsPerGroup)
    int size;
    int capacity;
    int threshold;
    final int slotsPerGroup;

    FlatTwoKeyMap(int slotsPerGroup) {
      this.slotsPerGroup = slotsPerGroup;
      this.capacity = INITIAL_CAPACITY;
      this.keys0 = new long[capacity];
      this.keys1 = new long[capacity];
      this.occupied = new boolean[capacity];
      this.accData = new long[capacity * slotsPerGroup];
      this.size = 0;
      this.threshold = (int) (capacity * LOAD_FACTOR);
    }

    /**
     * Find existing slot or insert new entry for the given key pair. For a new entry, initializes
     * the slot in the contiguous accData array to zeros (already guaranteed by Java array
     * initialization or resize copy). Returns the slot index. The caller accumulates into accData
     * at slot*slotsPerGroup.
     */
    int findOrInsert(long key0, long key1) {
      int mask = capacity - 1;
      int h = TwoKeyHashMap.hash2(key0, key1) & mask;
      while (occupied[h]) {
        if (keys0[h] == key0 && keys1[h] == key1) {
          return h;
        }
        h = (h + 1) & mask;
      }
      // New entry
      keys0[h] = key0;
      keys1[h] = key1;
      occupied[h] = true;
      // accData[h*slotsPerGroup..] is already 0 from array init
      size++;
      if (size > threshold) {
        resize();
        // Find the slot in the new layout
        return findExisting(key0, key1);
      }
      return h;
    }

    /**
     * Find existing slot or insert new entry, but refuse new insertions once size >= cap. Returns
     * the slot index if the key already exists or was just inserted; returns -1 if the key is new
     * and the map already has {@code cap} groups.
     *
     * <p>This enables LIMIT-without-ORDER-BY early-close: once we have enough groups, skip new
     * groups entirely, avoiding hash map growth for high-cardinality queries.
     */
    int findOrInsertCapped(long key0, long key1, int cap) {
      int mask = capacity - 1;
      int h = TwoKeyHashMap.hash2(key0, key1) & mask;
      while (occupied[h]) {
        if (keys0[h] == key0 && keys1[h] == key1) {
          return h;
        }
        h = (h + 1) & mask;
      }
      // New entry -- reject if capped
      if (size >= cap) {
        return -1;
      }
      keys0[h] = key0;
      keys1[h] = key1;
      occupied[h] = true;
      size++;
      if (size > threshold) {
        resize();
        return findExisting(key0, key1);
      }
      return h;
    }

    private int findExisting(long key0, long key1) {
      int mask = capacity - 1;
      int h = TwoKeyHashMap.hash2(key0, key1) & mask;
      while (occupied[h]) {
        if (keys0[h] == key0 && keys1[h] == key1) return h;
        h = (h + 1) & mask;
      }
      throw new IllegalStateException("Key not found after resize");
    }

    private void resize() {
      if (size > MAX_CAPACITY) {
        throw new RuntimeException(
            "GROUP BY exceeded memory limit ("
                + size
                + " unique groups, max "
                + MAX_CAPACITY
                + "). "
                + "Add a WHERE clause to reduce cardinality.");
      }
      int newCap = capacity * 2;
      long[] nk0 = new long[newCap];
      long[] nk1 = new long[newCap];
      boolean[] nocc = new boolean[newCap];
      long[] nacc = new long[newCap * slotsPerGroup];
      int nm = newCap - 1;
      for (int s = 0; s < capacity; s++) {
        if (occupied[s]) {
          int nh = TwoKeyHashMap.hash2(keys0[s], keys1[s]) & nm;
          while (nocc[nh]) nh = (nh + 1) & nm;
          nk0[nh] = keys0[s];
          nk1[nh] = keys1[s];
          nocc[nh] = true;
          System.arraycopy(accData, s * slotsPerGroup, nacc, nh * slotsPerGroup, slotsPerGroup);
        }
      }
      this.keys0 = nk0;
      this.keys1 = nk1;
      this.occupied = nocc;
      this.accData = nacc;
      this.capacity = newCap;
      this.threshold = (int) (newCap * LOAD_FACTOR);
    }

    /** Merge all entries from another FlatTwoKeyMap into this one, summing accumulator slots. */
    void mergeFrom(FlatTwoKeyMap other) {
      for (int s = 0; s < other.capacity; s++) {
        if (!other.occupied[s]) continue;
        int slot = findOrInsert(other.keys0[s], other.keys1[s]);
        int dstBase = slot * slotsPerGroup;
        int srcBase = s * other.slotsPerGroup;
        for (int j = 0; j < slotsPerGroup; j++) {
          accData[dstBase + j] += other.accData[srcBase + j];
        }
      }
    }
  }

  /**
   * Open-addressing hash map with three primitive long keys and contiguous flat accumulator
   * storage, analogous to {@link FlatTwoKeyMap}. Used for 3-key GROUP BY queries (e.g., Q19: GROUP
   * BY UserID, extract(minute FROM EventTime), SearchPhrase) to avoid per-group object allocation
   * (SegmentGroupKey, AccumulatorGroup, MergeableAccumulator).
```

---

## 8. collectFlatSingleKeyDoc & collectFlatSingleKeyDocWithLength (line 4685)

```java
  private static void collectFlatSingleKeyDoc(
      int doc,
      SortedNumericDocValues dv0,
      FlatSingleKeyMap flatMap,
      int slotsPerGroup,
      int numAggs,
      boolean[] isCountStar,
      int[] accType,
      int[] accOffset,
      SortedNumericDocValues[] numericAggDvs)
      throws IOException {
    long key0 = 0;
    if (dv0 != null && dv0.advanceExact(doc)) key0 = dv0.nextValue();
    int slot = flatMap.findOrInsert(key0);
    int base = slot * slotsPerGroup;

    for (int i = 0; i < numAggs; i++) {
      int off = base + accOffset[i];
      if (isCountStar[i]) {
        flatMap.accData[off]++;
        continue;
      }
      SortedNumericDocValues aggDv = numericAggDvs[i];
      if (aggDv != null && aggDv.advanceExact(doc)) {
        long rawVal = aggDv.nextValue();
        switch (accType[i]) {
          case 0: // COUNT (non-star, non-distinct)
            flatMap.accData[off]++;
            break;
          case 1: // SUM long
            flatMap.accData[off] += rawVal;
            break;
          case 2: // AVG long (sum, count)
            flatMap.accData[off] += rawVal;
            flatMap.accData[off + 1]++;
            break;
        }
      }
    }
  }

  /**
   * Same as collectFlatSingleKeyDoc but handles applyLength: reads VARCHAR DocValues for length.
   */
  private static void collectFlatSingleKeyDocWithLength(
      int doc,
      SortedNumericDocValues dv0,
      FlatSingleKeyMap flatMap,
      int slotsPerGroup,
      int numAggs,
      boolean[] isCountStar,
      int[] accType,
      int[] accOffset,
      SortedNumericDocValues[] numericAggDvs,
      SortedSetDocValues[] lengthVarcharDvs,
      int[][] ordLengthMaps,
      boolean[] aggApplyLength,
      int bucket,
      int numBuckets)
      throws IOException {
    long key0 = 0;
    if (dv0 != null && dv0.advanceExact(doc)) key0 = dv0.nextValue();
    // Hash-partition filter: skip docs not in this bucket
    if (numBuckets > 1
        && (SingleKeyHashMap.hash1(key0) & 0x7FFFFFFF) % numBuckets != bucket) {
      return;
    }
    int slot = flatMap.findOrInsert(key0);
    int base = slot * slotsPerGroup;

    for (int i = 0; i < numAggs; i++) {
      int off = base + accOffset[i];
      if (isCountStar[i]) {
        flatMap.accData[off]++;
        continue;
      }
      long rawVal = 0;
      boolean hasValue = false;
      if (aggApplyLength[i]) {
        // Use pre-computed ordinal→length map for O(1) per-doc lookup
        SortedSetDocValues varcharDv = lengthVarcharDvs[i];
        if (varcharDv != null && varcharDv.advanceExact(doc)) {
          int ord = (int) varcharDv.nextOrd();
          rawVal =
              (ordLengthMaps[i] != null && ord < ordLengthMaps[i].length)
                  ? ordLengthMaps[i][ord]
                  : varcharDv.lookupOrd(ord).length;
          hasValue = true;
        }
      } else {
        SortedNumericDocValues aggDv = numericAggDvs[i];
        if (aggDv != null && aggDv.advanceExact(doc)) {
          rawVal = aggDv.nextValue();
          hasValue = true;
        }
      }
      if (hasValue) {
        switch (accType[i]) {
          case 0:
            flatMap.accData[off]++;
            break;
          case 1:
            flatMap.accData[off] += rawVal;
            break;
          case 2:
            flatMap.accData[off] += rawVal;
            flatMap.accData[off + 1]++;
            break;
        }
      }
    }
  }

  /**
   * Scan a single segment for the flat single-key numeric GROUP BY path. Handles both MatchAll
   * (direct DocValues iteration) and filtered (Weight+Scorer) paths. Accumulates into the provided
   * FlatSingleKeyMap.
   */
```
