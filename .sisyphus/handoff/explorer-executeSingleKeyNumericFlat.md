# executeSingleKeyNumericFlat — Extraction for Parallelization

## Source File
`dqe/src/main/java/org/opensearch/sql/dqe/shard/source/FusedGroupByAggregate.java`

## Key Constants (lines 116-127)
```java
private static final String PARALLELISM_MODE = System.getProperty("dqe.parallelism", "docrange");
private static final int THREADS_PER_SHARD =
    Math.max(
        1,
        Runtime.getRuntime().availableProcessors() / Integer.getInteger("dqe.numLocalShards", 4));

private static final java.util.concurrent.ForkJoinPool PARALLEL_POOL =
    new java.util.concurrent.ForkJoinPool(
        Runtime.getRuntime().availableProcessors(),
        java.util.concurrent.ForkJoinPool.defaultForkJoinWorkerThreadFactory,
        null,
        true); // asyncMode=true for work-stealing
```

---

## 1. executeSingleKeyNumericFlat (lines 4269–4660)

### Method Signature
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
    long topN)
    throws Exception {
```

### Method Body — Accumulator Layout Setup
```java
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
```

### MatchAllDocsQuery Fast Path (per-leaf iteration)
```java
  try (org.opensearch.index.engine.Engine.Searcher engineSearcher =
      shard.acquireSearcher("dqe-fused-groupby-numeric-1key-flat")) {

    if (query instanceof MatchAllDocsQuery) {
      // Fast path: iterate all docs directly without Collector/Scorer overhead.
      for (LeafReaderContext leafCtx : engineSearcher.getIndexReader().leaves()) {
        LeafReader reader = leafCtx.reader();
        int maxDoc = reader.maxDoc();
        Bits liveDocs = reader.getLiveDocs();
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

        if (liveDocs == null && dv0 != null) {
          boolean allCountStar = true;
          boolean hasApplyLen = false;
          for (int i = 0; i < numAggs; i++) {
            if (!isCountStar[i]) allCountStar = false;
            if (aggApplyLength[i]) hasApplyLen = true;
          }

          if (allCountStar) {
            // Ultra-fast path: only COUNT(*), use nextDoc() on key column
            int doc = dv0.nextDoc();
            while (doc != DocIdSetIterator.NO_MORE_DOCS) {
              long key0 = dv0.nextValue();
              int slot = flatMap.findOrInsert(key0);
              flatMap.accData[slot * slotsPerGroup + accOffset[0]]++;
              doc = dv0.nextDoc();
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
              // Some agg columns missing — fall back to advanceExact
              for (int doc = 0; doc < maxDoc; doc++) {
                collectFlatSingleKeyDocWithLength(
                    doc, dv0, flatMap, slotsPerGroup, numAggs, isCountStar,
                    accType, accOffset, numericAggDvs, lengthVarcharDvs,
                    ordLengthMaps, aggApplyLength);
              }
            }
          } else {
            // Has applyLength — use advanceExact
            for (int doc = 0; doc < maxDoc; doc++) {
              collectFlatSingleKeyDocWithLength(
                  doc, dv0, flatMap, slotsPerGroup, numAggs, isCountStar,
                  accType, accOffset, numericAggDvs, lengthVarcharDvs,
                  ordLengthMaps, aggApplyLength);
            }
          }
        } else if (liveDocs == null) {
          // Key column is null — all keys are 0
          for (int doc = 0; doc < maxDoc; doc++) {
            collectFlatSingleKeyDocWithLength(
                doc, dv0, flatMap, slotsPerGroup, numAggs, isCountStar,
                accType, accOffset, numericAggDvs, lengthVarcharDvs,
                ordLengthMaps, aggApplyLength);
          }
        } else {
          for (int doc = 0; doc < maxDoc; doc++) {
            if (liveDocs.get(doc)) {
              collectFlatSingleKeyDocWithLength(
                  doc, dv0, flatMap, slotsPerGroup, numAggs, isCountStar,
                  accType, accOffset, numericAggDvs, lengthVarcharDvs,
                  ordLengthMaps, aggApplyLength);
            }
          }
        }
      }
```

### Filtered Path (Weight+Scorer loop)
```java
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

        final SortedNumericDocValues[] numericAggDvs2 = new SortedNumericDocValues[numAggs];
        final SortedSetDocValues[] lengthVarcharDvs2 = new SortedSetDocValues[numAggs];
        final int[][] ordLengthMaps2 = new int[numAggs][];
        final boolean[] aggApplyLength2 = new boolean[numAggs];
        for (int i = 0; i < numAggs; i++) {
          if (!isCountStar[i]) {
            if (specs.get(i).applyLength) {
              SortedSetDocValues vdv = reader.getSortedSetDocValues(specs.get(i).arg);
              lengthVarcharDvs2[i] = vdv;
              aggApplyLength2[i] = true;
              if (vdv != null) {
                int oc = (int) Math.min(vdv.getValueCount(), 10_000_000);
                int[] ol = new int[oc];
                for (int o = 0; o < oc; o++) ol[o] = vdv.lookupOrd(o).length;
                ordLengthMaps2[i] = ol;
              }
            } else {
              numericAggDvs2[i] = reader.getSortedNumericDocValues(specs.get(i).arg);
            }
          }
        }

        DocIdSetIterator docIt = scorer.iterator();
        int doc;
        while ((doc = docIt.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
          collectFlatSingleKeyDocWithLength(
              doc, dv0, flatMap, slotsPerGroup, numAggs, isCountStar,
              accType, accOffset, numericAggDvs2, lengthVarcharDvs2,
              ordLengthMaps2, aggApplyLength2);
        }
      }
    }
  }
```

### Output: Top-N Selection and Page Building
```java
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

## 2. Helper: collectFlatSingleKeyDoc (lines 4662–4700)
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
```

## 3. Helper: collectFlatSingleKeyDocWithLength (lines 4706–4762)
```java
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
    boolean[] aggApplyLength)
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
    long rawVal = 0;
    boolean hasValue = false;
    if (aggApplyLength[i]) {
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
```

---

## 4. FlatSingleKeyMap Inner Class (lines 11841–11940)
```java
private static final class FlatSingleKeyMap {
  private static final int INITIAL_CAPACITY = 4096;
  private static final float LOAD_FACTOR = 0.7f;
  private static final int MAX_CAPACITY = 8_000_000;

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

  int findOrInsert(long key) {
    int mask = capacity - 1;
    int h = SingleKeyHashMap.hash1(key) & mask;
    while (occupied[h]) {
      if (keys[h] == key) {
        return h;
      }
      h = (h + 1) & mask;
    }
    keys[h] = key;
    occupied[h] = true;
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
}
```

**Note:** No `merge(FlatSingleKeyMap other)` method exists yet — this must be added for parallelization.

---

## 5. Parallel Pattern Template — from executeSingleVarcharCountStar (lines 1638–1760)

This is the pattern to replicate for parallelizing `executeSingleKeyNumericFlat`.

### Parallelism Guard & Segment Partitioning
```java
// === Parallel multi-segment path ===
boolean canParallelize =
    !"off".equals(PARALLELISM_MODE) && THREADS_PER_SHARD > 1 && leaves.size() > 1;
if (canParallelize) {
  SortedSetDocValues checkDv = leaves.get(0).reader().getSortedSetDocValues(columnName);
  if (checkDv != null && checkDv.getValueCount() > 500_000) {
    canParallelize = false;
  }
}
if (canParallelize) {
  int numWorkers = Math.min(THREADS_PER_SHARD, leaves.size());
  // Partition segments: largest-first to lightest worker
  @SuppressWarnings("unchecked")
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
```

### Per-Worker CompletableFuture (VARCHAR version — adapt for FlatSingleKeyMap)
```java
  IndexSearcher luceneSearcher = new IndexSearcher(engineSearcher.getIndexReader());
  Weight weight =
      luceneSearcher.createWeight(
          luceneSearcher.rewrite(query), ScoreMode.COMPLETE_NO_SCORES, 1.0f);

  @SuppressWarnings("unchecked")
  java.util.concurrent.CompletableFuture<HashMap<BytesRefKey, long[]>>[] futures =
      new java.util.concurrent.CompletableFuture[numWorkers];

  for (int w = 0; w < numWorkers; w++) {
    final List<LeafReaderContext> mySegments = workerSegments[w];
    futures[w] =
        java.util.concurrent.CompletableFuture.supplyAsync(
            () -> {
              HashMap<BytesRefKey, long[]> workerCounts = new HashMap<>();
              try {
                for (LeafReaderContext leafCtx : mySegments) {
                  Scorer scorer = weight.scorer(leafCtx);
                  if (scorer == null) continue;
                  // ... per-segment accumulation ...
                }
              } catch (IOException e) {
                throw new java.io.UncheckedIOException(e);
              }
              return workerCounts;
            },
            PARALLEL_POOL);
  }
```

### Merge Worker Results into Global Map
```java
  java.util.concurrent.CompletableFuture.allOf(futures).join();

  HashMap<BytesRefKey, long[]> globalCounts = new HashMap<>();
  for (var future : futures) {
    HashMap<BytesRefKey, long[]> wMap = future.join();
    for (Map.Entry<BytesRefKey, long[]> e : wMap.entrySet()) {
      long[] existing = globalCounts.get(e.getKey());
      if (existing == null) {
        globalCounts.put(e.getKey(), e.getValue());
      } else {
        existing[0] += e.getValue()[0];
      }
    }
  }
```

---

## 6. Helper: writeNumericKeyValue (line 12554)
```java
private static void writeNumericKeyValue(BlockBuilder builder, KeyInfo keyInfo, long value) {
  Type type = keyInfo.type;
  if (type instanceof DoubleType) {
    DoubleType.DOUBLE.writeDouble(builder, Double.longBitsToDouble(value));
  } else if (type instanceof TimestampType) {
    TimestampType.TIMESTAMP_MILLIS.writeLong(builder, value * 1000L);
  } else if (type instanceof BooleanType) {
    BooleanType.BOOLEAN.writeBoolean(builder, value == 1);
  } else {
    type.writeLong(builder, value);
  }
}
```

---

## 7. Parallelization Plan

To parallelize `executeSingleKeyNumericFlat`, follow this pattern:

1. **Add `mergeFrom(FlatSingleKeyMap other)` to FlatSingleKeyMap** — iterate `other`'s occupied slots, for each key call `findOrInsert` on `this`, then add accData element-wise for `slotsPerGroup` longs.

2. **Segment partitioning** — same largest-first-to-lightest-worker strategy from `executeSingleVarcharCountStar`.

3. **Per-worker lambda** — each worker creates its own `FlatSingleKeyMap(slotsPerGroup)`, processes assigned segments (both MatchAll and filtered paths), returns its local map.

4. **Merge** — on the main thread, merge all worker maps into a single global `FlatSingleKeyMap` using `mergeFrom()`.

5. **Output** — existing top-N selection and page building code works unchanged on the merged global map.

### Key Differences from VARCHAR Pattern
| Aspect | VARCHAR (existing) | Numeric Flat (new) |
|--------|-------------------|-------------------|
| Per-worker map type | `HashMap<BytesRefKey, long[]>` | `FlatSingleKeyMap` |
| Merge strategy | HashMap merge with `existing[0] += ...` | `mergeFrom()` adding `slotsPerGroup` longs per key |
| Accumulator slots | Always 1 (count only) | Variable: 1 for COUNT/SUM, 2 for AVG |
| MatchAll fast path | Not applicable (always filtered in parallel) | Must handle both MatchAll and filtered in worker |
