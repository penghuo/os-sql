# Hash Table & Hot Loop Code for Two-Level Hash Table Design

## 1. LongOpenHashSet.java (full source)

File: `dqe/src/main/java/org/opensearch/sql/dqe/operator/LongOpenHashSet.java`

- Open-addressing hash set for primitive longs, eliminates Long boxing
- Sentinel-based: `EMPTY = Long.MIN_VALUE`, separate booleans for 0 and sentinel
- Murmur3 finalizer hash, linear probing, 0.65 load factor
- Key methods: `add(long)`, `contains(long)`, `addAll(LongOpenHashSet)`, `ensureCapacity(int)`
- `resize()` doubles capacity, rehashes all keys

## 2. scanDocRangeFlatSingleKeyCountStar (line 14412)

File: `dqe/src/main/java/org/opensearch/sql/dqe/shard/source/FusedGroupByAggregate.java:14412`

```java
private static void scanDocRangeFlatSingleKeyCountStar(
    long[] keyValues, int startDoc, int endDoc, Bits liveDocs,
    FlatSingleKeyMap flatMap, int accOffset0, int slotsPerGroup) {
  if (liveDocs != null) {
    for (int doc = startDoc; doc < endDoc; doc++) {
      if (!liveDocs.get(doc)) continue;
      int slot = flatMap.findOrInsert(keyValues[doc]);
      flatMap.accData[slot * slotsPerGroup + accOffset0]++;
    }
    return;
  }

  // Prefetch-batched path for the common no-deletes case.
  final int BATCH = 16;
  long prefetchSink = 0;
  int doc = startDoc;

  for (; doc + BATCH <= endDoc; doc += BATCH) {
    long[] mapKeys = flatMap.keys;
    int mask = flatMap.capacity - 1;
    // Phase 1: compute hash slots and prefetch cache lines
    for (int j = 0; j < BATCH; j++) {
      int slot = SingleKeyHashMap.hash1(keyValues[doc + j]) & mask;
      prefetchSink += mapKeys[slot]; // force cache line load
    }
    // Phase 2: actual probe and insert
    for (int j = 0; j < BATCH; j++) {
      int slot = flatMap.findOrInsert(keyValues[doc + j]);
      flatMap.accData[slot * slotsPerGroup + accOffset0]++;
    }
  }
  // Remainder
  for (; doc < endDoc; doc++) {
    int slot = flatMap.findOrInsert(keyValues[doc]);
    flatMap.accData[slot * slotsPerGroup + accOffset0]++;
  }
}
```

## 3. FlatSingleKeyMap inner class (line 13640)

File: `dqe/src/main/java/org/opensearch/sql/dqe/shard/source/FusedGroupByAggregate.java:13640`

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

  private int findExisting(long key) {
    int mask = capacity - 1;
    int h = SingleKeyHashMap.hash1(key) & mask;
    while (keys[h] != EMPTY_KEY) {
      if (keys[h] == key) return h;
      h = (h + 1) & mask;
    }
    throw new IllegalStateException("Key not found after resize");
  }

  private void resize() {
    if (size > MAX_CAPACITY) {
      throw new RuntimeException("GROUP BY exceeded memory limit (" + size + " unique groups)");
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
    this.keys = nk; this.accData = nacc;
    this.capacity = newCap; this.threshold = (int) (newCap * LOAD_FACTOR);
  }

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

## 4. executeSingleKeyNumericFlat dispatch (line 5143)

File: `dqe/src/main/java/org/opensearch/sql/dqe/shard/source/FusedGroupByAggregate.java:5143`

Key dispatch logic (abbreviated — full method is ~350 lines):

```java
private static List<Page> executeSingleKeyNumericFlat(
    IndexShard shard, Query query, List<KeyInfo> keyInfos, List<AggSpec> specs,
    Map<String, Type> columnTypeMap, List<String> groupByKeys,
    int numAggs, boolean[] isCountStar, int[] accType,
    int sortAggIndex, boolean sortAscending, long topN,
    int bucket, int numBuckets) throws Exception {

  // Compute flat layout: accOffset[] and slotsPerGroup
  final int[] accOffset = new int[numAggs];
  int totalSlots = 0;
  for (int i = 0; i < numAggs; i++) {
    accOffset[i] = totalSlots;
    switch (accType[i]) {
      case 0: case 1: totalSlots += 1; break;  // COUNT, SUM
      case 2: totalSlots += 2; break;           // AVG (sum+count)
    }
  }
  final int slotsPerGroup = totalSlots;

  // Three execution paths:
  // 1. MatchAll + allCountStar + >10M docs → sequential scan (better L3 locality)
  // 2. MatchAll + allCountStar + ≤10M docs → doc-range parallel with worker maps + mergeFrom
  // 3. Otherwise → segment-parallel with per-worker FlatSingleKeyMap + mergeFrom

  // Path 1 (sequential, high-cardinality):
  flatMap = new FlatSingleKeyMap(slotsPerGroup, 8_000_000);
  for (each segment) {
    scanDocRangeFlatSingleKeyCountStar(keyValues, 0, len, liveDocs, flatMap, accOffset[0], slotsPerGroup);
  }

  // Path 2 (parallel, lower-cardinality):
  // Splits docs into work units, each worker builds local FlatSingleKeyMap
  // then mergeFrom into main flatMap

  // Path 3 (segment-parallel, non-COUNT(*) or filtered):
  // Largest-first greedy segment assignment, per-worker scanSegmentFlatSingleKey

  // Output: optional top-N heap selection on sortAggIndex, then build Page blocks
}
```
