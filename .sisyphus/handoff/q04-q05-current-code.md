# Q04 & Q05 Current Implementation Analysis

## 1. `collectDistinctValuesRaw` (Q04 — COUNT(DISTINCT UserID))
**File:** `dqe/src/main/java/org/opensearch/sql/dqe/shard/source/FusedScanAggregate.java:1511`

### Signature
```java
public static LongOpenHashSet collectDistinctValuesRaw(
    String columnName, IndexShard shard, Query query) throws Exception
```

### Key Logic
- **Pre-sizing:** `new LongOpenHashSet(Math.min(totalDocs, 32_000_000))`
- **MatchAllDocsQuery path (no filter):**
  - **Sequential scan** across all leaf segments (NOT parallel)
  - Comment explains: "parallel with merge was 4.3x SLOWER due to LongOpenHashSet merge cost"
  - **No-deletes segments:** Uses `dv.nextDoc()` loop (sequential, avoids `advanceExact` overhead)
  - **Has-deletes segments:** Uses `liveDocs.get(doc) && dv.advanceExact(doc)` per doc
  - All values go into a **single shared `LongOpenHashSet`**
- **Filtered query path:** Uses Collector-based approach with `dv.advanceExact(doc)`

### Performance Bottleneck (Q04 = 2.15s)
- Single-threaded sequential scan into one `LongOpenHashSet`
- ~18M distinct UserIDs → large hash set with many probes
- No parallelism because `LongOpenHashSet.addAll()` merge is expensive (iterates full backing array)

---

## 2. `collectDistinctStringsRaw` (Q05 — COUNT(DISTINCT SearchPhrase))
**File:** `dqe/src/main/java/org/opensearch/sql/dqe/shard/source/FusedScanAggregate.java:1588`

### Signature
```java
public static java.util.HashSet<String> collectDistinctStringsRaw(
    String columnName, IndexShard shard, Query query) throws Exception
```

### Key Logic
- **MatchAllDocsQuery path:**
  - Separates segments into `noDeleteLeaves` and `hasDeleteLeaves`
  - **Parallel path** (when numWorkers > 1 and multiple no-delete segments):
    - `numWorkers = min(availableProcessors / dqe.numLocalShards, noDeleteLeaves.size())`
    - Segments sorted by `valueCount` descending, assigned largest-first to lightest worker
    - Each worker iterates **all ordinals** in its segments: `for (ord = 0; ord < valueCount; ord++) localSet.add(dv.lookupOrd(ord).utf8ToString())`
    - Worker results merged via `distinctStrings.addAll(future.join())`
  - **Sequential fallback** for single segment or single worker: same ordinal iteration
  - **Has-deletes segments:** Uses `FixedBitSet usedOrdinals` to track which ordinals are live, then resolves only used ordinals
- **Filtered query path:** Per-segment `FixedBitSet` ordinal collection, then bulk string resolution

### Performance Bottleneck (Q05 = 3.96s)
- `dv.lookupOrd(ord).utf8ToString()` materializes every distinct String object
- `java.util.HashSet<String>` has boxing/hashing overhead for strings
- Parallel workers each build full `HashSet<String>`, then merge (addAll) — duplicates across segments cause redundant String creation
- For COUNT(DISTINCT), we only need the **count**, not the actual strings

---

## 3. `LongOpenHashSet` (Custom primitive hash set)
**File:** `dqe/src/main/java/org/opensearch/sql/dqe/operator/LongOpenHashSet.java:20`

### Design
- Open-addressing with linear probing, Murmur3 finalizer hash
- Sentinel-based: `EMPTY = Long.MIN_VALUE`, separate `hasZero` and `hasSentinel` booleans
- Load factor: 0.65, initial capacity: 8 (power-of-two sizing)

### `add(long value)` — Line 88
```java
public boolean add(long value) {
    if (value == 0) { if (hasZero) return false; hasZero = true; size++; return true; }
    if (value == EMPTY) { if (hasSentinel) return false; hasSentinel = true; size++; return true; }
    // Murmur3 finalizer hash → linear probe → insert
    // Resizes at threshold (capacity * 0.65)
}
```

### `addAll(LongOpenHashSet other)` — Line 172
```java
public void addAll(LongOpenHashSet other) {
    if (other.hasZero && !hasZero) { hasZero = true; size++; }
    if (other.hasSentinel && !hasSentinel) { hasSentinel = true; size++; }
    long[] otherKeys = other.keys;
    for (int i = 0; i < otherKeys.length; i++) {
        if (otherKeys[i] != EMPTY) { add(otherKeys[i]); }
    }
}
```
**Problem:** `addAll` iterates the **entire backing array** (not just `size` elements). With 0.65 load factor, a set of N elements has ~1.54N slots, so merge scans ~1.54N slots and does N `add()` calls with probing. This is why parallel merge was 4.3x slower for Q04.

---

## Optimization Opportunities

### Q04 (COUNT(DISTINCT UserID) — numeric)
1. **Ordinal-based counting:** If the column has `SortedNumericDocValues` backed by ordinals, count ordinals via `FixedBitSet` instead of materializing values into `LongOpenHashSet`
2. **Parallel with ordinal merge:** `FixedBitSet.or()` is O(words) — much cheaper than `LongOpenHashSet.addAll()`
3. **HyperLogLog approximation** if exact count isn't required

### Q05 (COUNT(DISTINCT SearchPhrase) — string)
1. **Ordinal-only counting:** For MatchAll with no deletes, the answer is simply `dv.getValueCount()` — no need to materialize strings at all
2. **FixedBitSet ordinal dedup** for segments with deletes — count set bits instead of materializing strings
3. **Avoid String materialization entirely** — only need `bitSet.cardinality()` for the count
