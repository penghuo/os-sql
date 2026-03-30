# Q08 & Q13 Current Code Analysis

## File: `dqe/src/main/java/org/opensearch/sql/dqe/shard/transport/TransportShardExecuteAction.java`

---

## 1. `executeCountDistinctWithHashSets` (Q08 — GROUP BY numeric + COUNT(DISTINCT numeric))

**Signature** (line 982):
```java
private ShardExecuteResponse executeCountDistinctWithHashSets(
    AggregationNode aggNode, ShardExecuteRequest req,
    String keyName0, String keyName1, Type type0, Type type1) throws Exception
```

**Architecture**:
- GROUP BY key0 (numeric, e.g. RegionID) with COUNT(DISTINCT key1) (numeric, e.g. UserID)
- Uses `Map<Long, LongOpenHashSet>` — key0 → set of distinct key1 values
- Parallel segment scanning via ForkJoinPool (dispatches N-1 segments to pool, runs last on current thread)
- Each segment calls `scanSegmentForCountDistinct()` which returns its own `Map<Long, LongOpenHashSet>`

**Merge logic** (lines ~1060-1080):
```java
// Merge per-segment results: union all LongOpenHashSets per key0
finalSets = segResults[0];
for (int s = 1; s < segResults.length; s++) {
  for (var entry : segResults[s].entrySet()) {
    LongOpenHashSet existing = finalSets.get(entry.getKey());
    if (existing == null) {
      finalSets.put(entry.getKey(), entry.getValue());
    } else {
      // Merge smaller into larger
      if (other.size() > existing.size()) {
        mergeHashSets(other, existing);
        finalSets.put(entry.getKey(), other);
      } else {
        mergeHashSets(existing, other);
      }
    }
  }
}
```

**Output**: Page with 3 columns: (key0, placeholder=0, COUNT=distinct_count). Sets response.setDistinctSets(finalSets).

**Key observation**: All keys are `long` — no String conversion needed. Merge is O(n) in set sizes. This is fast (2.40s).

---

## 2. `executeVarcharCountDistinctWithHashSets` (Q13 — GROUP BY varchar + COUNT(DISTINCT numeric))

**Signature** (line 2351):
```java
private ShardExecuteResponse executeVarcharCountDistinctWithHashSets(
    AggregationNode aggNode, ShardExecuteRequest req,
    String varcharKeyName, String numericKeyName, Type numericKeyType) throws Exception
```

**Architecture**:
- GROUP BY varcharKeyName (e.g. SearchPhrase) with COUNT(DISTINCT numericKeyName) (e.g. UserID)
- Uses `Map<String, LongOpenHashSet>` as the cross-segment accumulator
- Within each segment, uses ordinal-indexed array: `LongOpenHashSet[] ordSets` indexed by ordinal
- Two paths: parallel (MatchAll + multiple segments) and sequential

**Parallel path** (lines ~2400-2480):
- Partitions segments across workers using largest-first greedy assignment
- Each worker builds its own `Map<String, LongOpenHashSet> localMap`
- Inner loop per segment:
  ```java
  // Unwrap singleton SortedDocValues for single-valued fields
  SortedDocValues sdv = DocValues.unwrapSingleton(varcharDv);
  while ((doc = sdv.nextDoc()) != NO_MORE_DOCS) {
    int ord = sdv.ordValue();
    if (ordSets[ord] == null) ordSets[ord] = new LongOpenHashSet(16);
    long val = 0;
    if (numericDv != null && numericDv.advanceExact(doc)) val = numericDv.nextValue();
    ordSets[ord].add(val);
  }
  // After scanning all docs in segment:
  mergeOrdSetsIntoMap(varcharDv, ordSets, localMap);
  ```
- After all workers complete, merges worker maps into main `varcharDistinctSets`

**Sequential path** (lines ~2500-2595):
- Same inner loop structure but on single thread
- Has special non-MatchAll path that collects matching doc IDs first, then does sequential DV scan

**Output**: Page with 3 columns: (VARCHAR key, placeholder=0, COUNT=distinct_count). Sets response.setVarcharDistinctSets(varcharDistinctSets).

---

## 3. `mergeOrdSetsIntoMap` (the bottleneck)

**Signature** (line 2629):
```java
private static void mergeOrdSetsIntoMap(
    SortedSetDocValues dv,
    LongOpenHashSet[] ordSets,
    Map<String, LongOpenHashSet> target) throws IOException
```

**Full implementation**:
```java
for (int ord = 0; ord < ordSets.length; ord++) {
  if (ordSets[ord] == null) continue;
  BytesRef bytes = dv.lookupOrd(ord);       // <-- lookupOrd per ordinal
  String key = bytes.utf8ToString();         // <-- String allocation per ordinal
  LongOpenHashSet existing = target.get(key);
  if (existing == null) {
    target.put(key, ordSets[ord]);
  } else {
    long[] srcKeys = ordSets[ord].keys();
    for (long v : srcKeys) {
      if (v != ordSets[ord].emptyMarker()) existing.add(v);
    }
    if (ordSets[ord].hasZeroValue()) existing.add(0L);
    if (ordSets[ord].hasSentinelValue()) existing.add(Long.MIN_VALUE);
  }
}
```

**Why this is the Q13 bottleneck**:
1. `dv.lookupOrd(ord)` — called for EVERY non-null ordinal in EVERY segment. For Q13 (SearchPhrase), there are potentially millions of unique ordinals across segments.
2. `bytes.utf8ToString()` — allocates a new `String` object for every ordinal. This is O(bytes) per call.
3. `target.get(key)` / `target.put(key, ...)` — HashMap lookup on String keys requires `hashCode()` + `equals()` on potentially long SearchPhrase strings.
4. This runs per-segment, so the same SearchPhrase string is looked up, allocated, and hashed multiple times (once per segment it appears in).

**Contrast with Q08**: Q08 uses `Map<Long, LongOpenHashSet>` — merge is just long-keyed HashMap operations with no String allocation, no lookupOrd, no UTF-8 decoding. That's why Q08 is 2.40s vs Q13's 7.50s.

---

## Call sites

- **Q08 dispatch** (line 309): `executeCountDistinctWithHashSets(aggDedupNode, req, keyName0, keyName1, t0, t1);`
- **Q13 dispatch** (line 317): `executeVarcharCountDistinctWithHashSets(aggDedupNode, req, keyName0, keyName1, t1);`

## Key types used

- `org.opensearch.sql.dqe.operator.LongOpenHashSet` — custom open-addressing hash set for longs
- `org.apache.lucene.index.SortedSetDocValues` — Lucene's sorted set doc values (varchar ordinals)
- `org.apache.lucene.index.SortedNumericDocValues` — Lucene's sorted numeric doc values
- `org.apache.lucene.index.SortedDocValues` — unwrapped singleton for single-valued sorted fields
