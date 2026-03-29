# Bitset Pre-Collection Pattern & Collector-Based GROUP BY Paths

## 1. Existing Bitset Pre-Collect Pattern (TransportShardExecuteAction.java)

### 1a. Collect-then-sequential-scan pattern (line 1555)

**File:** `dqe/src/main/java/org/opensearch/sql/dqe/shard/transport/TransportShardExecuteAction.java`
**Method:** `executeVarcharCountDistinctWithHashSets()` (line 1482)

This is the existing "collect-then-sequential-scan" pattern for WHERE-filtered queries. It:
1. Collects matching doc IDs into a sorted int[] array via Scorer
2. Sequentially scans DocValues, matching against the collected array

```java
// Line 1555-1600: Collect-then-sequential-scan
// Step 1: Collect matching doc IDs into sorted array
org.apache.lucene.search.Weight weight =
    engineSearcher.createWeight(
        engineSearcher.rewrite(luceneQuery),
        org.apache.lucene.search.ScoreMode.COMPLETE_NO_SCORES, 1.0f);
org.apache.lucene.search.Scorer scorer = weight.scorer(leafCtx);
// ...
org.apache.lucene.search.DocIdSetIterator disi = scorer.iterator();
int[] matchDocs = new int[1024];
int matchCount = 0;
int doc;
while ((doc = disi.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
    if (matchCount == matchDocs.length)
        matchDocs = java.util.Arrays.copyOf(matchDocs, matchDocs.length * 2);
    matchDocs[matchCount++] = doc;
}

// Step 2: Sequential scan of varchar DocValues, matching against collected doc IDs
SortedDocValues sdv = DocValues.unwrapSingleton(varcharDv);
if (sdv != null && matchCount > 0) {
    int matchIdx = 0;
    int dvDoc;
    while ((dvDoc = sdv.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
        if (matchIdx >= matchCount) break;
        if (dvDoc == matchDocs[matchIdx]) {
            int ord = sdv.ordValue();
            // ... accumulate into ordSets[ord]
            matchIdx++;
        }
    }
}
```

**Key insight:** Uses sorted int[] array, NOT FixedBitSet. This is a merge-join between DocValues iterator and sorted doc IDs.

### 1b. FixedBitSet ordinal dedup pattern (FusedScanAggregate.java)

**File:** `dqe/src/main/java/org/opensearch/sql/dqe/shard/source/FusedScanAggregate.java`
**Lines:** 1597, 1609, 1661, 1737

FixedBitSet is used for **ordinal dedup** (not doc ID collection) in VARCHAR DISTINCT paths:

```java
// Line 1737 (single-segment fast path)
FixedBitSet usedOrdinals = new FixedBitSet((int) Math.min(valueCount, Integer.MAX_VALUE));
for (int doc = 0; doc < maxDoc; doc++) {
    boolean isLive = liveDocs == null || liveDocs.get(doc);
    if (isLive && dv.advanceExact(doc)) {
        usedOrdinals.set((int) dv.nextOrd());
    }
}
// Then iterate set bits:
for (int ord = usedOrdinals.nextSetBit(0);
     ord != -1;
     ord = (ord + 1 < usedOrdinals.length()) ? usedOrdinals.nextSetBit(ord + 1) : -1) {
    BytesRef bytes = dv.lookupOrd(ord);
    // ... build output
}
```

---

## 2. Collector-Based GROUP BY Path (FusedGroupByAggregate.java:7407)

**File:** `dqe/src/main/java/org/opensearch/sql/dqe/shard/source/FusedGroupByAggregate.java`
**Lines:** ~7407-7490 (the `else` branch after MatchAllDocsQuery check)

This is the **general Collector path for filtered queries** in the two-key varchar flat path:

```java
// Line ~7407: General Collector path for filtered queries
engineSearcher.search(
    query,
    new Collector() {
      @Override
      public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
          Object[] keyReaders = new Object[2];
          for (int i = 0; i < 2; i++) {
              KeyInfo ki = keyInfos.get(i);
              if (ki.isVarchar)
                  keyReaders[i] = context.reader().getSortedSetDocValues(ki.name);
              else
                  keyReaders[i] = context.reader().getSortedNumericDocValues(ki.name);
          }
          keyReadersHolder[0] = keyReaders;

          final SortedNumericDocValues[] numericAggDvs = new SortedNumericDocValues[numAggs];
          for (int i = 0; i < numAggs; i++) {
              if (!isCountStar[i])
                  numericAggDvs[i] = context.reader().getSortedNumericDocValues(specs.get(i).arg);
          }

          return new LeafCollector() {
              @Override public void setScorer(Scorable scorer) {}

              @Override
              public void collect(int doc) throws IOException {
                  long k0 = 0, k1 = 0;
                  // ... advanceExact(doc) for each key and agg DV
                  // ... flatMap.findOrInsert(k0, k1) or findOrInsertCapped
                  // ... accumulate into flatMap.accData[off]
              }
          };
      }
      @Override public ScoreMode scoreMode() { return ScoreMode.COMPLETE_NO_SCORES; }
    });
```

**Performance problem:** Each `collect(int doc)` call does `advanceExact(doc)` on every DocValues field (2 keys + N agg fields). This is random-access per doc, with virtual dispatch overhead from the Collector/BulkScorer framework.

---

## 3. MatchAllDocsQuery Direct Iteration Path (FusedGroupByAggregate.java:7060)

**File:** `dqe/src/main/java/org/opensearch/sql/dqe/shard/source/FusedGroupByAggregate.java`
**Lines:** ~7060-7130

```java
// Line ~7060: MatchAllDocsQuery fast path
if (query instanceof MatchAllDocsQuery) {
    LeafReaderContext leafCtx = leaves.get(0);
    LeafReader reader = leafCtx.reader();
    int maxDoc = reader.maxDoc();
    Bits liveDocs = reader.getLiveDocs();

    // Open key readers (once)
    Object[] keyReaders = new Object[2];
    for (int i = 0; i < 2; i++) { /* ... getSortedSetDocValues / getSortedNumericDocValues */ }

    // Open aggregate doc values (once)
    SortedNumericDocValues[] numericAggDvs = new SortedNumericDocValues[numAggs];
    for (int i = 0; i < numAggs; i++) { /* ... */ }

    // Direct sequential loop — no Collector overhead
    for (int doc = 0; doc < maxDoc; doc++) {
        // liveDocs check if needed
        long k0 = 0, k1 = 0;
        if (key0Varchar) {
            SortedSetDocValues dv = (SortedSetDocValues) keyReaders[0];
            if (dv != null && dv.advanceExact(doc)) k0 = dv.nextOrd();
        } else { /* numeric path */ }
        // ... same for k1
        int slot = flatMap.findOrInsert(k0, k1);
        // ... accumulate
    }
}
```

**Key advantage:** No Collector/BulkScorer virtual dispatch. Sequential doc iteration `for (doc = 0; doc < maxDoc; doc++)` with direct advanceExact calls.

---

## 4. Comparison: Collector vs Direct Iteration vs Bitset

| Aspect | MatchAllDocsQuery (direct) | Collector path | Proposed bitset path |
|--------|---------------------------|----------------|---------------------|
| Doc iteration | `for (doc=0; doc<maxDoc; doc++)` | Lucene BulkScorer → Collector.collect(doc) | `for (doc = bitSet.nextSetBit(0); doc != -1; ...)` |
| DV access | Sequential advanceExact | Random advanceExact per collected doc | Sequential advanceExact (sorted doc IDs) |
| Overhead | None (tight loop) | Virtual dispatch per doc + BulkScorer | FixedBitSet allocation + population |
| Filter support | MatchAllDocsQuery only | Any Lucene query | Any Lucene query |
| Queries benefiting | Unfiltered | Q31, Q32, Q38, Q41 | Q31(2.64x), Q32(2.23x), Q38(2.98x), Q41(3.28x) |

## 5. Proposed Optimization Strategy

Convert the Collector path (section 2) to a two-phase bitset approach:

**Phase 1:** Pre-collect matching doc IDs into FixedBitSet
```java
FixedBitSet matchingDocs = new FixedBitSet(maxDoc);
engineSearcher.search(query, new Collector() {
    // ... collect(int doc) { matchingDocs.set(doc); }
});
```

**Phase 2:** Iterate FixedBitSet sequentially (same loop structure as MatchAllDocsQuery path)
```java
for (int doc = matchingDocs.nextSetBit(0); doc != -1;
     doc = (doc + 1 < matchingDocs.length()) ? matchingDocs.nextSetBit(doc + 1) : -1) {
    // Same body as MatchAllDocsQuery direct iteration path
}
```

This eliminates:
- BulkScorer virtual dispatch per doc
- Random advanceExact() calls (now sequential)
- Enables the same tight loop as the MatchAllDocsQuery path

**Existing precedent:** TransportShardExecuteAction.java:1555 already uses collect-then-scan with sorted int[]. FixedBitSet is better because:
- O(1) set/test vs O(n) array growth
- `nextSetBit()` gives sequential iteration
- Memory: maxDoc/8 bytes vs 4*matchCount bytes (FixedBitSet wins when selectivity > 50%)
