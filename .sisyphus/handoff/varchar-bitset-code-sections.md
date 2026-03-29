# Code Sections from FusedGroupByAggregate.java

Source: `dqe/src/main/java/org/opensearch/sql/dqe/shard/source/FusedGroupByAggregate.java`

---

## Section 1: Varchar Filtered Branch (Collector-based) — `executeSingleVarcharGeneric()`

**Lines 2096–2140** — The `else` branch handling non-MatchAll queries. Currently uses a Collector (no bitset lockstep optimization).

```java
// Line 2096
            }
          } else {
            // General path: use Lucene's search framework with Collector
            engineSearcher.search(
                query,
                new Collector() {
                  @Override
                  public LeafCollector getLeafCollector(LeafReaderContext context)
                      throws IOException {
                    return new LeafCollector() {
                      @Override
                      public void setScorer(Scorable scorer) {}

                      @Override
                      public void collect(int doc) throws IOException {
                        if (dv == null || !dv.advanceExact(doc)) return;
                        int ord = (int) dv.nextOrd();
                        AccumulatorGroup accGroup = ordGroups[ord];
                        if (accGroup == null) {
                          accGroup = createAccumulatorGroup(specs);
                          ordGroups[ord] = accGroup;
                        }
                        collectVarcharGenericAccumulate(
                            doc,
                            accGroup,
                            numAggs,
                            isCountStar,
                            isVarcharArg,
                            isDoubleArg,
                            accType,
                            numericAggDvs,
                            varcharAggDvs);
                      }
                    };
                  }

                  @Override
                  public ScoreMode scoreMode() {
                    return ScoreMode.COMPLETE_NO_SCORES;
                  }
                });
          }
```

---

## Section 2: Bitset Lockstep in `executeSingleKeyNumericFlat()` — The Reference Implementation

**Lines 4967–5049** — Selectivity check, FixedBitSet collection, and lockstep iteration loop. This is the pattern to port to the VARCHAR path.

```java
// Line 4967
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
      boolean useBitsetLockstep = estCount >= 0 && estCount < maxDoc / 2
          && dv0 != null && !hasApplyLen;

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

## Section 3: `collectVarcharGenericAccumulate()` Method — Lines 12635–12685

**Lines 12635–12685** — Method signature and accumulation logic for VARCHAR group-by aggregates. Uses `advanceExact(doc)` for random-access doc values.

```java
// Line 12635
  private static void collectVarcharGenericAccumulate(
      int doc,
      AccumulatorGroup accGroup,
      int numAggs,
      boolean[] isCountStar,
      boolean[] isVarcharArg,
      boolean[] isDoubleArg,
      int[] accType,
      SortedNumericDocValues[] numericAggDvs,
      SortedSetDocValues[] varcharAggDvs)
      throws IOException {
    for (int i = 0; i < numAggs; i++) {
      MergeableAccumulator acc = accGroup.accumulators[i];
      if (isCountStar[i]) {
        ((CountStarAccum) acc).count++;
        continue;
      }
      if (isVarcharArg[i]) {
        SortedSetDocValues varcharDv = varcharAggDvs[i];
        if (varcharDv != null && varcharDv.advanceExact(doc)) {
          BytesRef bytes = varcharDv.lookupOrd(varcharDv.nextOrd());
          String val = bytes.utf8ToString();
          switch (accType[i]) {
            case 5:
              ((CountDistinctAccum) acc).objectDistinctValues.add(val);
              break;
            case 3:
              MinAccum ma = (MinAccum) acc;
              if (!ma.hasValue || val.compareTo((String) ma.objectVal) < 0) {
                ma.objectVal = val;
                ma.hasValue = true;
              }
              break;
            case 4:
              MaxAccum xa = (MaxAccum) acc;
              if (!xa.hasValue || val.compareTo((String) xa.objectVal) > 0) {
                xa.objectVal = val;
                xa.hasValue = true;
              }
              break;
          }
        }
        continue;
      }
      SortedNumericDocValues aggDv = numericAggDvs[i];
      if (aggDv != null && aggDv.advanceExact(doc)) {
        long rawVal = aggDv.nextValue();
        switch (accType[i]) {
          case 0:
            ((CountStarAccum) acc).count++;
            break;
```

---

## Key Observations for Porting Bitset Lockstep to VARCHAR

1. **Numeric path (Section 2)** uses `weight.scorer()` + `weight.count()` for selectivity estimation, then `FixedBitSet` + `nextDoc()` lockstep for key DV and agg DVs.
2. **VARCHAR path (Section 1)** currently uses `engineSearcher.search()` with an anonymous `Collector` — no selectivity check, no bitset optimization.
3. **VARCHAR accumulate (Section 3)** uses `advanceExact(doc)` for both `SortedSetDocValues` (varchar aggs) and `SortedNumericDocValues` (numeric aggs). The bitset lockstep port will need to handle `SortedSetDocValues` key DV via `nextDoc()`/`nextOrd()` instead of `advanceExact()`.
4. The numeric lockstep tracks `keyDoc` position and `aggDocPos[]` array — the VARCHAR port needs analogous tracking for the `SortedSetDocValues` key iterator.
