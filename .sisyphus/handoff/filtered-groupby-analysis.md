# Filtered GROUP BY Analysis: Q14 & Q30 Optimization Opportunities

## Execution Path Summary

Both Q14 and Q30 have `WHERE SearchPhrase <> ''` (highly selective, ~1.5% of docs) and 2 group-by keys with one VARCHAR.

**Dispatch chain:** `executeInternal` (L940) → `hasVarchar=true`, `keyInfos.size()==2` → `executeWithVarcharKeys` (L7285) → single-segment 2-key flat path → `canUseFlatVarchar=true` → **FlatTwoKeyMap** with flat `long[]` accumulators.

**Filtered path dispatch (L7773-7774):**
```java
int estCount = weight.count(leafCtx);
boolean useDirectBitset = (estCount >= 0 && estCount < maxDoc / 2);
```
Since SearchPhrase <> '' matches ~1.5% of docs → **bitset path is taken**.

## Current Bitset Path (L7775-7849)

1. **Pass 1:** Collect matching doc IDs into `FixedBitSet` via scorer DISI iteration
2. **Pass 2:** Iterate `matchingDocs.nextSetBit()`, calling `advanceExact(doc)` per doc per column

For Q14 (COUNT(*) only): 2 advanceExact calls per doc (2 keys, 0 agg DV reads)
For Q30 (COUNT(*) + SUM + AVG): 2 advanceExact calls for keys + 2 advanceExact calls for agg columns = **4 advanceExact per doc**

## Optimization Opportunities

### OPT-1: DISI-Direct Iteration (Eliminate Bitset Pass) — Est. 3-5%

The bitset path does two passes: (1) collect into FixedBitSet, (2) iterate nextSetBit + advanceExact. For selective filters (~1.5%), we can use the scorer's DISI directly:

```java
DocIdSetIterator disi = scorer.iterator();
for (int doc = disi.nextDoc(); doc != NO_MORE_DOCS; doc = disi.nextDoc()) {
    // advanceExact for keys and aggs (same as current)
}
```

This eliminates:
- FixedBitSet allocation (`maxDoc / 8` bytes, ~12.5MB for 100M docs)
- First pass DISI iteration to populate bitset
- nextSetBit overhead (bit scanning)

The Collector path (L7851) already does single-pass but has virtual dispatch overhead. A DISI-direct path gets single-pass without Collector overhead.

### OPT-2: nextDoc() Lockstep for DocValues (Replace advanceExact) — Est. 2-4%

The numeric-only path (L3393, L4874) already uses nextDoc() lockstep for dense columns, avoiding advanceExact's binary search. The 2-key varchar bitset path does NOT use this.

For the filtered path, since docs come in order from DISI/bitset, we can use lockstep:

```java
int keyDoc0 = numericDv0.nextDoc(); // SearchEngineID (numeric key)
int keyDoc1 = varcharDv1.nextDoc(); // ClientIP/SearchPhrase (varchar key)
int aggDoc0 = aggDv0.nextDoc();     // IsRefresh
int aggDoc1 = aggDv1.nextDoc();     // ResolutionWidth

for (int doc = disi.nextDoc(); doc != NO_MORE_DOCS; doc = disi.nextDoc()) {
    // Advance key0 (numeric)
    while (keyDoc0 < doc) keyDoc0 = numericDv0.nextDoc();
    long k0 = (keyDoc0 == doc) ? numericDv0.nextValue() : 0;
    // Advance key1 (varchar)  
    while (keyDoc1 < doc) keyDoc1 = varcharDv1.nextDoc();
    long k1 = (keyDoc1 == doc) ? varcharDv1.nextOrd() : 0;
    // ... same for agg columns
}
```

**Key insight:** `advanceExact()` does a binary search within the DocValues block. `nextDoc()` is a forward-only scan that's cheaper when docs are visited in order (which they always are from DISI/bitset). For Q30 with 4 DV columns, this saves 4 binary searches per doc.

**Caveat:** SortedSetDocValues doesn't expose `nextDoc()` directly in the same way — need to check if `DocValues.unwrapSingleton()` works for the varchar key to get SortedDocValues which extends DISI.

### OPT-3: Combine DISI + Lockstep (OPT-1 + OPT-2) — Est. 5-8%

Combining both: single-pass DISI iteration with nextDoc lockstep for all DocValues columns. This is the pattern already used in the numeric-only MatchAll path (L4874) but adapted for:
1. Filtered queries (DISI instead of 0..maxDoc)
2. VARCHAR keys (SortedSetDocValues ordinals)

### OPT-4: Redundant truncUnits/arithUnits Checks — Est. <1%

In the bitset inner loop (L7807-7830), every doc checks `truncUnits[i]` and `arithUnits[i]` even though Q14/Q30 have plain column keys (no DATE_TRUNC or arithmetic). These are pre-computed as null but the branch is still evaluated per doc. Could hoist the check outside the loop with specialized code paths, but the branch predictor likely handles this well.

### OPT-5: Bitset Lockstep Already Exists but Disabled (L5044)

```java
boolean useBitsetLockstep = false; // Disabled: needs more testing
```

The numeric-only single-key path at L5044 has a bitset+lockstep implementation that's **disabled**. It collects into FixedBitSet then uses nextDoc lockstep for DocValues. This is exactly the pattern needed but for the 2-key varchar case. Enabling and adapting this could be the quickest win.

## Recommendation

**Highest impact:** Implement OPT-3 (DISI-direct + nextDoc lockstep) for the 2-key flat varchar filtered path. This combines:
- Eliminating the bitset materialization pass (saves memory + one full DISI scan)
- Replacing advanceExact with nextDoc lockstep (saves binary search per DV column per doc)

For Q14: saves 1 pass + 2 advanceExact→nextDoc per doc
For Q30: saves 1 pass + 4 advanceExact→nextDoc per doc

**Implementation location:** L7775-7849 in `executeWithVarcharKeys`, replace the bitset block with DISI-direct + lockstep. Use `DocValues.unwrapSingleton()` for varchar keys to get SortedDocValues (which is a DISI) for lockstep iteration.
