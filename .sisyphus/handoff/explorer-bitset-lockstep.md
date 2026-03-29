# Bitset Lockstep Analysis in FusedGroupByAggregate.java

## 1. Disabled Flag Locations

Both methods have the same disabled flag:

- **Two-key**: Line ~5940: `boolean useBitsetLockstep = false; // Disabled: causes EOFException on some DocValues`
- **Single-key**: Line ~4990: `boolean useBitsetLockstep = false; // Disabled: causes EOFException on some DocValues`

Both are in the **filtered path** (the `else` branch of `if (isMatchAll)`), after `Scorer scorer = weight.scorer(leafCtx)`.

## 2. Two Paths in Each Method

Each `scanSegmentFlat{Single,Two}Key` has:

```
if (isMatchAll) {
  // UNFILTERED PATH — iterates doc=0..maxDoc
  //   Has nextDoc() lockstep that WORKS (allDense / allDenseDedup)
  //   Sequential iteration: every doc visited in order
} else {
  // FILTERED PATH — uses Weight+Scorer
  if (useBitsetLockstep) {   // <-- DISABLED
    // Collect scorer into FixedBitSet, then iterate set bits with nextDoc() lockstep
  } else {
    // Scorer-based iteration with advanceExact() per doc (current active path)
  }
}
```

## 3. How the Bitset Lockstep Iterates DocValues

The bitset path uses `nextDoc()` lockstep with **skip-forward** via while-loops:

```java
// Two-key filtered bitset path (line ~5960-5990):
for (int doc = matchingDocs.nextSetBit(0); doc >= 0;
    doc = matchingDocs.nextSetBit(doc + 1)) {
  // Advance key iterators to current doc
  while (keyDoc0 != NO_MORE_DOCS && keyDoc0 < doc) { keyDoc0 = dv0.nextDoc(); }
  if (keyDoc0 == doc) { key0 = dv0.nextValue(); keyDoc0 = dv0.nextDoc(); }
  // Same for keyDoc1, uniqueDvDocs[d], aggDocs[i]...
  while (uniqueDvDocs[d] != NO_MORE_DOCS && uniqueDvDocs[d] < doc) {
    uniqueDvDocs[d] = uniqueDvReaders[d].nextDoc();
  }
}
```

**Key difference from the working unfiltered path:**
- Unfiltered: iterates `doc = 0, 1, 2, ..., maxDoc-1` — every doc in order, DV iterators always advance by exactly 1
- Bitset: iterates only set bits — **gaps between docs** require the `while (pos < doc) nextDoc()` skip loops

## 4. Single-Key Bitset Lockstep — Same Pattern, Same Bug

The single-key filtered path (line ~4990-5040) has identical structure:

```java
boolean useBitsetLockstep = false; // Disabled: causes EOFException on some DocValues
if (useBitsetLockstep) {
  FixedBitSet matchingDocs = new FixedBitSet(maxDoc);
  // ... collect scorer into bitset ...
  for (int doc = matchingDocs.nextSetBit(0); doc >= 0; ...) {
    while (keyDoc != NO_MORE_DOCS && keyDoc < doc) { keyDoc = dv0.nextDoc(); }
    // ... same skip-forward pattern for aggDocPos[i] ...
  }
}
```

Also disabled with the same comment. Same bug.

## 5. Root Cause Analysis: Why EOFException

The unfiltered lockstep (MatchAll path) works because:
- It iterates `doc = 0..maxDoc-1` sequentially
- For each doc, it checks `if (keyDoc0 == doc)` and calls `nextValue()` then `nextDoc()`
- The DV iterator is always exactly at or ahead of the current doc — never needs to skip

The bitset lockstep fails because:
- It iterates only matching docs (gaps between set bits)
- The `while (pos < doc) nextDoc()` skip loop calls `nextDoc()` repeatedly to catch up
- **BUG**: When `nextDoc()` returns `NO_MORE_DOCS` (all DV entries exhausted), the code continues to the next bitset doc and calls `nextDoc()` again on an exhausted iterator
- Lucene's `SortedNumericDocValues.nextDoc()` throws `EOFException` when called after `NO_MORE_DOCS`

**Specifically, the while-loop guard `uniqueDvDocs[d] != NO_MORE_DOCS` should prevent this, BUT:**
- After `nextValue()` + `nextDoc()` on the matching case, if `nextDoc()` returns `NO_MORE_DOCS`, the tracker is set to `NO_MORE_DOCS`
- On the NEXT bitset doc, the while-loop correctly skips (guard catches it)
- The `if (pos == doc)` check correctly fails
- So the guard logic looks correct...

**Alternative root cause**: The issue may be with **sparse DocValues columns** where the DV iterator has fewer entries than the segment's maxDoc. When the bitset contains docs beyond the last DV entry:
1. The skip loop advances past all DV entries → `nextDoc()` returns `NO_MORE_DOCS`
2. But if a column is **not present for any doc** (null DV reader), the `allDense` check should catch it
3. The real issue: a column exists but is **sparse** — has values for some docs but not all. The `allDense` check only verifies the reader is non-null, not that it covers all docs.

**Most likely root cause**: After `nextDoc()` returns `NO_MORE_DOCS` and the value is consumed via `nextValue()`, calling `nextDoc()` again on the exhausted iterator throws EOFException. Look at this sequence in the inner loop:

```java
if (uniqueDvDocs[d2] == doc) {
  dvValues[d2] = uniqueDvReaders[d2].nextValue();  // consume value
  dvHasValue[d2] = true;
  uniqueDvDocs[d2] = uniqueDvReaders[d2].nextDoc(); // advance — could return NO_MORE_DOCS
}
```

If `nextDoc()` here returns `NO_MORE_DOCS`, then on the next outer iteration, the while-loop guard works. But the issue is that `nextValue()` is called on a doc that was reached via the skip-forward loop — and `nextDoc()` positions the iterator AT the doc but doesn't guarantee `nextValue()` is valid unless `advanceExact()` returned true. **`nextDoc()` positions to the next doc WITH a value, so `nextValue()` should be valid.** This path should be safe.

**Revised root cause**: The bug is likely in the **key DV iteration**, not the agg DVs. Look at:

```java
while (keyDoc0 != NO_MORE_DOCS && keyDoc0 < doc) { keyDoc0 = dv0.nextDoc(); }
long key0 = 0;
if (keyDoc0 == doc) { key0 = dv0.nextValue(); keyDoc0 = dv0.nextDoc(); }
```

If `dv0` is a sparse column (not all docs have key values), and the bitset contains a doc AFTER the last key entry, `dv0.nextDoc()` inside the while-loop returns `NO_MORE_DOCS`. The guard catches it. But `dv0` was obtained fresh at the top of the method — **wait, in the filtered path, `dv0` is obtained BEFORE the scorer iteration, then the scorer consumes the iterator to build the bitset, then the DV iteration starts.** The DV iterators (`dv0`, `dv1`, `uniqueDvReaders`) are obtained fresh and unused — this should be fine.

**Final hypothesis**: The EOFException may come from calling `nextDoc()` on a `SortedNumericDocValues` that has already been fully consumed (returned `NO_MORE_DOCS`). In Lucene, calling `nextDoc()` after `NO_MORE_DOCS` is undefined behavior and some implementations throw. The while-loop guard should prevent this, but there may be an edge case where the guard variable (`keyDoc0`) is stale or the `nextDoc()` call inside `if (keyDoc0 == doc)` returns `NO_MORE_DOCS` and then the NEXT iteration's while-loop doesn't execute (because `keyDoc0 == NO_MORE_DOCS`), which is correct. The logic appears sound on paper — the bug may be in a specific Lucene codec implementation that throws on repeated access patterns.
