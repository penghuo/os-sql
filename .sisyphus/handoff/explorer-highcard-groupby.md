# High-Cardinality GROUP BY Path Analysis

## Configuration (line 118-128)
- `PARALLELISM_MODE`: defaults to `"docrange"` (sys prop `dqe.parallelism`)
- `THREADS_PER_SHARD`: `availableProcessors / numLocalShards(default=4)`
- `PARALLEL_POOL`: shared `ForkJoinPool` with `asyncMode=true` (work-stealing)

---

## Q15: `GROUP BY UserID ORDER BY COUNT(*) LIMIT 10`
**Regression: 2.80x | Method: `executeSingleKeyNumericFlat` (line 5143)**

### Data Structure: `FlatSingleKeyMap` (line 13521)
- Open-addressing hash map with linear probing
- `long[] keys` — one slot per group (sentinel: `Long.MIN_VALUE`)
- `long[] accData` — contiguous: slot i at `[i*slotsPerGroup .. (i+1)*slotsPerGroup)`
- `LOAD_FACTOR = 0.7`, `MAX_CAPACITY = 16_000_000`
- Initial capacity: 4,000,000 (pre-sized in parallel path)
- Hash: `SingleKeyHashMap.hash1(key)` with power-of-2 masking

### Inner Loop (line 5643, COUNT(*)-only MatchAll fast path)
```java
long[] keyValues = loadNumericColumn(leafCtx, keyInfos.get(0).name());
for (int doc = 0; doc < maxDoc; doc++) {
    long key0 = keyValues[doc];
    int slot = flatMap.findOrInsert(key0);
    flatMap.accData[slot * slotsPerGroup + accOffset[0]]++;
}
```
- Loads entire key column into `long[]` array (columnar cache)
- Sequential scan, one hash probe per doc

### Parallelism: YES (line 5195-5270)
Two parallel strategies exist:
1. **Doc-range parallel** (MatchAll + COUNT(*) only, no bucketing): splits segments into work units by doc range, each worker builds local `FlatSingleKeyMap`, then merges into largest
2. **Segment-parallel** (general case): greedy largest-first segment assignment across workers, each builds local map, then sequential `mergeFrom()`

### Bottleneck for 17M unique UserIDs
- Map at 70% load → needs ~24.3M capacity → `keys[24M]` + `accData[24M]` = ~390MB
- `mergeFrom()` is O(other.capacity) with random hash probes into target — cache-hostile at this size
- Multiple resize cascades: 4M → 8M → 16M → 32M, each copies all data
- `findOrInsert` linear probing degrades as load factor approaches 0.7

---

## Q32: `GROUP BY WatchID, ClientIP`
**Regression: 3.05x | Method: `executeTwoKeyNumericFlat` (line 6317)**

### Data Structure: `FlatTwoKeyMap` (line 13244)
- Same open-addressing design as FlatSingleKeyMap
- `long[] keys0`, `long[] keys1` — two key arrays
- `long[] accData` — contiguous accumulator storage
- `LOAD_FACTOR = 0.7`, `MAX_CAPACITY = 16_000_000`
- Hash: `TwoKeyHashMap.hash2(key0, key1)` with power-of-2 masking
- Has `findOrInsertCapped()` for LIMIT-without-ORDER-BY early termination

### Inner Loop (line 6700, MatchAll dense lockstep)
```java
int keyDoc0 = dv0.nextDoc(); int keyDoc1 = dv1.nextDoc();
for (int doc = 0; doc < maxDoc; doc++) {
    long key0 = 0, key1 = 0;
    if (keyDoc0 == doc) { key0 = dv0.nextValue(); keyDoc0 = dv0.nextDoc(); }
    if (keyDoc1 == doc) { key1 = dv1.nextValue(); keyDoc1 = dv1.nextDoc(); }
    int slot = flatMap.findOrInsert(key0, key1);
    int base = slot * slotsPerGroup;
    // accumulate per agg...
}
```
- Uses `nextDoc()` lockstep iteration (avoids `advanceExact` binary search)
- DV deduplication: multiple aggs on same column share one DV reader

### Parallelism: YES (line 6400-6500)
- Segment-parallel: same greedy largest-first assignment as Q15
- Each worker builds local `FlatTwoKeyMap`, then sequential `mergeFrom()`
- No doc-range parallel variant for two-key path

### Bottleneck
- WatchID is near-unique (~100M groups possible) — will hit `MAX_CAPACITY = 16M` and throw
- 3 arrays of 16M longs each + accData = massive memory footprint
- `mergeFrom()` iterates full capacity of source map with random probes into target

---

## Q16: `GROUP BY UserID, SearchPhrase ORDER BY COUNT(*) LIMIT 10`
**Regression: 6.85x | Method: `executeWithVarcharKeys` (line 8208)**

### Path Selection (multi-segment)
1. Single-segment → `tryOrdinalIndexedTwoKeyCountStar` (line 9340) or flat `FlatTwoKeyMap` with ordinals
2. Multi-segment → tries `executeMultiSegGlobalOrdFlatTwoKey` (line 12106) for global-ord flat path
3. Falls through to `executeNKeyVarcharParallelDocRange` (line 11771) if `PARALLELISM_MODE=docrange`
4. Final fallback: sequential `MergedGroupKey` path with `LinkedHashMap`

### Key Data Structures
- **Single-segment**: `FlatTwoKeyMap` with varchar ordinals as long keys (line 8340)
- **Multi-segment parallel**: per-worker `HashMap<SegmentGroupKey, AccumulatorGroup>` (line 11870)
  - `SegmentGroupKey`: `long[] values` + `boolean[] nulls`, hash = `31*h + Long.hashCode(v)` (line 12773)
  - `AccumulatorGroup`: object array of `MergeableAccumulator` instances — **heap-allocated per group**
- **Cross-segment merge**: `MergedGroupKey` with `Object[] values` using `BytesRefKey` for varchar (line 12873)

### Parallelism in `executeNKeyVarcharParallelDocRange` (line 11771)
- Per-segment: collects matched docIDs into `int[]`, splits among workers by chunk
- Each worker opens own DV readers, builds local `HashMap<SegmentGroupKey, AccumulatorGroup>`
- Uses `advanceExact(doc)` per key per doc (not lockstep — random access pattern)
- Merge: resolves ordinals → `MergedGroupKey`, merges into global `LinkedHashMap`

### Bottleneck
- **No flat map**: uses `HashMap` + boxed objects, not `FlatTwoKeyMap`, in multi-segment varchar path
- Per-group allocation: `SegmentGroupKey` (long[] clone + boolean[] clone) + `AccumulatorGroup` + `CountStarAccum`
- `advanceExact()` per doc per key = 2 binary searches per doc (vs lockstep `nextDoc()` in numeric path)
- Cross-segment merge materializes `String`/`BytesRefKey` objects for every group

---

## Q18: `GROUP BY UserID, minute(EventTime), SearchPhrase ORDER BY COUNT(*) LIMIT 10`
**Regression: 10.22x | Method: `executeNKeyVarcharPath` (line 10408)**

### Path Selection
1. 3-key check at line 10431: `anyVarcharKey=true` (SearchPhrase is varchar)
2. Single-segment + anyVarchar → tries `FlatThreeKeyMap` (line 10434) — **YES, eligible** if COUNT/SUM/AVG only
3. Multi-segment + anyVarchar → **CANNOT use FlatThreeKeyMap** (ordinals are segment-local)
4. Falls through to `executeNKeyVarcharParallelDocRange` (line 11348)

### FlatThreeKeyMap (line 13396) — single-segment only
- `long[] keys0, keys1, keys2` + `long[] accData`
- `hash3()`: multiplicative hash with `0x9E3779B97F4A7C15L` (Fibonacci hashing)
- `MAX_CAPACITY = 16_000_000`, buckets if totalDocs > MAX_CAPACITY

### Multi-segment path (the actual bottleneck)
- Same as Q16: `HashMap<SegmentGroupKey, AccumulatorGroup>` per worker
- 3 keys → 3x `advanceExact()` calls per doc
- `minute(EventTime)` applies `applyArith(val, "E:minute")` in the inner loop
- `NumericProbeKey` with 3-element arrays, `computeHash()` per doc = 3 multiplications
- Cross-segment merge: 3-element `Object[]` per `MergedGroupKey`

### Bottleneck (worst of all queries)
- 3 keys × advanceExact = 3 binary searches per doc
- HashMap with object keys: ~100+ bytes per group vs ~32 bytes in flat map
- No lockstep iteration possible (varchar key requires advanceExact)
- Highest cardinality: UserID × minute × SearchPhrase could be tens of millions of groups

---

## Summary Table

| Query | Method | HashMap Type | Parallel? | Inner Loop | Key Bottleneck |
|-------|--------|-------------|-----------|------------|----------------|
| Q15 | executeSingleKeyNumericFlat:5143 | FlatSingleKeyMap (long[]) | Yes: segment + doc-range | columnar load + findOrInsert | mergeFrom() cache misses at 17M groups |
| Q16 | executeWithVarcharKeys:8208 → executeNKeyVarcharParallelDocRange:11771 | HashMap<SegmentGroupKey> (objects) | Yes: doc-range chunks | advanceExact per key per doc | No flat map for varchar; per-group heap alloc |
| Q18 | executeNKeyVarcharPath:10408 → executeNKeyVarcharParallelDocRange:11771 | HashMap<SegmentGroupKey> (objects) | Yes: doc-range chunks | 3× advanceExact per doc | 3 keys × binary search; highest object overhead |
| Q32 | executeTwoKeyNumericFlat:6317 | FlatTwoKeyMap (long[][]) | Yes: segment-parallel | nextDoc lockstep | Near-unique WatchID → 16M cap risk; merge cost |

## Recommendations

1. **Q16/Q18 (varchar multi-seg)**: Build a `FlatNKeyMap` with global ordinal remapping for varchar keys across segments — eliminates HashMap + object allocation
2. **Q15/Q32 merge cost**: Use concurrent/partitioned flat maps instead of build-local-then-merge — or use atomic accumulators on a shared map
3. **Q18 advanceExact**: For MatchAll, load varchar ordinal columns into long[] arrays (like numeric columnar cache) to replace advanceExact with array lookup
4. **All queries**: The `mergeFrom()` pattern iterates full capacity (not just size) of source map — skip empty slots faster with a bitset or dense slot list
5. **Existing parallel pattern to extend**: The doc-range parallel in `executeSingleKeyNumericFlat` (line 5210) with `CompletableFuture + PARALLEL_POOL + mergeFrom` is the template — already used in 1-key and 2-key numeric paths, needs extension to varchar N-key with flat storage
