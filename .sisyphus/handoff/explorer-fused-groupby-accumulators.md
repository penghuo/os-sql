# FusedGroupByAggregate: Flat vs Non-Flat Path Analysis

## 1. canFuse() — Line 198
Enables fused GROUP BY when:
- All group-by keys are VARCHAR, numeric, or timestamp (or supported expressions: DATE_TRUNC, EXTRACT, arithmetic, EvalNode CASE WHEN)
- All aggregate functions match `AGG_FUNCTION` pattern (COUNT, SUM, AVG, MIN, MAX)
- Aggregate arguments are physical columns, `*`, or `length(varchar_col)`
- Child plan has a TableScanNode (possibly via EvalNode)

`canFuseWithExpressionKey()` (line 314) is a separate check for single computed-expression key over VARCHAR (ordinal-cached expression evaluation, e.g. REGEXP_REPLACE).

## 2. Dispatch Tree (executeInternal, line 1280)

```
executeInternal
├── hasVarchar?
│   ├── 1 VARCHAR key + COUNT(*) only → executeSingleVarcharCountStar (line 1616)
│   │   └── Single-segment: long[] ordCounts[ordinal]++ — ZERO allocation hot loop
│   │   └── Multi-segment: BytesRefKey HashMap or global ordinals
│   ├── 1 VARCHAR key + general aggs → executeSingleVarcharGeneric (line 2293)
│   │   └── Single-segment: AccumulatorGroup[] ordGroups[ordinal] — O(1) lookup
│   │   └── Multi-segment: BytesRefKey HashMap with parallel workers
│   ├── hasEvalKey → executeWithEvalKeys (line 7170) ← Q39 PATH
│   │   └── Per-doc Block materialization in 256-row micro-batches
│   │   └── Single-segment + all-long-keys: flat open-addressing long[] map
│   │   └── Otherwise: LinkedHashMap<ProbeGroupKey, AccumulatorGroup>
│   └── else → executeWithVarcharKeys (line 8218)
│       └── Single-segment 2-key: FlatTwoKeyMap (ordinal-indexed)
│       └── Multi-segment: BytesRefKey HashMap with parallel workers
└── numeric only → executeNumericOnly (line 3159)
    ├── 1 key → executeSingleKeyNumeric (line 3519)
    │   ├── canUseFlatAccumulators? → executeSingleKeyNumericFlat (line 5143)
    │   │   └── FlatSingleKeyMap: long[] keys + long[] accData (contiguous)
    │   │   └── Parallel: segment-parallel or doc-range-parallel with mergeFrom
    │   │   └── >10M docs: SEQUENTIAL scan (avoids L3 thrashing from parallel maps)
    │   └── else → SingleKeyHashMap with AccumulatorGroup per slot
    ├── 2 keys → executeTwoKeyNumeric (uses FlatTwoKeyMap or TwoKeyHashMap)
    └── 3+ keys → generic HashMap path
```

## 3. Flat Path Data Structures

### FlatSingleKeyMap (line 13531)
- `long[] keys` + `long[] accData` (contiguous, slot-indexed)
- Open-addressing with linear probing, Murmur3 hash
- MAX_CAPACITY = 16M, LOAD_FACTOR = 0.7
- `findOrInsert(long key)` → returns slot index, caller does `accData[slot*slotsPerGroup + offset]++`
- `mergeFrom()` for parallel worker map merging

### FlatTwoKeyMap (line 13254)
- `long[] keys0` + `long[] keys1` + `long[] accData`
- Same open-addressing scheme, MAX_CAPACITY = 16M
- `findOrInsertCapped()` for LIMIT-without-ORDER-BY early close

### canUseFlatAccumulators (line 3536)
Flat path requires ALL aggregates be representable as longs:
- COUNT(*) ✓, COUNT(col) ✓, SUM(long) ✓, AVG(long) ✓
- **Disqualifiers**: COUNT(DISTINCT), SUM(double), AVG(double), MIN, MAX, VARCHAR args

## 4. Hot Loops

### Flat path (fastest) — scanDocRangeFlatSingleKeyCountStar (line 14278)
```java
for (int doc = startDoc; doc < endDoc; doc++) {
    int slot = flatMap.findOrInsert(keyValues[doc]);
    flatMap.accData[slot * slotsPerGroup + accOffset0]++;
}
```
Pure long[] operations, zero object allocation, columnar pre-loaded keys.

### Ordinal-indexed path — executeSingleVarcharGeneric (line 2375)
```java
while ((doc = sdv.nextDoc()) != NO_MORE_DOCS) {
    int ord = sdv.ordValue();
    AccumulatorGroup accGroup = ordGroups[ord];  // O(1) array lookup
    if (accGroup == null) { accGroup = createAccumulatorGroup(specs); ordGroups[ord] = accGroup; }
    collectVarcharGenericAccumulate(doc, accGroup, ...);
}
```
O(1) ordinal lookup, but AccumulatorGroup objects allocated per unique group.

### Non-flat HashMap path — executeWithEvalKeys (line 7870)
```java
probeKey.computeHash();
accGroup = globalGroups.get(probeKey);  // LinkedHashMap lookup
if (accGroup == null) {
    MergedGroupKey mgk = probeKey.snapshot();  // Object[] allocation
    accGroup = createAccumulatorGroup(specs);   // AccumulatorGroup + MergeableAccumulator[] alloc
    globalGroups.put(mgk, accGroup);
}
```
Per-group: MergedGroupKey (Object[]), AccumulatorGroup, MergeableAccumulator[] — heavy allocation.

## 5. Why Q39 is 15x Slower

Q39 profile: filtered query (CounterID=62) with 5 GROUP BY keys including CASE WHEN expression.

Path taken: `executeWithEvalKeys` (line 7170) because it has an eval key.

**Key bottlenecks:**
1. **Block materialization per micro-batch (256 rows)**: For each batch, builds BlockBuilder arrays for eval source columns, evaluates CASE WHEN expression via `BlockExpression.evaluate(Page)`. This involves Slice/BytesRef allocation for VARCHAR columns.
2. **Per-doc key resolution**: 5 keys resolved per doc — mix of DocValues reads, expression evaluation, and inline CASE WHEN condition checking.
3. **HashMap grouping**: With 5 keys, uses `LinkedHashMap<ProbeGroupKey, AccumulatorGroup>` (or flat long[] map if all keys resolve to longs in single-segment). Each new group allocates MergedGroupKey(Object[5]) + AccumulatorGroup.
4. **No parallelism**: `executeWithEvalKeys` has NO parallel execution path — it's purely sequential across segments.
5. **Inline CASE WHEN optimization** (line 7340-7410): Detects simple `CASE WHEN (col=const) THEN varchar_col ELSE '' END` and bypasses Block evaluation, but still requires per-doc condition checking across all condition columns.

**Contrast with fast queries**: Q15/Q16 use `executeSingleKeyNumericFlat` with FlatSingleKeyMap — pure long[] arrays, parallel workers, zero per-group allocation.

## 6. Parallel Execution Paths

| Path | Parallel? | Mechanism |
|------|-----------|-----------|
| executeSingleKeyNumericFlat | ✅ | Segment-parallel or doc-range-parallel ForkJoinPool |
| executeSingleKeyNumericFlatMultiBucket | ✅ | Segment-parallel |
| executeSingleVarcharCountStar | ✅ | Segment-parallel (multi-segment path) |
| executeSingleVarcharGeneric | ✅ | Segment-parallel (multi-segment path) |
| executeWithVarcharKeys | ✅ | Segment-parallel (multi-segment path) |
| executeWithEvalKeys | ❌ | **Sequential only** |
| executeWithExpressionKey | ✅ | Segment-parallel (line 853) |

Config: `THREADS_PER_SHARD = availableProcessors / dqe.numLocalShards(4)`, ForkJoinPool with asyncMode=true.

**High-cardinality threshold** (line 5217): When totalDocs > 10M, flat numeric path falls back to sequential scan to avoid L3 cache thrashing from parallel worker maps.

## 7. Numeric vs VARCHAR Key Handling

- **Numeric keys**: Read via `SortedNumericDocValues.nextValue()` → raw long. Stored directly in `long[]` keys arrays. Hash via Murmur3 finalizer.
- **VARCHAR keys (single-segment)**: Read via `SortedSetDocValues.nextOrd()` → ordinal (long). Used as array index (O(1)) or stored as long in flat maps. String resolution deferred to output phase.
- **VARCHAR keys (multi-segment)**: Ordinals are segment-local. Two strategies:
  1. **Global ordinals**: Build OrdinalMap, convert segment ordinals to global ordinals (longs), enabling flat long map path.
  2. **BytesRefKey HashMap**: Resolve ordinal to BytesRef immediately, use as HashMap key. More allocation but works when global ordinals are too expensive.
