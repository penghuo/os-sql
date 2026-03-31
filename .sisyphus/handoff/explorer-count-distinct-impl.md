# COUNT(DISTINCT) Implementation in FusedGroupByAggregate.java

## 1. No `executeCountDistinctWithHashSets` Method Exists

There is **no** method named `executeCountDistinctWithHashSets` in the file. COUNT(DISTINCT) is handled inline within the existing generic paths, not via a dedicated method.

## 2. Accumulator Type System

Accumulators are dispatched by `accType` integer codes:
- `0` = COUNT (non-star, non-distinct) / COUNT(*)
- `1` = SUM long, `2` = AVG long, `3` = MIN, `4` = MAX
- **`5` = COUNT(DISTINCT)**
- `6` = SUM double, `7` = AVG double

Set at parse time (line ~3546): `accType[i] = spec.isDistinct ? 5 : 0;`

## 3. CountDistinctAccum Class (lines 14118-14168)

```java
private static class CountDistinctAccum implements MergeableAccumulator {
    final Type argType;
    final boolean usePrimitiveLong;
    final LongOpenHashSet longDistinctValues;   // for numeric non-double
    final Set<Object> objectDistinctValues;     // for varchar and double

    CountDistinctAccum(Type argType) {
        this.usePrimitiveLong = !(argType instanceof VarcharType) && !(argType instanceof DoubleType);
        this.longDistinctValues = usePrimitiveLong ? new LongOpenHashSet(16) : null;
        this.objectDistinctValues = usePrimitiveLong ? null : new HashSet<>();
    }
}
```

**Two-branch strategy:**
- Numeric non-double → `LongOpenHashSet` (primitive, no boxing)
- Varchar/Double → `HashSet<Object>` (boxed)

## 4. LongOpenHashSet (dqe/src/main/java/org/opensearch/sql/dqe/operator/LongOpenHashSet.java)

Custom open-addressing hash set for primitive longs:
- Sentinel-based: `EMPTY = Long.MIN_VALUE`, separate `hasZero`/`hasSentinel` booleans
- Murmur3 finalizer for hash distribution
- Linear probing, 0.65 load factor
- `add(long)` → returns boolean, auto-resizes
- `addAll(LongOpenHashSet)` → used for cross-segment merge
- `size()` → returns distinct count
- Initial capacity 16 per group (small to minimize allocation for high-cardinality GROUP BY)

## 5. Per-Document Accumulation (Hot Loop)

In `executeSingleKeyNumeric` (line ~3732) and other paths:

**Numeric args:**
```java
case 5: // COUNT(DISTINCT)
    CountDistinctAccum cda = (CountDistinctAccum) acc;
    if (cda.usePrimitiveLong) cda.longDistinctValues.add(rawVal);
    else cda.objectDistinctValues.add(Double.longBitsToDouble(rawVal));
    break;
```

**Varchar args (line ~3989):**
```java
if (acc instanceof CountDistinctAccum cda) {
    cda.objectDistinctValues.add(val);  // val is String from BytesRef
}
```

## 6. Merge Logic

**MergeableAccumulator interface** (line 13593):
```java
interface MergeableAccumulator { void merge(MergeableAccumulator other); void writeTo(BlockBuilder builder); }
```

**CountDistinctAccum.merge** (line ~14139):
```java
public void merge(MergeableAccumulator other) {
    CountDistinctAccum o = (CountDistinctAccum) other;
    if (usePrimitiveLong) longDistinctValues.addAll(o.longDistinctValues);
    else objectDistinctValues.addAll(o.objectDistinctValues);
}
```

**AccumulatorGroup.merge** (line 13615): iterates all accumulators and calls `merge()` on each.

Cross-segment merge happens in `executeSingleKeyNumeric` via `SingleKeyHashMap` — when same key appears in multiple segments, `AccumulatorGroup.merge()` unions the distinct sets.

## 7. Finalize/Output (writeTo)

```java
public void writeTo(BlockBuilder builder) {
    long count = usePrimitiveLong ? longDistinctValues.size() : objectDistinctValues.size();
    BigintType.BIGINT.writeLong(builder, count);
}
```

Simply emits the set size as a BIGINT.

## 8. Why COUNT(DISTINCT) Cannot Use Flat Accumulators

At line 3548: `if (spec.isDistinct) { accType[i] = 5; canUseFlatAccumulators = false; }`

The flat path (`FlatSingleKeyMap`) stores accumulators as `long[]` arrays — fixed-width slots. COUNT(DISTINCT) requires a **variable-size hash set per group**, which cannot be represented in a flat long[] layout. So COUNT(DISTINCT) always falls through to the `SingleKeyHashMap` path which uses `AccumulatorGroup` objects containing `CountDistinctAccum` instances.

This is the key architectural constraint: the flat path (fastest) is unavailable for COUNT(DISTINCT).

## 9. Execution Path Summary

```
executeSingleKeyNumeric()
  ├── canUseFlatAccumulators=true → executeSingleKeyNumericFlat() [FAST, no COUNT(DISTINCT)]
  └── canUseFlatAccumulators=false → SingleKeyHashMap path [SLOWER, supports COUNT(DISTINCT)]
        ├── Per-doc: switch(accType) case 5 → add to LongOpenHashSet/HashSet
        ├── Cross-segment: AccumulatorGroup.merge() → LongOpenHashSet.addAll()
        └── Output: CountDistinctAccum.writeTo() → emit set.size()
```

## 10. Key Files

| File | Purpose |
|------|---------|
| `dqe/src/main/java/org/opensearch/sql/dqe/shard/source/FusedGroupByAggregate.java` | Main fused operator (14259 lines) |
| `dqe/src/main/java/org/opensearch/sql/dqe/operator/LongOpenHashSet.java` | Primitive long hash set for COUNT(DISTINCT) |
