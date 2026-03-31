# FusedGroupByAggregate: COUNT(DISTINCT) Analysis

## 1. accType=5 (COUNT DISTINCT) Accumulator Logic

**Assignment** (lines ~3547, ~6027, and similar in each dispatch path):
```java
case "COUNT":
    accType[i] = spec.isDistinct ? 5 : 0;
    break;
```

**Accumulation** — three data paths feed into accType=5:

| Data path | Code pattern | Storage |
|-----------|-------------|---------|
| Numeric (non-double) | `cda.longDistinctValues.add(rawVal)` | `LongOpenHashSet` |
| Double | `cda.objectDistinctValues.add(Double.longBitsToDouble(rawVal))` | `HashSet<Object>` |
| VARCHAR | `cda.objectDistinctValues.add(val)` where val is `String` | `HashSet<Object>` |

**Output**: `BigintType.BIGINT.writeLong(builder, count)` — always emits the set size.

## 2. Per-Group LongOpenHashSet Usage

`CountDistinctAccum` (line ~11530) holds **one of two** set types per instance:

```java
private static class CountDistinctAccum implements MergeableAccumulator {
    final boolean usePrimitiveLong;  // true unless VARCHAR or DoubleType
    final LongOpenHashSet longDistinctValues;   // for numeric non-double
    final Set<Object> objectDistinctValues;     // for varchar/double
```

- `usePrimitiveLong = !(argType instanceof VarcharType) && !(argType instanceof DoubleType)`
- Each **group** gets its own `CountDistinctAccum` → its own `LongOpenHashSet(16)` (initial capacity 16)
- `LongOpenHashSet` is an open-addressing set using `long[]` with sentinel `Long.MIN_VALUE`, avoiding `Long` boxing
- **Merge**: `longDistinctValues.addAll(o.longDistinctValues)` — union of sets across segments/workers

## 3. Flat Path Rejection Logic

In `executeSingleKeyNumeric` (line ~3547) and `executeTwoKeyNumeric` (line ~6027):

```java
boolean canUseFlatAccumulators = true;
// ...
case "COUNT":
    if (spec.isDistinct) {
        accType[i] = 5;
        canUseFlatAccumulators = false;  // ← REJECTS flat path
    }
```

**Why accType=5 forces non-flat `AccumulatorGroup`:**

The flat path (`FlatSingleKeyMap`, `FlatTwoKeyMap`) stores ALL accumulator state in a contiguous `long[]` array with fixed slots per group:
- COUNT(*): 1 long slot (count)
- SUM(long): 1 long slot (sum)
- AVG(long): 2 long slots (sum, count)

COUNT(DISTINCT) requires a **variable-size set** per group (`LongOpenHashSet` or `HashSet<Object>`). This cannot be represented as a fixed number of `long` slots in the flat array. Therefore, any query with COUNT(DISTINCT) falls back to the object-based `SingleKeyHashMap`/`TwoKeyHashMap` with `AccumulatorGroup` containing `CountDistinctAccum` objects.

Same rejection applies for: MIN, MAX, SUM(double), AVG(double), VARCHAR args.

## 4. What Would Need to Change for Flat Path COUNT(DISTINCT)

To support COUNT(DISTINCT) in the flat `long[]` path:

1. **Hybrid storage**: Keep the flat `long[]` for COUNT/SUM/AVG accumulators, but add a parallel `LongOpenHashSet[]` array indexed by slot for COUNT(DISTINCT) columns. Each slot's distinct set lives outside the contiguous `long[]`.

2. **Accumulator layout**: Add a new accType (e.g., accType=5 in flat mode) that:
   - Allocates 0 slots in `accData` (no fixed-width state)
   - Uses a side-car `LongOpenHashSet[] distinctSets` array at `[slot]`
   - On `findOrInsert`, lazily initializes `distinctSets[slot]` on first access

3. **Merge**: `mergeFrom()` would need to union the `LongOpenHashSet` at each slot.

4. **Output**: `writeTo` reads `distinctSets[slot].size()` instead of `accData[offset]`.

5. **Memory**: The flat map's memory advantage is partially lost since each group still allocates a `LongOpenHashSet`. The benefit is eliminating `AccumulatorGroup` + `MergeableAccumulator[]` wrapper objects (~3 objects/group saved).

## 5. executeInternal Dispatch for GROUP BY + COUNT(DISTINCT)

`executeInternal` (line ~1340) dispatches based on key types:

```
executeInternal
  ├─ hasVarchar?
  │   ├─ single VARCHAR key + COUNT(*) only → executeSingleVarcharCountStar (NO CD support)
  │   ├─ single VARCHAR key + general aggs → executeSingleVarcharGeneric ✅ handles CD
  │   ├─ hasEvalKey → executeWithEvalKeys ✅ handles CD
  │   └─ else → executeWithVarcharKeys ✅ handles CD (N-key path)
  └─ numeric only
      ├─ 1 key, no expr → executeSingleKeyNumeric
      │   ├─ canUseFlatAccumulators=true → executeSingleKeyNumericFlat (NO CD)
      │   └─ canUseFlatAccumulators=false → SingleKeyHashMap path ✅ handles CD
      ├─ 2 keys, no expr → executeTwoKeyNumeric
      │   ├─ canUseFlatAccumulators=true → executeTwoKeyNumericFlat (NO CD)
      │   └─ canUseFlatAccumulators=false → TwoKeyHashMap path ✅ handles CD
      └─ 3+ keys or expr → executeNumericOnly (HashMap path) ✅ handles CD
```

**Key methods that handle COUNT(DISTINCT):**
- `executeSingleVarcharGeneric` — via `collectVarcharGenericAccumulate` switch on accType=5
- `executeSingleKeyNumeric` (non-flat) — via `collectSingleKeyNumericDoc` switch on accType=5
- `executeTwoKeyNumeric` (non-flat) — via `accumulateDocNumeric` switch on accType=5
- `executeWithVarcharKeys` (N-key) — inline switch on accType=5 in LeafCollector
- `executeWithEvalKeys` — inline switch on accType=5
- `executeWithExpressionKey` — inline switch on accType=5
- `executeNKeyVarcharPath` — inline switch on accType=5

**Methods that CANNOT handle COUNT(DISTINCT):**
- `executeSingleVarcharCountStar` — COUNT(*) only
- `executeSingleKeyNumericFlat` — flat long[] only
- `executeTwoKeyNumericFlat` — flat long[] only
- `executeThreeKeyFlat` — flat long[] only
- `executeDerivedSingleKeyNumeric` (flat path) — flat long[] only
