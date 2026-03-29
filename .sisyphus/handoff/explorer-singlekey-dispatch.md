# Single-Key Numeric Dispatch Chain: executeInternal → executeNumericOnly → executeSingleKeyNumeric → executeSingleKeyNumericFlat

**File**: `dqe/src/main/java/org/opensearch/sql/dqe/shard/source/FusedGroupByAggregate.java`

---

## 1. Dispatch Chain Overview

```
executeInternal (line 937)
  └─ if (!hasVarchar) → executeNumericOnly (line 1110→2811)
       └─ if (keyInfos.size() == 1 && !anyTrunc) → executeSingleKeyNumeric (line 2849→3171)
            └─ if (canUseFlatAccumulators) → executeSingleKeyNumericFlat (line 3242→4269)
```

---

## 2. executeInternal → executeNumericOnly (lines 1100-1113)

At the end of `executeInternal`, after classifying keys as varchar or numeric:

```java
    // line ~1100
    } else {
      return executeNumericOnly(
          shard,
          query,
          keyInfos,
          specs,
          columnTypeMap,
          groupByKeys,
          sortAggIndex,
          sortAscending,
          topN);
    }
```

**Condition**: `!hasVarchar` — all group-by keys are numeric types (no VarcharType).

---

## 3. executeNumericOnly → executeSingleKeyNumeric (lines 2811-2860)

`executeNumericOnly` (line 2811) first pre-computes expression metadata (DATE_TRUNC, arith, EXTRACT), then dispatches:

```java
    // line ~2843
    final boolean anyTrunc = hasExpr;

    // Fast path: single numeric key with no expressions
    if (keyInfos.size() == 1 && !anyTrunc) {
      return executeSingleKeyNumeric(
          shard, query, keyInfos, specs, columnTypeMap, groupByKeys,
          sortAggIndex, sortAscending, topN);
    }
```

**Condition**: Exactly 1 group-by key AND no DATE_TRUNC/arith/EXTRACT expressions.

Other branches in `executeNumericOnly`:
- `keyInfos.size() == 2 && !anyTrunc` → `executeTwoKeyNumeric` (line ~2858)
- `anyTrunc && keyInfos.size() > 1` with all-same-column arith → `executeDerivedSingleKeyNumeric` (line ~2880)
- Fallback → generic `SegmentGroupKey`-based HashMap path (line ~2900+)

---

## 4. executeSingleKeyNumeric (line 3171) — Full Method

### 4a. Signature (lines 3171-3182)

```java
  private static List<Page> executeSingleKeyNumeric(
      IndexShard shard,
      Query query,
      List<KeyInfo> keyInfos,
      List<AggSpec> specs,
      Map<String, Type> columnTypeMap,
      List<String> groupByKeys,
      int sortAggIndex,
      boolean sortAscending,
      long topN)
      throws Exception {
```

### 4b. canUseFlatAccumulators Check (lines 3188-3237)

This loop classifies each aggregate and determines if the flat (long[]-based) path can be used:

```java
    final int numAggs = specs.size();
    final boolean[] isCountStar = new boolean[numAggs];
    final boolean[] isDoubleArg = new boolean[numAggs];
    final int[] accType = new int[numAggs];
    boolean canUseFlatAccumulators = true;

    for (int i = 0; i < numAggs; i++) {
      AggSpec spec = specs.get(i);
      isCountStar[i] = "*".equals(spec.arg);
      isDoubleArg[i] = spec.argType instanceof DoubleType;
      if (isCountStar[i]) {
        accType[i] = 0;                          // COUNT(*)
      } else {
        switch (spec.funcName) {
          case "COUNT":
            if (spec.isDistinct) {
              accType[i] = 5;                     // COUNT(DISTINCT) → NOT flat
              canUseFlatAccumulators = false;
            } else {
              accType[i] = 0;                     // COUNT(col) → flat OK
            }
            break;
          case "SUM":
            if (isDoubleArg[i]) {
              accType[i] = 6;                     // SUM(double) → NOT flat
              canUseFlatAccumulators = false;
            } else {
              accType[i] = 1;                     // SUM(long) → flat OK
            }
            break;
          case "AVG":
            if (isDoubleArg[i]) {
              accType[i] = 7;                     // AVG(double) → NOT flat
              canUseFlatAccumulators = false;
            } else {
              accType[i] = 2;                     // AVG(long) → flat OK
            }
            break;
          case "MIN":
            accType[i] = 3;                       // MIN → always NOT flat
            canUseFlatAccumulators = false;
            break;
          case "MAX":
            accType[i] = 4;                       // MAX → always NOT flat
            canUseFlatAccumulators = false;
            break;
        }
        if (spec.argType instanceof VarcharType) {
          canUseFlatAccumulators = false;          // VARCHAR args → NOT flat
        }
      }
    }
```

**Flat-eligible accTypes** (representable as longs):
| accType | Aggregate | Slots per group |
|---------|-----------|-----------------|
| 0 | COUNT(*) or COUNT(col) | 1 (count) |
| 1 | SUM(long) | 1 (longSum) |
| 2 | AVG(long) | 2 (longSum + count) |

**Non-flat accTypes** (require object accumulators):
| accType | Aggregate | Reason |
|---------|-----------|--------|
| 3 | MIN | Needs hasValue flag + type dispatch |
| 4 | MAX | Needs hasValue flag + type dispatch |
| 5 | COUNT(DISTINCT) | Needs HashSet |
| 6 | SUM(double) | Needs double accumulation |
| 7 | AVG(double) | Needs double accumulation |

### 4c. Flat Dispatch (lines 3240-3253)

```java
    if (canUseFlatAccumulators) {
      return executeSingleKeyNumericFlat(
          shard, query, keyInfos, specs, columnTypeMap, groupByKeys,
          numAggs, isCountStar, accType,
          sortAggIndex, sortAscending, topN);
    }
```

### 4d. Non-Flat Fallback (lines 3255+)

When `canUseFlatAccumulators == false`, uses `SingleKeyHashMap` (open-addressing with `AccumulatorGroup[]`):

```java
    final SingleKeyHashMap singleKeyMap = new SingleKeyHashMap(specs);
```

Then has three iteration strategies:
1. **MatchAllDocsQuery + allDense + no varchar aggs** (line ~3305): Sequential `nextDoc()` lockstep iteration — fastest path, avoids `advanceExact()` binary search.
2. **MatchAllDocsQuery + sparse/varchar** (line ~3400): Per-doc `collectSingleKeyNumericDoc()` helper.
3. **Filtered query** (line ~3420): `Weight+Scorer` loop with `collectSingleKeyNumericDoc()`.

After collection, builds result with optional top-N heap selection (lines 3470-3570).

---

## 5. executeSingleKeyNumericFlat (line 4269) — Entry Point

```java
  private static List<Page> executeSingleKeyNumericFlat(
      IndexShard shard,
      Query query,
      List<KeyInfo> keyInfos,
      List<AggSpec> specs,
      Map<String, Type> columnTypeMap,
      List<String> groupByKeys,
      int numAggs,
      boolean[] isCountStar,
      int[] accType,
      int sortAggIndex,
      boolean sortAscending,
      long topN)
      throws Exception {
```

Uses `FlatSingleKeyMap` — a flat `long[]` array per hash map slot instead of `AccumulatorGroup` objects. Layout computed from `accType`:
- accType 0 (COUNT): 1 slot
- accType 1 (SUM long): 1 slot
- accType 2 (AVG long): 2 slots (sum + count)

---

## 6. Summary: Full Condition Chain for Flat Path

For `executeSingleKeyNumericFlat` to be reached, ALL of these must be true:

1. **No VARCHAR group-by keys** (`!hasVarchar` in `executeInternal`)
2. **Exactly 1 group-by key** (`keyInfos.size() == 1` in `executeNumericOnly`)
3. **No DATE_TRUNC/arith/EXTRACT on the key** (`!anyTrunc` in `executeNumericOnly`)
4. **All aggregates are flat-eligible** (`canUseFlatAccumulators` in `executeSingleKeyNumeric`):
   - COUNT(*) ✓
   - COUNT(col) ✓ (non-distinct only)
   - SUM(long) ✓
   - AVG(long) ✓
   - Everything else ✗ (MIN, MAX, COUNT DISTINCT, SUM/AVG double, VARCHAR args)
