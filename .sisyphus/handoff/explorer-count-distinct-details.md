# COUNT(DISTINCT) in DQE — Calcite Plan Structure & Execution Details

## 1. Plan Decomposition (PlanFragmenter)

**File:** `dqe/src/main/java/org/opensearch/sql/dqe/coordinator/fragment/PlanFragmenter.java`

### Scalar COUNT(DISTINCT col) — No GROUP BY
- PlanFragmenter strips the AggregationNode entirely (line ~143: `return aggNode.getChild()`)
- Shard receives a bare `TableScanNode` with single column
- Detected by `isBareSingleNumericColumnScan()` (line 1781) or `isBareSingleVarcharColumnScan()` (line 1803)

### GROUP BY + COUNT(DISTINCT col) — Two-Level Dedup
- `extractCountDistinctColumns()` (line 256) extracts distinct columns
- Shard plan becomes: `AggregationNode(PARTIAL, groupBy=[originalKeys + distinctCols], aggs=[COUNT(*)])`
- Example: `GROUP BY RegionID, COUNT(DISTINCT UserID)` → shard does `GROUP BY (RegionID, UserID) COUNT(*)`
- Lines 151-163

### Mixed Aggregates (COUNT(DISTINCT) + SUM/COUNT/AVG)
- `buildMixedDedupShardPlan()` (line 175) handles queries like Q10
- AVG decomposed to SUM+COUNT; COUNT(DISTINCT) omitted from shard aggs
- Shard plan: `GROUP BY (originalKeys + distinctCols)` with partial aggs for decomposable functions

## 2. AggSpec Class & isDistinct Flag

**File:** `FusedGroupByAggregate.java:11187-11201`

```java
private static final class AggSpec {
    final String funcName;    // "COUNT", "SUM", etc.
    final boolean isDistinct; // true for COUNT(DISTINCT x)
    final String arg;         // column name or "*"
    final Type argType;       // Trino type
}
```

**Parsing** (line 420-426): Uses `AGG_FUNCTION` regex (line 81):
```java
Pattern.compile("^\\s*(COUNT|SUM|MIN|MAX|AVG)\\((DISTINCT\\s+)?(.+?)\\)\\s*$", CASE_INSENSITIVE);
// group(2) != null → isDistinct = true
```

## 3. accType=5 Assignment & Flat Accumulator Disabling

**File:** `FusedGroupByAggregate.java:3190-3210` (executeSingleKeyNumeric)

```java
case "COUNT":
    if (spec.isDistinct) {
        accType[i] = 5;
        canUseFlatAccumulators = false;  // ← disables flat long[] path
    } else {
        accType[i] = 0;
    }
```

When `canUseFlatAccumulators = false`, execution falls through to the object-based accumulator path using `AccumulatorGroup` with per-group `CountDistinctAccum` instances.

**accType=5 runtime** (line 3370):
```java
case 5: // COUNT(DISTINCT)
    CountDistinctAccum cda = (CountDistinctAccum) acc;
    if (cda.usePrimitiveLong) cda.longDistinctValues.add(rawVal);
    else cda.objectDistinctValues.add(Double.longBitsToDouble(rawVal));
```

## 4. CountDistinctAccum (Fused Path)

**File:** `FusedGroupByAggregate.java:12604-12645`

```java
private static class CountDistinctAccum implements MergeableAccumulator {
    final LongOpenHashSet longDistinctValues;   // numeric non-double
    final Set<Object> objectDistinctValues;     // varchar/double fallback

    CountDistinctAccum(Type argType) {
        usePrimitiveLong = !(argType instanceof VarcharType) && !(argType instanceof DoubleType);
        longDistinctValues = usePrimitiveLong ? new LongOpenHashSet(16) : null;
        objectDistinctValues = usePrimitiveLong ? null : new HashSet<>();
    }
    void merge(MergeableAccumulator other) { /* union sets */ }
    void writeTo(BlockBuilder builder) { BIGINT.writeLong(builder, set.size()); }
}
```

**Factory** (line 12115-12119):
```java
case "COUNT":
    if (spec.isDistinct) return new CountDistinctAccum(spec.argType);
```

### CountDistinctAccumulator (Generic Operator Path)

**File:** `HashAggregationOperator.java:718-775`
- Same dual-set design (LongOpenHashSet vs HashSet<Object>)
- Used when query falls through to generic `HashAggregationOperator` pipeline
- Factory: `HashAggregationOperator.countDistinct(colIdx, inputType)` (line 1089)

## 5. Coordinator Merge Logic

**File:** `TransportTrinoSqlAction.java`

### Scalar COUNT(DISTINCT) — Numeric (line 342-344)
```java
if (isScalarCountDistinctLong(singleCdAgg, columnTypeMap)) {
    mergedPages = mergeCountDistinctValues(shardPages);  // line 1925
}
```
`mergeCountDistinctValues` (line 1925): Unions all shard values into global `LongOpenHashSet`, returns `set.size()`.

### Scalar COUNT(DISTINCT) — VARCHAR (line 347-349)
```java
if (isScalarCountDistinctVarchar(singleCdVarcharAgg, columnTypeMap)) {
    mergedPages = mergeCountDistinctVarcharValues(shardPages);
}
```

### GROUP BY + COUNT(DISTINCT) — Dedup Merge (line 352-365)
```java
if (isShardDedupCountDistinct(shardPlan, singleAgg, columnTypeMap)) {
    mergedPages = mergeDedupCountDistinct(shardPages, singleAgg, shardPlan, ...);
}
```
`mergeDedupCountDistinct` (line 2089):
- Stage 1: FINAL dedup merge — removes cross-shard duplicates using hash map on (originalKey + distinctCol) composite keys
- Stage 2: Re-aggregate — counts distinct values per original group key
- Fast path for 2-key all-numeric: `mergeDedupCountDistinct2Key()` uses flat long[] arrays

### Mixed Dedup Merge (line 370-382)
```java
if (isShardMixedDedup(shardPlan, singleMixed)) {
    mergedPages = mergeMixedDedup(shardPages, singleMixed, shardPlan, ...);
}
```

## 6. Dispatch Flow Summary (TransportShardExecuteAction lines 240-400)

```
Shard receives plan →
  1. FusedScanAggregate (scalar agg, no GROUP BY) → line 240
  2. Bare TableScanNode + single numeric col → executeDistinctValuesScanWithRawSet (line 251)
  3. Bare TableScanNode + single varchar col → executeDistinctValuesScanVarcharWithRawSet (line 259)
  4. FusedScanAggregate with eval → executeFusedEvalAggregate (line 267)
  5. AggDedupNode (2 numeric keys, COUNT(*)) → executeCountDistinctWithHashSets (line 280)
  6. AggDedupNode (varchar key0 + numeric key1) → executeVarcharCountDistinctWithHashSets (line 310)
  7. AggDedupNode (mixed SUM/COUNT) → executeMixedDedupWithHashSets (line 325)
  8. Expression GROUP BY → executeFusedExprGroupByAggregate (line 340)
  9. FusedGroupByAggregate.canFuse() → executeFusedGroupByAggregate (line 350)
     ↳ accType=5 for COUNT(DISTINCT) → CountDistinctAccum per group
  10. Sort+Limit wrapping → fused sort path (line 360)
  11. Generic operator pipeline → HashAggregationOperator with CountDistinctAccumulator
```
