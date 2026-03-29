# COUNT(DISTINCT) Execution Paths in DQE

## Architecture Overview

COUNT(DISTINCT) follows a **two-path** architecture depending on whether GROUP BY is present:

1. **Scalar COUNT(DISTINCT)** (no GROUP BY): PlanOptimizer marks as `SINGLE` → PlanFragmenter strips the AggNode → shards scan raw distinct values into `LongOpenHashSet` → coordinator unions sets and counts.
2. **Grouped COUNT(DISTINCT)** (with GROUP BY): PlanFragmenter decomposes into dedup plan `GROUP BY (original_keys + distinct_cols) WITH COUNT(*)` at `PARTIAL` step → shards run fused HashSet-per-group scan → coordinator does two-stage merge (dedup + re-aggregate).

## 1. PlanOptimizer: Non-Decomposable Detection

**File:** `dqe/src/main/java/org/opensearch/sql/dqe/planner/optimizer/PlanOptimizer.java:371-410`

```java
// AggregationSplitVisitor.visitAggregation():
if (hasNonDecomposableAgg(node.getAggregateFunctions(), node.getGroupByKeys())) {
    return new AggregationNode(optimizedChild, ..., AggregationNode.Step.SINGLE);
}

// hasNonDecomposableAgg():
if (upper.contains("COUNT(DISTINCT")) hasCountDistinct = true;
if (hasCountDistinct) return true;  // Forces SINGLE step
```

COUNT(DISTINCT) is classified as **non-decomposable** → cannot use PARTIAL/FINAL split → forced to `SINGLE` step.

## 2. PlanFragmenter: Dedup Decomposition

**File:** `dqe/src/main/java/org/opensearch/sql/dqe/coordinator/fragment/PlanFragmenter.java:143-170`

For grouped queries, PlanFragmenter transforms:
```
GROUP BY RegionID, COUNT(DISTINCT UserID)
→ Shard plan: GROUP BY (RegionID, UserID) WITH COUNT(*) [PARTIAL]
```

```java
List<String> distinctColumns = extractCountDistinctColumns(aggNode.getAggregateFunctions());
if (distinctColumns != null) {
    List<String> dedupKeys = new ArrayList<>(aggNode.getGroupByKeys());
    dedupKeys.addAll(distinctColumns);
    return new AggregationNode(aggNode.getChild(), dedupKeys, List.of("COUNT(*)"), Step.PARTIAL);
}
```

For **mixed aggregates** (Q10 pattern: `SUM + COUNT(DISTINCT)`), `buildMixedDedupShardPlan()` at line 175 creates:
```
GROUP BY (RegionID, UserID) WITH SUM(AdvEngineID), COUNT(*), SUM(ResolutionWidth), COUNT(ResolutionWidth)
```

## 3. TransportShardExecuteAction: Dispatch Logic

**File:** `dqe/src/main/java/org/opensearch/sql/dqe/shard/transport/TransportShardExecuteAction.java:246-320`

Three fast paths detected in order:

### Path A: Bare scan for scalar COUNT(DISTINCT) (line 246-260)
```java
if (scanFactory == null && isBareSingleNumericColumnScan(plan)) {
    return executeDistinctValuesScanWithRawSet(plan, req);  // LongOpenHashSet, no Page
}
if (scanFactory == null && isBareSingleVarcharColumnScan(plan)) {
    return executeDistinctValuesScanVarcharWithRawSet(plan, req);
}
```

### Path B: Grouped COUNT(DISTINCT) with 2 numeric keys (line 273-304)
```java
if (effectivePlan instanceof AggregationNode aggDedupNode
    && aggDedupNode.getStep() == AggregationNode.Step.PARTIAL
    && aggDedupNode.getGroupByKeys().size() == 2
    && FusedGroupByAggregate.canFuse(aggDedupNode, columnTypeMap)) {
    boolean isSingleCountStar = aggDedupNode.getAggregateFunctions().size() == 1
        && "COUNT(*)".equals(aggDedupNode.getAggregateFunctions().get(0));
    if (isSingleCountStar && bothNumeric(t0, t1)) {
        return executeCountDistinctWithHashSets(aggDedupNode, req, keyName0, keyName1, t0, t1);
    }
    // VARCHAR key0 + numeric key1: Q14 pattern
    if (t0 instanceof VarcharType && numericKey1) {
        return executeVarcharCountDistinctWithHashSets(...);
    }
}
```

### Path C: Mixed dedup (line 315+)
For queries with both COUNT(DISTINCT) and SUM/COUNT, routes to mixed dedup execution.

## 4. executeCountDistinctWithHashSets (Shard Execution)

**File:** `TransportShardExecuteAction.java:788-920`

```java
private ShardExecuteResponse executeCountDistinctWithHashSets(
    AggregationNode aggNode, ShardExecuteRequest req,
    String keyName0, String keyName1, Type type0, Type type1)
```

- Parallel segment scanning: each segment builds `Map<Long, LongOpenHashSet>` (key0 → set of key1 values)
- Segments merged by unioning HashSets (smaller into larger)
- Output: compact page with `(key0, 0, count_distinct)` + attached `distinctSets` map
- `resp.setDistinctSets(finalSets)` — raw sets travel to coordinator for cross-shard union

## 5. FusedGroupByAggregate: COUNT(DISTINCT) Accumulator

**File:** `dqe/src/main/java/org/opensearch/sql/dqe/shard/source/FusedGroupByAggregate.java`

### Accumulator class (line 12600-12650)
```java
private static class CountDistinctAccum implements MergeableAccumulator {
    final LongOpenHashSet longDistinctValues;   // numeric non-double
    final Set<Object> objectDistinctValues;     // varchar/double fallback
    // merge: longDistinctValues.addAll(other) or objectDistinctValues.addAll(other)
    // writeTo: BigintType.BIGINT.writeLong(builder, set.size())
}
```

### Case 5 dispatch in accumulator update (line 3370, 3680)
```java
case 5: // COUNT(DISTINCT)
    CountDistinctAccum cda = (CountDistinctAccum) acc;
    if (cda.usePrimitiveLong) cda.longDistinctValues.add(rawVal);
    else cda.objectDistinctValues.add(Double.longBitsToDouble(rawVal));
```

### executeSingleKeyNumericFlat (line 4350)
```java
private static List<Page> executeSingleKeyNumericFlat(
    IndexShard shard, Query query, List<KeyInfo> keyInfos, List<AggSpec> specs, ...)
```
- Flat array path for COUNT/SUM/AVG (no DISTINCT) — uses `FlatSingleKeyMap` with primitive arrays
- Does NOT handle COUNT(DISTINCT) — only accType 0 (COUNT), 1 (SUM long), 2 (AVG long)

## 6. FusedScanAggregate: Scalar COUNT(DISTINCT)

**File:** `dqe/src/main/java/org/opensearch/sql/dqe/shard/source/FusedScanAggregate.java:1405-1510`

```java
public static List<Page> executeDistinctValues(String columnName, IndexShard shard, Query query)
```
- Iterates DocValues, adds to `LongOpenHashSet`
- Returns distinct values as a single-column Page (not the count)
- Coordinator unions across shards then counts

```java
public static LongOpenHashSet collectDistinctValuesRaw(String columnName, IndexShard shard, Query query)
```
- Returns raw `LongOpenHashSet` directly (no Page construction) — used for local shard path

## 7. Coordinator Merge (TransportTrinoSqlAction)

**File:** `dqe/src/main/java/org/opensearch/sql/dqe/coordinator/transport/TransportTrinoSqlAction.java:331-380`

Decision tree at coordinator:

| Condition | Method | Description |
|-----------|--------|-------------|
| `isScalarPartialMerge(aggNode)` | `mergeScalarAggregation` | SUM/COUNT partial merge |
| `isScalarCountDistinctLong(agg, typeMap)` | `mergeCountDistinctValues` | Union long sets, count |
| `isScalarCountDistinctVarchar(agg, typeMap)` | `mergeCountDistinctVarcharValues` | Union string sets, count |
| `isShardDedupCountDistinct(shardPlan, agg, typeMap)` | `mergeDedupCountDistinct` | Two-stage: dedup merge + re-aggregate |
| `isShardMixedDedup(shardPlan, agg)` | `mergeMixedDedup` | Mixed agg dedup merge |

### isShardDedupCountDistinct (line 2051-2078)
```java
// True when: shardPlan is PARTIAL AggNode, coordinator is SINGLE with GROUP BY,
// shard has more group-by keys than coordinator (original + distinct cols),
// shard aggregates exactly COUNT(*)
```

### mergeDedupCountDistinct (line 2090+)
Two-stage merge:
1. **Stage 1**: FINAL merge on dedup keys → removes cross-shard duplicates
2. **Stage 2**: GROUP BY original keys with COUNT(*) → produces COUNT(DISTINCT)

When raw HashSets are available (local path), uses `mergeDedupCountDistinctViaSets` for O(n) direct union.

## Key Data Structures

- **`LongOpenHashSet`**: Custom open-addressing hash set for primitive longs (avoids boxing)
- **`SliceRangeHashSet`**: For VARCHAR COUNT(DISTINCT) coordinator merge
- **`SliceLongDedupMap`**: Compound (Slice, long) keys for VARCHAR key + numeric distinct
- **`CountDistinctAccum`**: Per-group accumulator wrapping LongOpenHashSet or HashSet<Object>
