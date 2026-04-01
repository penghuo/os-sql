# Q39 Top-N Pruning Code Analysis

## 1. The Decision Point (TransportShardExecuteAction.java:529-580)

There are TWO top-N paths, both gated by `sortIndices.get(0) >= numGroupByCols`:

### Path A — Fast fused top-N (line 529)
```java
// Line 529: Single sort key that is an AGGREGATE column
if (sortIndices.size() == 1 && sortIndices.get(0) >= numGroupByCols) {
    int sortAggIndex = sortIndices.get(0) - numGroupByCols;
    // Calls executeWithTopN → heap selection inside FusedGroupByAggregate
    aggPages = executeFusedGroupByAggregateWithTopN(innerAgg, req, sortAggIndex, sortAsc, topN);
    // Pages already sorted, just project and return
}
```

### Path B — Fallback with pre-filter (line 573)
```java
// Line 573: Multi-key sort, but primary key is still an aggregate column
if (sortIndices.get(0) >= numGroupByCols) {
    aggPages = executeFusedGroupByAggregateWithTopN(innerAgg, req, primarySortAggIndex, primarySortAsc, topN);
} else {
    // Q39 HITS THIS BRANCH — no top-N at all!
    aggPages = executeFusedGroupByAggregate(innerAgg, req);
}
```

### Why Q39 fails the check

For Q39, the sort key is `PageViews DESC`. `PageViews` is a GROUP BY key (index 4 out of 5 group-by keys), NOT an aggregate column. So:
- `sortIndices.get(0)` = 4 (position of PageViews in output columns)
- `numGroupByCols` = 5
- `4 >= 5` → **FALSE**
- Result: falls through to `executeFusedGroupByAggregate(innerAgg, req)` — **no top-N pruning**, all groups materialized.

### How sortIndices is computed (line 446-456)
```java
aggOutputColumns = [groupByKey0, groupByKey1, ..., groupByKey4, agg0, agg1, ...]
//                  ^--- numGroupByCols = 5 ---^   ^--- aggregates ---^
sortIndices.add(aggOutputColumns.indexOf(sortKey));
// PageViews is groupByKey4 → index 4
```

## 2. FusedGroupByAggregate.executeWithTopN (line 1249-1258)

```java
public static List<Page> executeWithTopN(
    AggregationNode aggNode, IndexShard shard, Query query,
    Map<String, Type> columnTypeMap,
    int sortAggIndex,      // index within AGGREGATE columns (0-based), -1 to disable
    boolean sortAscending,
    long topN)             // number of top groups to keep, 0 = all
{
    return executeInternal(aggNode, shard, query, columnTypeMap, sortAggIndex, sortAscending, topN);
}
```

Key: `sortAggIndex` is an index into the **aggregate** columns array, not the full output. The current API cannot express "sort by group-by key #4" — it only supports sorting by aggregate columns.

## 3. How Top-N Pruning Works Inside FusedGroupByAggregate

The top-N pruning is a **post-aggregation heap selection** (NOT during-aggregation pruning). It happens in two places depending on the storage strategy:

### Flat long map path (line 8158-8211)
```java
if (sortAggIndex >= 0 && topN > 0 && effectiveTopN < flatSize) {
    int[] heap = new int[n];  // min-heap for DESC, max-heap for ASC
    for (int s = 0; s < flatCap; s++) {
        if (flatValues[s] == null) continue;
        long val = flatValues[s].accumulators[sortAggIndex].getSortValue();
        // Standard heap insert/replace logic
    }
    topSlots = Arrays.copyOf(heap, heapSize);
}
```

### HashMap path (line 8300-8340)
```java
if (sortAggIndex >= 0 && topN > 0 && topN < globalGroups.size()) {
    Map.Entry<Object, AccumulatorGroup>[] heap = new Map.Entry[n];
    for (Map.Entry<Object, AccumulatorGroup> entry : globalGroups.entrySet()) {
        long val = entry.getValue().accumulators[sortAggIndex].getSortValue();
        // Same heap logic
    }
}
```

Both use `accumulators[sortAggIndex].getSortValue()` — this only works for aggregate columns. There's no equivalent for group-by key values.

### Also in the ordinal-based path (line 1708-1750)
Same pattern: `ordCounts[i]` is used as the sort value, which is the COUNT accumulator for single-agg queries.

## 4. executeWithEvalKeys (line 7396-7640+)

This is the path Q39 uses (CASE WHEN expressions as group-by keys). Signature:
```java
private static List<Page> executeWithEvalKeys(
    AggregationNode aggNode, IndexShard shard, Query query,
    List<KeyInfo> keyInfos, List<AggSpec> specs,
    Map<String, Type> columnTypeMap, List<String> groupByKeys,
    int sortAggIndex, boolean sortAscending, long topN)
```

It already accepts `sortAggIndex` and `topN` parameters and has heap-based top-N selection at lines 8158 and 8300. **The infrastructure for top-N pruning already exists in this method** — the problem is solely that TransportShardExecuteAction never passes a valid sortAggIndex when the sort key is a GROUP BY column.

## 5. Key Insight for Implementation

The fundamental issue: `sortAggIndex` is defined as an index into the **aggregate** columns. To support sorting by a GROUP BY key:

**Option A**: Extend the API to support a "sort by group-by key" index. This would require:
- New parameter or encoding (e.g., negative sortAggIndex = group-by key index)
- New heap comparison logic using group-by key values instead of `accumulators[sortAggIndex].getSortValue()`

**Option B**: Change the condition in TransportShardExecuteAction (line 573) to also handle sort-by-group-key cases. The heap selection would compare group-by key values (longs for numeric keys, BytesRef for varchar) instead of accumulator values.

The heap infrastructure (min-heap/max-heap with sift-up/sift-down) is already implemented in 3 places. The missing piece is extracting the sort value from a group-by key column instead of an accumulator.
