# Q39 Top-N Actual Code Reference

## 1. TransportShardExecuteAction.java — Dispatch Block (lines 434-600)

### Key: How sortIndices and sortAggIndex are computed (lines 454-466)

```java
List<Integer> sortIndices = new ArrayList<>();
for (String sortKey : sortNode.getSortKeys()) {
    int idx = aggOutputColumns.indexOf(sortKey);
    if (idx < 0) {
        sortIndices = null;
        break;
    }
    sortIndices.add(idx);
}
// ...
long topN = limitNode.getCount() + limitNode.getOffset();
int numGroupByCols = innerAgg.getGroupByKeys().size();
```

### Single-sort-key fast path — ONLY for aggregate keys (lines 530-563)

```java
if (sortIndices.size() == 1 && sortIndices.get(0) >= numGroupByCols) {
    int sortAggIndex = sortIndices.get(0) - numGroupByCols;
    boolean sortAsc = sortNode.getAscending().get(0);
    List<Page> aggPages =
        executeFusedGroupByAggregateWithTopN(innerAgg, req, sortAggIndex, sortAsc, topN);
    // ... projection + return
}
```

### Fallback branch — multi-key sort or GROUP BY key sort (lines 565-600)

```java
// Fallback: aggregation (with optional shard-level top-N pre-filter) + SortOperator.
List<Page> aggPages;
if (sortIndices.get(0) >= numGroupByCols) {
    int primarySortAggIndex = sortIndices.get(0) - numGroupByCols;
    boolean primarySortAsc = sortNode.getAscending().get(0);
    aggPages =
        executeFusedGroupByAggregateWithTopN(
            innerAgg, req, primarySortAggIndex, primarySortAsc, topN);
} else {
    // *** THIS IS THE Q39 PATH — sort key is a GROUP BY key ***
    // Currently NO top-N pre-filtering; aggregates ALL groups then sorts
    aggPages = executeFusedGroupByAggregate(innerAgg, req);
}
List<Type> aggColumnTypes =
    FusedGroupByAggregate.resolveOutputTypes(innerAgg, colTypeMap);
// Then applies SortOperator on full result set
```

**Q39 Problem**: When `sortIndices.get(0) < numGroupByCols` (sort key is a GROUP BY key like PageViews), the code falls into the `else` branch and calls `executeFusedGroupByAggregate` WITHOUT top-N, aggregating ALL groups before sorting.

---

## 2. FusedGroupByAggregate.java — executeWithTopN (lines 1249-1270)

```java
public static List<Page> executeWithTopN(
    AggregationNode aggNode,
    IndexShard shard,
    Query query,
    Map<String, Type> columnTypeMap,
    int sortAggIndex,
    boolean sortAscending,
    long topN)
    throws Exception {
  return executeInternal(aggNode, shard, query, columnTypeMap, sortAggIndex, sortAscending, topN);
}
```

### executeInternal signature (line 1280):

```java
private static List<Page> executeInternal(
    AggregationNode aggNode,
    IndexShard shard,
    Query query,
    Map<String, Type> columnTypeMap,
    int sortAggIndex,       // index into AccumulatorGroup.accumulators[]
    boolean sortAscending,
    long topN)
```

### execute (no topN) passes -1/false/0 (line 1276):

```java
return executeInternal(aggNode, shard, query, columnTypeMap, -1, false, 0);
```

---

## 3. FusedGroupByAggregate.java — Flat Map Heap Selection (lines 8150-8220)

### The heap uses `sortAggIndex` to extract sort value from accumulators:

```java
// Select top-N from flat map using heap
int effectiveTopN = (topN > 0) ? (int) Math.min(topN, flatSize) : flatSize;
int[] topSlots;
if (sortAggIndex >= 0 && topN > 0 && effectiveTopN < flatSize) {
    int n = effectiveTopN;
    int[] heap = new int[n];
    int heapSize = 0;
    for (int s = 0; s < flatCap; s++) {
        if (flatValues[s] == null) continue;
        long val = flatValues[s].accumulators[sortAggIndex].getSortValue();
        // ... heap insert/sift logic using val ...
    }
    topSlots = java.util.Arrays.copyOf(heap, heapSize);
} else {
    // No sort or all groups: collect all occupied slots
    topSlots = new int[flatSize];
    int idx = 0;
    for (int s = 0; s < flatCap; s++) {
        if (flatValues[s] != null) topSlots[idx++] = s;
    }
}
```

**Key insight**: `sortAggIndex` is an index into `accumulators[]` (aggregate columns only). For GROUP BY key sorting, we'd need to sort on `flatKeys[slot * numKeys + sortKeyIndex]` instead.

### Flat map key storage (line 7745):

```java
long[] flatKeys = null; // flatKeys[slot * numKeys + k] = key k at slot
```

### HashMap path heap selection (lines 8300+):

```java
if (sortAggIndex >= 0 && topN > 0 && topN < globalGroups.size()) {
    // Same heap pattern but on Map.Entry<Object, AccumulatorGroup>
    // entry.getValue().accumulators[sortAggIndex].getSortValue()
}
```

---

## 4. TransportShardExecuteAction.java — executeFusedGroupByAggregateWithTopN (lines 3170-3210)

```java
private List<Page> executeFusedGroupByAggregateWithTopN(
    AggregationNode aggNode,
    ShardExecuteRequest req,
    int sortAggIndex,
    boolean sortAscending,
    long topN)
    throws Exception {
  TableScanNode scanNode = findTableScanNode(aggNode);
  String indexName = scanNode.getIndexName();
  CachedIndexMeta cachedMeta = getOrBuildIndexMeta(indexName);

  IndexMetadata indexMeta = clusterService.state().metadata().index(indexName);
  IndexShard shard = indicesService.indexService(indexMeta.getIndex()).getShard(req.getShardId());

  Query luceneQuery =
      compileOrCacheLuceneQuery(scanNode.getDslFilter(), cachedMeta.fieldTypeMap());

  List<Page> result =
      FusedGroupByAggregate.executeWithTopN(
          aggNode, shard, luceneQuery, cachedMeta.columnTypeMap(),
          sortAggIndex, sortAscending, topN);
  // GC hint for large results
  int totalRows = 0;
  for (Page p : result) { totalRows += p.getPositionCount(); }
  if (totalRows > 10000) { System.gc(); }
  return result;
}
```

---

## Architecture Summary for Q39 Fix

### Current flow for Q39 (sort by GROUP BY key PageViews):
1. `sortIndices.get(0)` = 4 (PageViews is index 4 in aggOutputColumns)
2. `numGroupByCols` = 5 (Q39 has 5 GROUP BY keys)
3. `4 < 5` → falls into `else` branch → `executeFusedGroupByAggregate()` (no topN)
4. ALL groups aggregated, then SortOperator sorts the full result

### What needs to change:
The top-N pruning currently only works with `sortAggIndex` (index into `accumulators[]`). To support GROUP BY key sorting:

1. **In TransportShardExecuteAction.java (fallback branch ~line 567)**: When `sortIndices.get(0) < numGroupByCols`, pass a "sort GROUP BY key index" to a new or modified top-N path.

2. **In FusedGroupByAggregate.java (heap selection)**: Instead of `flatValues[s].accumulators[sortAggIndex].getSortValue()`, use `flatKeys[s * numKeys + sortKeyIndex]` for GROUP BY key sorting.

3. **Two heap paths need updating**:
   - Flat map path (line ~8161): uses `flatKeys[]` for key values
   - HashMap path (line ~8300): uses `entry.getKey()` (the map key object) for key values

### Data access patterns:
- **Flat map**: `flatKeys[slot * numKeys + keyIndex]` gives the long value of GROUP BY key `keyIndex` at `slot`
- **HashMap**: Key is `Object` (could be `Long`, `long[]`, or composite) — need to extract the specific key component
