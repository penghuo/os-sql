# FusedGroupByAggregate TopN Analysis

## 1. `executeWithTopN` Method Signatures (FusedGroupByAggregate.java:1249-1290)

Two overloads, both delegate to `executeInternal`:

```java
// Overload 1: sort by aggregate column (sortGroupKeyIndex defaults to -1)
public static List<Page> executeWithTopN(
    AggregationNode aggNode, IndexShard shard, Query query,
    Map<String,Type> columnTypeMap, int sortAggIndex, boolean sortAscending, long topN)

// Overload 2: sort by group key column
public static List<Page> executeWithTopN(
    AggregationNode aggNode, IndexShard shard, Query query,
    Map<String,Type> columnTypeMap, int sortAggIndex, boolean sortAscending, long topN, int sortGroupKeyIndex)
```

Both call `executeInternal(...)` which is also used by `execute()` (with topN=0, sortAggIndex=-1).

## 2. Flat Hash Map Heap-Based Top-N Selection (FusedGroupByAggregate.java:8196+)

At output time, when `useFlatLongMap && flatSize > 0`:

```java
int effectiveTopN = (topN > 0) ? (int) Math.min(topN, flatSize) : flatSize;
boolean doHeapSelect = (sortAggIndex >= 0 || sortGroupKeyIndex >= 0) && topN > 0 && effectiveTopN < flatSize;
```

When `doHeapSelect` is true, a **min/max heap of size N** is used:
- Iterates all flat map slots
- Maintains a heap of `effectiveTopN` best slots
- Sort value comes from either `flatKeys[s * numKeys + sortGroupKeyIndex]` (group key sort) or `flatValues[s].accumulators[sortAggIndex].getSortValue()` (aggregate sort)
- Only the top-N slots are resolved to output Pages — avoids ordinal resolution for non-top groups

**Key insight**: topN is used **as-is** (no multiplier). The heap selects exactly `topN` groups.

## 3. Dispatch Logic (TransportShardExecuteAction.java:440-600)

The dispatch for `[Project] -> Limit -> Sort -> Agg` pattern:

### Condition chain (line 440+):
1. `canFuse(innerAgg, colTypeMap)` must be true
2. `sortNode != null && limitNode != null`
3. Sort keys must all resolve to aggregation output columns
4. `topN = limitNode.getCount() + limitNode.getOffset()`

### Three sub-paths:

**A. HAVING present** (line 468): No topN pre-filter. Full aggregation → HAVING filter → SortOperator.

**B. Single sort key = aggregate column** (line 533):
```java
if (sortIndices.size() == 1 && sortIndices.get(0) >= numGroupByCols)
```
→ Direct `executeFusedGroupByAggregateWithTopN(innerAgg, req, sortAggIndex, sortAsc, topN)`
→ Pages returned already sorted, no SortOperator needed.

**C. Fallback: multi-sort or sort-by-group-key** (line 570):
- If primary sort key is aggregate: `executeFusedGroupByAggregateWithTopN(innerAgg, req, primarySortAggIndex, primarySortAsc, topN)`
- If primary sort key is group key: `executeFusedGroupByAggregateWithTopN(innerAgg, req, -1, primarySortAsc, topN, sortGroupKeyIndex)`
- Then applies `SortOperator` on the reduced set for correct multi-key ordering.

### No-Sort path (line 649):
`LimitNode -> AggregationNode` (no Sort): calls `executeWithTopN(limitedAgg, req, -1, false, topN)` — returns first N groups arbitrarily. Comment says "For queries like Q18".

## 4. SHARD_MULTIPLIER

**NOT used in the FusedGroupByAggregate path.** `topN` is passed directly as `limitNode.getCount() + limitNode.getOffset()`.

`SHARD_MULTIPLIER = 200` exists only in the COUNT DISTINCT dedup paths (lines 3076, 3174, 3299) for ordinal-based pruning in `computeTopKOrdinals`.

## 5. Q15/Q16/Q18 Routing Analysis

| Query | Pattern | Route |
|-------|---------|-------|
| Q15 | GROUP BY + ORDER BY agg + LIMIT | Path B or C depending on sort key count — **gets topN pruning** |
| Q16 | GROUP BY + ORDER BY agg + LIMIT | Same as Q15 — **gets topN pruning** |
| Q18 | GROUP BY + LIMIT (no ORDER BY) | No-Sort path (line 649) — **gets topN but arbitrary selection** |

**All three queries already route through `executeWithTopN`** if `canFuse()` returns true. The shard returns exactly `topN` groups (no multiplier), which is correct for single-shard but may under-report for multi-shard since each shard only sends its local top-N without safety margin.

## 6. Potential Issue: No Shard Multiplier

Unlike the COUNT DISTINCT path which uses `SHARD_MULTIPLIER = 200`, the fused GROUP BY topN path sends exactly `topN` rows per shard. For multi-shard deployments, this could produce incorrect results if the global top-N spans groups that aren't in any single shard's local top-N. However, for single-shard or when groups are well-distributed, this is correct.
