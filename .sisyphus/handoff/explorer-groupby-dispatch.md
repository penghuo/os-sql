# GROUP BY Dispatch Path Analysis for Q15 Pattern

## Q15: `SELECT UserID, COUNT(*) FROM hits GROUP BY UserID ORDER BY COUNT(*) DESC LIMIT 10`
- UserID = BIGINT (single numeric key)
- Aggregate = COUNT(*)
- Has ORDER BY (DESC on aggregate) + LIMIT 10

## Dispatch Flow in `TransportShardExecuteAction.executePlan()`

### Plan Tree Pattern
```
LimitNode(10) -> [ProjectNode] -> SortNode(COUNT(*) DESC) -> AggregationNode(GROUP BY UserID, COUNT(*))
```

### Dispatch Priority (lines ~194-640)
1. **Scalar COUNT(*)** — skipped (has GROUP BY)
2. **Sorted scan** — skipped (has aggregation)
3. **Fused scalar agg** — skipped (has GROUP BY keys)
4. **COUNT(DISTINCT) fast paths** — skipped (not DISTINCT)
5. **Expression GROUP BY** — skipped (plain column key)
6. **Plain GROUP BY (no sort/limit)** — skipped (plan has LimitNode wrapping)
7. **✅ Fused GROUP BY with sort+limit** (line ~390-598) — **THIS IS THE MATCH**
   - `extractAggFromSortedLimit(plan)` extracts AggregationNode from `LimitNode -> [ProjectNode] -> SortNode -> AggregationNode`
   - `FusedGroupByAggregate.canFuse()` validates key types

### Inside the sort+limit dispatch (line ~418-495)
```
topN = limitNode.getCount() + limitNode.getOffset()  // = 10
sortIndices resolved against aggOutputColumns: [UserID, COUNT(*)]
  → sortIndices = [1] (COUNT(*) is at index 1)
numGroupByCols = 1
```

**Key condition (line ~487-493):**
```java
if (sortIndices.size() == 1 && sortIndices.get(0) >= numGroupByCols) {
    // Single sort key on an aggregate column → fused top-N path
    int sortAggIndex = sortIndices.get(0) - numGroupByCols;  // = 0
    boolean sortAsc = sortNode.getAscending().get(0);         // = false (DESC)
    aggPages = executeFusedGroupByAggregateWithTopN(innerAgg, req, sortAggIndex, sortAsc, topN);
}
```

This calls `FusedGroupByAggregate.executeWithTopN()` (line 907).

## Inside `FusedGroupByAggregate.executeInternal()` (line ~939)

### Key dispatch at line ~1047:
```java
if (hasVarchar) {
    // VARCHAR key paths...
} else {
    return executeNumericOnly(shard, query, keyInfos, specs, ...sortAggIndex, sortAscending, topN);
}
```

For Q15 (UserID = BIGINT), `hasVarchar = false` → goes to `executeNumericOnly()`.

### `executeNumericOnly()` (line 2812)
Single key, no expressions → dispatches to:
```java
return executeSingleKeyNumeric(shard, query, keyInfos, specs, ...sortAggIndex, sortAscending, topN);
```

### `executeSingleKeyNumeric()` (line 3172)
Checks if all aggs are flat-representable (COUNT/SUM long/AVG long). COUNT(*) → `accType[0] = 0` → `canUseFlatAccumulators = true`.

Dispatches to:
```java
return executeSingleKeyNumericFlat(shard, query, ...sortAggIndex, sortAscending, topN, bucket, numBuckets);
```

### `executeSingleKeyNumericFlat()` (line 4438) — **FINAL EXECUTION**
Uses `FlatSingleKeyMap` (open-addressing hash map with `long[] keys` + `long[] accData`).

**Top-N selection (line ~4570-4630):**
```java
if (sortAggIndex >= 0 && topN > 0 && topN < flatMap.size) {
    // Binary min-heap of size N over flatMap slots
    int[] heap = new int[n];
    // For DESC (sortAscending=false): keeps largest N values
    // Heap root = smallest of top-N; replace root when finding larger value
    for (int slot = 0; slot < flatMap.capacity; slot++) {
        if (!flatMap.occupied[slot]) continue;
        long val = flatMap.accData[slot * slotsPerGroup + sortAccOff];
        if (heapSize < n) { /* insert + sift up */ }
        else {
            boolean better = sortAscending ? (val < heapVals[0]) : (val > heapVals[0]);
            if (better) { /* replace root + sift down */ }
        }
    }
    outputSlots = heap;  // Only build Page for top-N slots
}
```

**This is already a fully optimized top-N path for single numeric keys.** It:
1. Aggregates ALL groups into `FlatSingleKeyMap` (must scan all docs)
2. Selects top-N via binary heap over the flat map (O(G log N) where G = groups)
3. Builds Page only for the N selected groups (avoids materializing all groups)

## Existing VARCHAR Top-N Optimization (for comparison)

### `executeSingleVarcharCountStar()` (line 1274)
Single-segment fast path uses `long[] ordCounts` (ordinal-indexed array).

**Top-N selection (line ~1370):**
```java
if (sortAggIndex >= 0 && topN > 0 && topN < groupCount) {
    int[] heap = new int[n];       // ordinal indices
    long[] heapVals = new long[n]; // count values
    // Same min-heap/max-heap pattern as numeric path
    // Only resolves ordinals to strings for the N selected groups
}
```

**Key difference:** VARCHAR path uses ordinal-indexed `long[]` array (no hash map), numeric path uses `FlatSingleKeyMap` (open-addressing hash map with `long[] keys` + `long[] accData`).

## Summary: Q15 Dispatch Chain
```
executePlan() 
  → extractAggFromSortedLimit() matches LimitNode→SortNode→AggNode
  → sortIndices=[1], single agg sort → fused top-N path
  → executeFusedGroupByAggregateWithTopN(sortAggIndex=0, sortAsc=false, topN=10)
    → FusedGroupByAggregate.executeWithTopN()
      → executeInternal(sortAggIndex=0, sortAsc=false, topN=10)
        → hasVarchar=false → executeNumericOnly()
          → single key, no expr → executeSingleKeyNumeric()
            → canUseFlatAccumulators=true → executeSingleKeyNumericFlat()
              → FlatSingleKeyMap aggregation → heap-based top-N selection → Page for 10 rows
```

## Key Findings
1. **Q15 already has a fully optimized path** — single numeric key + COUNT(*) + ORDER BY DESC + LIMIT hits the `executeSingleKeyNumericFlat` with heap-based top-N.
2. **The top-N optimization avoids materializing all groups into Pages** — only the N selected slots get Page construction.
3. **Both VARCHAR and numeric paths have equivalent heap-based top-N** — the pattern is identical, just different underlying data structures (ordinal array vs flat hash map).
4. **No further top-N optimization is needed for this query pattern** — the bottleneck is the full scan + hash map aggregation (must see all docs), not the output materialization.
