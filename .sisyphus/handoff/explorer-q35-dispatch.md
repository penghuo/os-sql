# Q35 Dispatch Path Analysis

## Query
```sql
SELECT ClientIP, ClientIP - 1, ClientIP - 2, ClientIP - 3, COUNT(*) AS c
FROM hits GROUP BY ClientIP, ClientIP - 1, ClientIP - 2, ClientIP - 3
ORDER BY c DESC LIMIT 10
```

## Plan Structure After Calcite Optimization

The shard plan is: `LimitNode -> ProjectNode -> SortNode -> AggregationNode -> TableScanNode`

The AggregationNode has:
- **groupByKeys**: `[ClientIP, (ClientIP - 1), (ClientIP - 2), (ClientIP - 3)]`
- **aggregateFunctions**: `[COUNT(*)]`
- All 4 keys are numeric (ClientIP is long), 3 are arithmetic expressions matching `ARITH_EXPR_PATTERN`

## Dispatch Path in TransportShardExecuteAction.executePlan()

### Step 1: Unwrap ProjectNode (line ~170)
The top-level `ProjectNode -> AggregationNode` is unwrapped. `effectivePlan = AggregationNode`.

### Step 2: Skip scalar/sorted/fused-scan paths
- Not scalar COUNT(*) (has GROUP BY keys)
- Not sorted scan (has AggregationNode)
- Not FusedScanAggregate (has GROUP BY keys)
- Not bare single-column scan
- Not fused eval-aggregate
- Not 2-key COUNT(DISTINCT) dedup

### Step 3: canFuseWithExpressionKey → **FALSE** (line ~230)
`canFuseWithExpressionKey` requires **exactly 1 group-by key** (`aggNode.getGroupByKeys().size() != 1`). Q35 has 4 keys → returns false immediately.

### Step 4: canFuse → **TRUE** (line ~240)
`FusedGroupByAggregate.canFuse()` succeeds because:
- groupByKeys is non-empty (4 keys)
- Child is TableScanNode (no EvalNode since arithmetic is inline)
- Key `ClientIP` → numeric type in columnTypeMap ✓
- Keys `(ClientIP - 1)`, `(ClientIP - 2)`, `(ClientIP - 3)` → match `ARITH_EXPR_PATTERN` → source col `ClientIP` is numeric ✓
- Aggregate `COUNT(*)` is supported ✓

### Step 5: extractAggFromSortedLimit → matches (line ~250)
Plan matches `LimitNode -> ProjectNode -> SortNode -> AggregationNode` pattern.

### Step 6: Sort key resolution (line ~270)
Sort key is `c` (COUNT(*)), which is at index 4 in aggOutputColumns (after 4 group-by keys). `sortIndices = [4]`, which is `>= numGroupByCols (4)` → enters the **fused top-N single-sort-key path**.

### Step 7: executeFusedGroupByAggregateWithTopN (line ~290)
Calls `FusedGroupByAggregate.executeWithTopN()` → `executeInternal()`.

## Dispatch Inside FusedGroupByAggregate.executeInternal()

### Key Classification
```
keyInfos[0] = KeyInfo("ClientIP", BigintType, false, null, null)        // plain column
keyInfos[1] = KeyInfo("ClientIP", BigintType, false, "arith", "-:1")    // arithmetic
keyInfos[2] = KeyInfo("ClientIP", BigintType, false, "arith", "-:2")    // arithmetic
keyInfos[3] = KeyInfo("ClientIP", BigintType, false, "arith", "-:3")    // arithmetic
```

- `hasVarchar = false` → enters `executeNumericOnly()`

### executeNumericOnly() Dispatch

1. **Single key?** No (4 keys) → skip `executeSingleKeyNumeric`
2. **Two keys?** No (4 keys) → skip `executeTwoKeyNumeric`
3. **All keys derive from same column?** → **YES!**
   - `sharedCol = "ClientIP"` for all 4 keys
   - `allSameCol = true`, `allArithOrPlain = true`
   - → Enters **`executeDerivedSingleKeyNumeric()`** (line ~2050)

## executeDerivedSingleKeyNumeric — The Actual Execution Path

This is the **critical optimization**: reduces 4-key GROUP BY to **single-key GROUP BY on ClientIP**.

### How it works:
1. Only reads `ClientIP` DocValues (1 column instead of 4)
2. Groups by `ClientIP` value only (single-key hash map)
3. At **output time**, derives the other 3 keys: `ClientIP-1`, `ClientIP-2`, `ClientIP-3`
4. Uses `FlatSingleKeyMap` (flat `long[]` accumulators, zero per-group object allocation)

### Aggregate check:
- `COUNT(*)` → `accType[0] = 0`, `canUseFlat = true`
- → Uses the **flat path** with `FlatSingleKeyMap`

### Parallelism:
- If `THREADS_PER_SHARD > 1` and multiple segments: parallel segment scanning with per-worker `FlatSingleKeyMap`, merged after completion
- Otherwise: sequential scan

### Top-N:
- `sortAggIndex >= 0` and `topN = 10` → uses min-heap to select top-10 groups by COUNT(*) DESC
- Only resolves derived keys for the 10 output groups

## canFuseWithExpressionKey — Multiple Expression Keys

**Does NOT handle multiple expression keys.** The method has a hard check:
```java
if (aggNode.getGroupByKeys().size() != 1) {
    return false;
}
```
This is by design — `canFuseWithExpressionKey` is for ordinal-cached evaluation of a single VARCHAR expression (e.g., REGEXP_REPLACE). Q35's keys are numeric arithmetic, not VARCHAR expressions.

## Bottleneck Analysis

Q35 at 5.0x ratio (DQE 1.862s vs CH 0.370s):

1. **Hash map overhead**: Even with `FlatSingleKeyMap`, ClientIP has very high cardinality (~2.5M unique IPs in 100M rows). The open-addressing hash map with Murmur3 hashing still does ~100M hash+probe operations.

2. **DocValues read**: Reading `ClientIP` via `advanceExact()` per doc is sequential but involves per-doc binary search in compressed DocValues blocks.

3. **Top-N heap**: With ~2.5M groups, the heap selection (10 out of 2.5M) is efficient but the hash map construction dominates.

4. **Comparison to ClickHouse**: CH likely uses vectorized hash aggregation with SIMD and/or radix partitioning, which is fundamentally faster for high-cardinality numeric GROUP BY.

## Summary

| Aspect | Value |
|--------|-------|
| **Dispatch path** | `executePlan → extractAggFromSortedLimit → executeFusedGroupByAggregateWithTopN → executeInternal → executeNumericOnly → executeDerivedSingleKeyNumeric` |
| **Key optimization** | 4-key → single-key reduction (all derived from ClientIP) |
| **Hash map** | `FlatSingleKeyMap` (flat long[] accumulators, zero object allocation per group) |
| **canFuseWithExpressionKey** | Returns false for multiple keys (requires exactly 1) |
| **Bottleneck** | High-cardinality hash map construction (~2.5M groups), per-doc DocValues advanceExact |
