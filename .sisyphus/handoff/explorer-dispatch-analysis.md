# Dispatch Logic & TopN Extraction Analysis

## 1. Dispatch Routing (lines ~274-400)

The dispatch in `shardExecute()` checks `effectivePlan instanceof AggregationNode` with `canFuse()`, then branches:

**Single COUNT(*) dedup (`isSingleCountStar`):**
- **2-key numeric**: → `executeCountDistinctWithHashSets(aggDedupNode, req, keyName0, keyName1, t0, t1)` — NO topN
- **2-key VARCHAR+numeric (Q14)**: → `executeVarcharCountDistinctWithHashSets(aggDedupNode, req, keyName0, keyName1, t1, shardTopN)` — **HAS topN** via `extractTopNFromPlan(plan)`
- **N-key all-numeric (3+)**: → `executeNKeyCountDistinctWithHashSets(aggDedupNode, req, keys, keyTypes)` — **NO topN parameter**
- **N-key mixed-type**: → `executeMixedTypeCountDistinctWithHashSets(aggDedupNode, req, keys, mixedKeyTypes)` — NO topN

**Mixed dedup (`isMixedDedup`, 2-key):**
- → `executeMixedDedupWithHashSets(...)` — NO topN

## 2. `executeNKeyCountDistinctWithHashSets` (line ~1396)

```java
private ShardExecuteResponse executeNKeyCountDistinctWithHashSets(
    AggregationNode aggNode, ShardExecuteRequest req,
    List<String> keyNames, Type[] keyTypes) throws Exception
```
- Groups by first N-1 keys, dedup key is last key
- Uses `HashMap<LongArrayKey, LongOpenHashSet>` per segment, merged across segments
- Outputs full dedup tuples (all N keys + COUNT(*)=1) for coordinator merge
- **No topN parameter, no pruning**

## 3. `executeMixedTypeCountDistinctWithHashSets` (line ~1850)

```java
private ShardExecuteResponse executeMixedTypeCountDistinctWithHashSets(
    AggregationNode aggNode, ShardExecuteRequest req,
    List<String> keyNames, Type[] keyTypes) throws Exception
```
- Uses `HashMap<ObjectArrayKey, LongOpenHashSet>` for mixed VARCHAR/numeric group keys
- Same pattern as N-key but with Object-based composite keys
- **No topN parameter, no pruning**

## 4. `executeVarcharCountDistinctWithHashSets` — REFERENCE (line ~2708)

```java
private ShardExecuteResponse executeVarcharCountDistinctWithHashSets(
    AggregationNode aggNode, ShardExecuteRequest req,
    String varcharKeyName, String numericKeyName,
    Type numericKeyType, long topN) throws Exception
```
- Uses `Map<String, LongOpenHashSet>` keyed by varchar group value
- **Accepts `topN` parameter** — used for shard-level pruning
- This is the reference pattern for adding topN to other paths

## 5. `extractTopNFromPlan` (line ~3981) — FULL

```java
private static long extractTopNFromPlan(DqePlanNode plan) {
    DqePlanNode node = plan;
    while (node != null) {
        if (node instanceof LimitNode limitNode) {
            return limitNode.getCount() + limitNode.getOffset();
        }
        List<DqePlanNode> children = node.getChildren();
        node = (children != null && !children.isEmpty()) ? children.get(0) : null;
    }
    return Long.getLong("dqe.varcharDistinctTopN", 10);
}
```
Walks plan tree depth-first (first child only) looking for LimitNode. Returns `count + offset`. Falls back to system property `dqe.varcharDistinctTopN` (default 10).

## 6. `computeTopKOrdinals` (line ~3378)

```java
private static java.util.BitSet computeTopKOrdinals(
    LongOpenHashSet[] ordSets, long[] firstValues, boolean[] hasMinValue,
    int ordCount, long sentinel, int K)
```
- Returns BitSet with top-K ordinals by distinct count using a min-heap
- If `K >= ordCount`, returns all ordinals (no pruning)
- Used in the varchar path for ordinal-based pruning

## 7. TopN Passing: VARCHAR vs N-Key

| Path | topN extracted? | topN passed? | Pruning? |
|------|----------------|-------------|----------|
| VARCHAR (2-key) | `extractTopNFromPlan(plan)` | Yes, as `long topN` param | Yes, via `computeTopKOrdinals` |
| N-key numeric (3+) | **No** | **No** | **None** |
| Mixed-type (3+) | **No** | **No** | **None** |

**Pattern to replicate:** In the dispatch block (line ~355), before calling `executeNKeyCountDistinctWithHashSets`, add:
```java
long shardTopN = extractTopNFromPlan(plan);
```
Then pass `shardTopN` to the method and use it for pruning the `HashMap<LongArrayKey, LongOpenHashSet>` before building output pages.
