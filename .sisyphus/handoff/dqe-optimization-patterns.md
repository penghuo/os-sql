# DQE Optimization Patterns for ClickBench Queries

## 1. COUNT(DISTINCT) Fast Paths — TransportShardExecuteAction.java

### `executeCountDistinctWithHashSets` (line 960)
```java
private ShardExecuteResponse executeCountDistinctWithHashSets(
    AggregationNode aggNode, ShardExecuteRequest req,
    String keyName0, String keyName1, Type type0, Type type1)
```
**Logic**: 2-key GROUP BY (key0) with `LongOpenHashSet` per group to collect key1 values. Parallel segment scanning — each segment builds its own per-group HashSet map, then merges across segments. Outputs compact page (~450 rows) + attached HashSets for coordinator union.

### `executeVarcharCountDistinctWithHashSets` (line 1798)
```java
private ShardExecuteResponse executeVarcharCountDistinctWithHashSets(
    AggregationNode aggNode, ShardExecuteRequest req,
    String keyName0, String keyName1, Type t1)
```
**Logic**: VARCHAR key0 + numeric key1 pattern (Q14: GROUP BY SearchPhrase, COUNT(DISTINCT UserID)). Uses ordinal-indexed arrays per segment (no hash computation for VARCHAR keys), defers string resolution to merge phase.

### `executeNKeyCountDistinctWithHashSets` (line 1248)
```java
private ShardExecuteResponse executeNKeyCountDistinctWithHashSets(
    AggregationNode aggNode, ShardExecuteRequest req,
    List<String> keys, List<Type> keyTypes)
```
**Logic**: N-key path (3+ keys): GROUP BY (key0..keyN-2) with HashSet for keyN-1 (dedup key). Full dedup tuples output for coordinator's generic merge path.

### `executeMixedDedupWithHashSets` (line 1537)
```java
private ShardExecuteResponse executeMixedDedupWithHashSets(
    AggregationNode aggNode, ShardExecuteRequest req,
    String keyName0, String keyName1, Type t0, Type t1)
```
**Logic**: Native path for GROUP BY key0 with per-group HashSet for key1 + accumulators for decomposable aggregates (SUM/COUNT). Handles mixed dedup queries (Q10 pattern).

### Bare scan fast paths (lines 246-270)
- **Numeric**: `isBareSingleNumericColumnScan` → collects distinct values into `LongOpenHashSet`, attaches raw set to response.
- **VARCHAR**: `isBareSingleVarcharColumnScan` → ordinal-based dedup via `FixedBitSet` for fast ordinal collection, attaches raw string set.

---

## 2. GROUP BY + ORDER BY LIMIT (TopN) — FusedGroupByAggregate.java

### `executeWithTopN` (line 907)
```java
public static List<Page> executeWithTopN(
    AggregationNode aggNode, IndexShard shard, Query query,
    Map<String, Type> columnTypeMap,
    int sortAggIndex, boolean sortAscending, long topN)
```
**Logic**: Delegates to `executeInternal` with topN > 0. Avoids full Page construction for high-cardinality GROUP BY with ORDER BY + LIMIT (Q17/Q18).

### `executeSingleVarcharCountStar` (line 1275)
```java
private static List<Page> executeSingleVarcharCountStar(
    IndexShard shard, Query query, String columnName,
    Map<String, Type> columnTypeMap, List<String> groupByKeys,
    int sortAggIndex, boolean sortAscending, long topN)
```
**Logic**: Ultra-fast path for single VARCHAR key + COUNT(*). Uses per-segment ordinal arrays. Single-segment index → ordinals ARE the final result (zero String allocations during aggregation). When topN > 0, applies **heap-based top-N selection** directly on ordinal count array (lines 1366-1391):
- Min-heap (DESC) or max-heap (ASC) of size N
- Compares `heapVals[k]` against parent, swaps to maintain heap property
- Avoids BlockBuilder construction for all groups — critical for Q34 (~700K URL groups, LIMIT 10)

---

## 3. Dispatch Logic — `executePlan` (line 194)

The dispatch is a **priority-ordered if-else chain** in `executePlan`:

1. **Unwrap ProjectNode** (line 196) — enables fused paths for single-shard indices
2. **Scalar COUNT(*)** → `executeScalarCountStar` — short-circuits pipeline entirely
3. **Lucene-native sorted scan** → `executeSortedScan` — LimitNode→SortNode→TableScanNode; uses `IndexSearcher.search(query, topN, Sort)` with early termination
4. **Fused scan-aggregate** → `executeFusedScanAggregate` — scalar aggregations (no GROUP BY)
5. **Bare numeric column scan** → `executeDistinctValuesScanWithRawSet` — COUNT(DISTINCT) pre-dedup
6. **Bare VARCHAR column scan** → `executeDistinctValuesScanVarcharWithRawSet` — ordinal-based dedup
7. **Fused eval-aggregate** → `executeFusedEvalAggregate` — SUM(col + constant) patterns
8. **COUNT(DISTINCT) dedup plan** (N≥2 keys) → dispatches to 2-key optimized, VARCHAR, N-key, or mixed dedup paths
9. **Ordinal-cached expression GROUP BY** (line 360) → `executeFusedExprGroupByAggregate` — pre-computes expression per unique ordinal (~16K evals vs ~921K docs)
10. **Fused ordinal GROUP BY** (line 378) → `executeFusedGroupByAggregate` — uses `FusedGroupByAggregate.canFuse()` check
11. **Fused GROUP BY + sort+limit** (line 393) → `executeFusedGroupByAggregateWithTopN` — extracts LimitNode→SortNode→AggregationNode pattern, applies TopN in-process
12. **Fallback** → generic operator pipeline (ScanOperator → HashAggregationOperator)

---

## 4. Ordinal-Based Optimization for Low-Cardinality String GROUP BY

**Yes, this exists extensively.**

### Fused ordinal GROUP BY — TransportShardExecuteAction.java (line 378)
```java
// Try fused ordinal-based GROUP BY for aggregations with string group keys
if (scanFactory == null
    && effectivePlan instanceof AggregationNode aggGroupNode
    && FusedGroupByAggregate.canFuse(aggGroupNode, ...columnTypeMap()))
```
Uses `SortedSetDocValues` ordinals as hash keys during grouping. Avoids expensive `lookupOrd()` per row — defers string resolution to final output phase.

### Ordinal-cached expression GROUP BY — TransportShardExecuteAction.java (line 360)
```java
// Try ordinal-cached expression GROUP BY: AggregationNode -> EvalNode -> TableScanNode
// VARCHAR column. Pre-computes the expression once per unique ordinal (~16K evaluations
// instead of ~921K per-doc evaluations)
```
Method: `executeFusedExprGroupByAggregate` (line 2024). Pre-computes GROUP BY expression once per unique ordinal, caches result, reuses during scan.

### Per-segment ordinal arrays — FusedGroupByAggregate.java (line 1270)
`executeSingleVarcharCountStar` uses per-segment ordinal count arrays. For single-segment indices, the ordinal array IS the final result — zero String allocations during aggregation. Multi-segment falls back to byte[]-based HashMap avoiding UTF-8→char[] round-trip.

### VARCHAR COUNT(DISTINCT) ordinal indexing — TransportShardExecuteAction.java (line 1815)
Per-group accumulators use ordinal-indexed arrays (no hash computation for VARCHAR keys). Ordinals resolved only during cross-segment merge phase.
