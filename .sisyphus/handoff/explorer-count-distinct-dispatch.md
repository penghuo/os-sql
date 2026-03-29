# COUNT(DISTINCT) Dispatch Logic — Exploration Findings

## 1. Dispatch Logic in TransportShardExecuteAction.java (lines 246-335)

The dispatch is a cascading series of `if` checks in the main `executeShardPlan` method. COUNT(DISTINCT) has **three fast paths**:

### Path A: Scalar COUNT(DISTINCT) — bare TableScanNode (lines 246-261)
- **Numeric**: `isBareSingleNumericColumnScan(plan)` → `executeDistinctValuesScanWithRawSet()` — collects distinct values into raw `LongOpenHashSet`, attaches to response
- **VARCHAR**: `isBareSingleVarcharColumnScan(plan)` → `executeDistinctValuesScanVarcharWithRawSet()` — ordinal-based dedup via `FixedBitSet`

### Path B: GROUP BY + COUNT(DISTINCT) — AggregationNode with 2 keys (lines 273-335)
Detection conditions (line 277-282):
```java
scanFactory == null
&& effectivePlan instanceof AggregationNode aggDedupNode
&& aggDedupNode.getStep() == AggregationNode.Step.PARTIAL
&& aggDedupNode.getGroupByKeys().size() == 2
&& FusedGroupByAggregate.canFuse(aggDedupNode, columnTypeMap)
```

Then sub-routes:
- **isSingleCountStar** (line 284-286): exactly 1 agg function == `"COUNT(*)"` 
  - Numeric key0 + Numeric key1 → `executeCountDistinctWithHashSets()` (line 304)
  - VARCHAR key0 + Numeric key1 → `executeVarcharCountDistinctWithHashSets()` (line 312)
- **isMixedDedup** (line 288-291): all aggs match `(sum|count)\(.*\)` pattern
  - Numeric key0 + Numeric key1 → `executeMixedDedupWithHashSets()` (line 330)

## 2. AggDedupNode Detection

There is **no separate AggDedupNode class**. The "dedup" detection is done by checking an `AggregationNode` with:
- `Step.PARTIAL` (shard-local)
- Exactly 2 group-by keys (key0 = real group key, key1 = the DISTINCT column)
- `FusedGroupByAggregate.canFuse()` returns true

This pattern arises because Calcite decomposes `GROUP BY x, COUNT(DISTINCT y)` into a two-level aggregation where the inner level does `GROUP BY (x, y)` with `COUNT(*)`, effectively deduplicating y per group x.

**AggregationNode** (file: `dqe/src/main/java/org/opensearch/sql/dqe/planner/plan/AggregationNode.java`):
- Fields: `child`, `groupByKeys` (List<String>), `aggregateFunctions` (List<String>), `step` (PARTIAL/FINAL/SINGLE)

## 3. executeCountDistinctWithHashSets (line 936)

```java
private ShardExecuteResponse executeCountDistinctWithHashSets(
    AggregationNode aggNode, ShardExecuteRequest req,
    String keyName0, String keyName1, Type type0, Type type1)
```

**Approach**:
1. Acquires `Engine.Searcher` on the shard
2. Builds Lucene query from `scanNode.getDslFilter()`
3. **Parallel segment scanning**: each Lucene segment builds its own `Map<Long, LongOpenHashSet>` (key0 → set of key1 values)
4. Single segment → direct scan; multi-segment → `ForkJoinPool` parallel dispatch
5. Merges per-segment maps by unioning HashSets
6. Outputs compact pages (~450 rows) + attached HashSets for coordinator to union across shards

## 4. FusedGroupByAggregate COUNT(DISTINCT) Handling

**File**: `dqe/src/main/java/org/opensearch/sql/dqe/shard/source/FusedGroupByAggregate.java`

### canFuse() (line 196)
Validates:
- Group-by keys are non-empty
- Child is a `TableScanNode` (possibly via `EvalNode`)
- All group-by keys are supported types (VARCHAR, numeric, timestamp) or recognized expressions (DATE_TRUNC, EXTRACT, arithmetic)
- All aggregate functions match `AGG_FUNCTION` regex: `(COUNT|SUM|MIN|MAX|AVG)((DISTINCT )?(.+?))`
- Aggregate arguments are physical columns, `*`, or supported expressions like `length(col)`

### CountDistinctAccum (line 12604)
Inner class implementing `MergeableAccumulator`:
- **Numeric non-double**: uses `LongOpenHashSet` (primitive, no boxing)
- **VARCHAR/double**: falls back to `HashSet<Object>`
- `merge()`: unions the sets
- `writeTo()`: emits `set.size()` as BIGINT
- Agg type code = `5` in switch statements (lines 3370, 3680, 7654, 7712, 12385)

## Key File References

| File | Lines | Purpose |
|------|-------|---------|
| `TransportShardExecuteAction.java` | 246-261 | Scalar COUNT(DISTINCT) bare scan dispatch |
| `TransportShardExecuteAction.java` | 273-335 | GROUP BY + COUNT(DISTINCT) dedup dispatch |
| `TransportShardExecuteAction.java` | 936-1000+ | `executeCountDistinctWithHashSets` implementation |
| `TransportShardExecuteAction.java` | 1224 | `executeMixedDedupWithHashSets` |
| `TransportShardExecuteAction.java` | 1485 | `executeVarcharCountDistinctWithHashSets` |
| `FusedGroupByAggregate.java` | 196-293 | `canFuse()` validation |
| `FusedGroupByAggregate.java` | 12600-12650 | `CountDistinctAccum` inner class |
| `AggregationNode.java` | 1-70 | Plan node with groupByKeys, aggregateFunctions, step |
