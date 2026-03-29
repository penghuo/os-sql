# COUNT(DISTINCT) Handling: PlanFragmenter + TransportTrinoSqlAction

## 1. Pipeline Flow for COUNT(DISTINCT)

### Step 1: LogicalPlanner
Always creates `AggregationNode(PARTIAL, ...)` for all aggregations.

### Step 2: PlanOptimizer (`AggregationSplitVisitor`)
`hasNonDecomposableAgg()` (line ~396) detects COUNT(DISTINCT) and forces **SINGLE** step:
```java
if (upper.contains("COUNT(DISTINCT")) hasCountDistinct = true;
// ...
if (hasCountDistinct) return true; // → forces SINGLE step
```
Also forces SINGLE for AVG without companion COUNT in GROUP BY queries.

### Step 3: PlanFragmenter (`buildShardPlan()`)
Decision tree (line ~100):
```
if (aggNode.getStep() == PARTIAL) {
  // Standard decomposable aggs: inflated limit, limit-only, or strip above agg
  return ...;
}
// Reaches here for SINGLE step (COUNT(DISTINCT) queries):
if (!aggNode.getGroupByKeys().isEmpty() && !columnTypeMap.isEmpty()) {
  // Try dedup paths for GROUP BY queries
  distinctCols = extractCountDistinctColumns(aggFunctions);
  if (distinctCols != null) → buildDedupShardPlan  // all-COUNT(DISTINCT)
  mixedPlan = buildMixedDedupShardPlan(aggNode);
  if (mixedPlan != null) → return mixedPlan         // mixed aggs
}
// Fallback: shards only scan+filter, coordinator does full aggregation
return aggNode.getChild();
```

### Step 4: Coordinator Plan (`buildCoordinatorPlan()`)
- PARTIAL → coordinator = FINAL
- SINGLE → coordinator = **SINGLE** (preserves original agg functions)

---

## 2. PlanFragmenter — Shard Plan Construction

### `extractCountDistinctColumns()` (line ~280)
- Returns non-null only if **ALL** agg functions are `COUNT(DISTINCT col)`.
- If any agg is NOT `COUNT(DISTINCT ...)`, returns null.
- Used for Q08, Q11, Q13 patterns.

### Dedup Shard Plan (all-COUNT(DISTINCT) + GROUP BY)
Built when `extractCountDistinctColumns()` succeeds:
```
AggregationNode(PARTIAL, groupBy=[origKeys + distinctCols], aggs=["COUNT(*)"])
```
Example Q08: `GROUP BY (RegionID, UserID)` with `COUNT(*)`

### `buildMixedDedupShardPlan()` (line ~185)
- Requires BOTH `hasCountDistinct` AND `hasDecomposable`.
- Decomposes AVG(col) → SUM(col) + COUNT(col).
- COUNT(DISTINCT) columns added to GROUP BY, their agg functions omitted from shard aggs.
```
AggregationNode(PARTIAL, groupBy=[origKeys + distinctCols], aggs=[decomposed non-distinct aggs])
```
Example Q09: `GROUP BY (RegionID, UserID)` with `SUM(AdvEngineID), COUNT(*), SUM(ResolutionWidth), COUNT(ResolutionWidth)`

### Scalar COUNT(DISTINCT) (no GROUP BY) — Q04, Q05
- `aggNode.getGroupByKeys().isEmpty()` → skips dedup paths entirely.
- Falls to `return aggNode.getChild()` → shards only scan+filter (emit raw rows).
- Coordinator plan: `AggregationNode(SINGLE, groupBy=[], aggs=["COUNT(DISTINCT col)"])`

---

## 3. TransportTrinoSqlAction — Coordinator Merge Dispatch

### Merge Decision Tree (checked IN ORDER):

| Path | Condition | Handles | Merge Method |
|------|-----------|---------|--------------|
| A | FINAL + no GROUP BY | Scalar SUM/COUNT/MIN/MAX/AVG | `mergeScalarAggregation()` |
| B | SINGLE + `isScalarCountDistinctLong()` | Scalar COUNT(DISTINCT numericCol) | `mergeCountDistinctValues()` / `mergeCountDistinctValuesViaRawSets()` |
| C | SINGLE + `isScalarCountDistinctVarchar()` | Scalar COUNT(DISTINCT varcharCol) | `mergeCountDistinctVarcharValues()` / `mergeCountDistinctVarcharViaRawSets()` |
| D | SINGLE + `isShardDedupCountDistinct()` | GROUP BY + all-COUNT(DISTINCT) | `mergeDedupCountDistinct()` / `mergeDedupCountDistinctViaSets()` / `mergeDedupCountDistinctViaVarcharSets()` |
| E | SINGLE + `isShardMixedDedup()` | GROUP BY + mixed aggs w/ COUNT(DISTINCT) | `mergeMixedDedup()` / `mergeMixedDedupViaSets()` |
| F | SINGLE (fallback) | Any SINGLE agg | `runCoordinatorAggregation()` — full agg on raw data |
| G | FINAL (any) | Standard PARTIAL/FINAL merge | `merger.mergeAggregation()` |

### Detection Functions:

**`isScalarCountDistinctLong()`**: no GROUP BY, exactly 1 agg, starts with "COUNT(DISTINCT ", column is BigintType/IntegerType/SmallintType/TinyintType/TimestampType.

**`isScalarCountDistinctVarchar()`**: no GROUP BY, exactly 1 agg, starts with "COUNT(DISTINCT ", column is VarcharType.

**`isShardDedupCountDistinct()`**: shardPlan is PARTIAL AggregationNode, coordinator is SINGLE, has GROUP BY, shard has MORE group-by keys than coordinator, shard aggs == exactly `["COUNT(*)"]`.

**`isShardMixedDedup()`**: shardPlan is PARTIAL, has GROUP BY, shard has MORE group-by keys, shard has >1 agg function, coordinator aggs contain COUNT(DISTINCT).

---

## 4. Merge Implementations (Key Methods)

### `mergeCountDistinctValues()` — Scalar numeric COUNT(DISTINCT)
- Shards emit pages of unique long values (pre-deduped per shard).
- Coordinator unions via `LongOpenHashSet`, returns count.
- Local fast path: `mergeCountDistinctValuesViaRawSets()` uses raw HashSet attachments from `ShardExecuteResponse.getScalarDistinctSet()`.

### `mergeCountDistinctVarcharValues()` — Scalar VARCHAR COUNT(DISTINCT)
- Shards emit pages of unique string values.
- Coordinator unions via `SliceRangeHashSet` (zero-copy, operates on raw byte ranges).
- Local fast path: `mergeCountDistinctVarcharViaRawSets()` uses `ShardExecuteResponse.getScalarDistinctStrings()`.

### `mergeDedupCountDistinct()` — GROUP BY + COUNT(DISTINCT)
Two-stage merge:
1. **Stage 1**: FINAL merge on dedup keys (removes cross-shard duplicates)
2. **Stage 2**: GROUP BY original keys with COUNT(*) → COUNT(DISTINCT)

Fast paths:
- `mergeDedupCountDistinct2Key()`: Ultra-fast for exactly 2 numeric dedup keys (flat long[] arrays)
- `mergeDedupCountDistinctVarcharKey()`: VARCHAR key + numeric distinct col via `SliceLongDedupMap`
- `mergeDedupCountDistinctViaSets()`: Uses per-group `LongOpenHashSet` attachments from shards
- `mergeDedupCountDistinctViaVarcharSets()`: Uses per-group `LongOpenHashSet` attachments with VARCHAR keys

### `mergeMixedDedup()` — GROUP BY + mixed aggs + COUNT(DISTINCT)
Two-stage merge:
1. **Stage 1**: FINAL merge on dedup keys + merge partial aggs (SUM→sum, COUNT→sum)
2. **Stage 2**: Re-aggregate by original keys: SUM partials, compute AVG from SUM/COUNT, COUNT rows for COUNT(DISTINCT)

Fast paths:
- `mergeMixedDedup2Key()`: 2 numeric dedup keys, flat long[] arrays
- `mergeMixedDedupViaSets()`: Uses per-group `LongOpenHashSet` attachments

---

## 5. What's MISSING for Q04, Q05, Q08, Q09, Q11, Q13

### Q04: `SELECT COUNT(DISTINCT UserID) FROM hits`
- **Plan**: SINGLE, no GROUP BY, 1 agg = "COUNT(DISTINCT UserID)"
- **Shard plan**: `aggNode.getChild()` = scan+filter (emits raw rows)
- **Coordinator**: Should hit Path B (`isScalarCountDistinctLong`)
- **ISSUE**: Shards emit ALL raw rows (no pre-dedup). Path B expects shards to emit pre-deduped distinct values. The shard executor needs to handle scalar COUNT(DISTINCT) by emitting distinct values, not raw rows. If the shard executor doesn't do this, it falls to Path F (`runCoordinatorAggregation`) which runs full aggregation on all raw data — correct but slow.
- **Likely actual problem**: The shard plan is just a TableScanNode (no aggregation), so `isScalarCountDistinctLong()` checks pass at coordinator level, but the shard data is raw rows not distinct values → `mergeCountDistinctValues()` tries to union ALL values as distinct → may work but is very slow, OR the shard executor has special handling.

### Q05: `SELECT COUNT(DISTINCT SearchPhrase) FROM hits`
- Same as Q04 but VARCHAR. Should hit Path C.
- Same potential issue with shard plan being raw scan.

### Q08: `SELECT RegionID, COUNT(DISTINCT UserID) FROM hits GROUP BY RegionID`
- **Plan**: SINGLE, GROUP BY=[RegionID], aggs=["COUNT(DISTINCT UserID)"]
- **Shard plan**: Dedup path → `AggregationNode(PARTIAL, groupBy=[RegionID, UserID], aggs=["COUNT(*)"])`
- **Coordinator**: Should hit Path D (`isShardDedupCountDistinct`)
- **SHOULD WORK** if PlanOptimizer correctly sets SINGLE and PlanFragmenter builds dedup plan.
- **Potential issue**: Check if `isShardDedupCountDistinct()` correctly matches — it requires shard aggs == `["COUNT(*)"]` and shard has more group-by keys. This should match.

### Q09: `SELECT RegionID, SUM(AdvEngineID), COUNT(*), AVG(ResolutionWidth), COUNT(DISTINCT UserID) FROM hits GROUP BY RegionID`
- **Plan**: SINGLE (due to COUNT(DISTINCT)), GROUP BY=[RegionID], mixed aggs
- **Shard plan**: Mixed dedup → `AggregationNode(PARTIAL, groupBy=[RegionID, UserID], aggs=["SUM(AdvEngineID)", "COUNT(*)", "SUM(ResolutionWidth)", "COUNT(ResolutionWidth)"])`
- **Coordinator**: Should hit Path E (`isShardMixedDedup`)
- **SHOULD WORK** if pipeline is correct.

### Q11: `SELECT MobilePhoneModel, COUNT(DISTINCT UserID) FROM hits GROUP BY MobilePhoneModel ORDER BY COUNT(DISTINCT UserID) DESC LIMIT 10`
- **Plan**: SINGLE, GROUP BY=[MobilePhoneModel], aggs=["COUNT(DISTINCT UserID)"]
- **Shard plan**: Dedup path → `AggregationNode(PARTIAL, groupBy=[MobilePhoneModel, UserID], aggs=["COUNT(*)"])`
- **Coordinator**: Should hit Path D
- **Potential issue**: MobilePhoneModel is VARCHAR. `mergeDedupCountDistinct()` has a VARCHAR key fast path (`mergeDedupCountDistinctVarcharKey`) that handles this. The `mergeDedupCountDistinctViaVarcharSets()` also handles VARCHAR keys. Should work.
- **ORDER BY + LIMIT**: `applyCoordinatorSort()` is called after merge. Sort key "COUNT(DISTINCT UserID)" needs to be found in agg output columns. The coordinator agg output is [MobilePhoneModel, COUNT(DISTINCT UserID)]. The sort key from the optimized plan's SortNode should reference this.

### Q13: `SELECT SearchPhrase, COUNT(DISTINCT UserID) FROM hits WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY COUNT(DISTINCT UserID) DESC LIMIT 10`
- Same as Q11 but with WHERE filter. Filter is pushed to TableScanNode by PlanOptimizer.
- Should work same as Q11.

### Summary: The Pipeline SHOULD Work

Based on code analysis, the full pipeline (PlanOptimizer → SINGLE step → PlanFragmenter dedup → TransportTrinoSqlAction merge) appears to be correctly wired for all 6 queries. The merge implementations exist and handle all the required patterns.

**If these queries are failing, the issue is likely in one of:**
1. **Shard executor**: The shard-level execution of the dedup PARTIAL plan may not correctly handle the expanded GROUP BY or may not attach HashSets to responses.
2. **Column type resolution**: `columnTypeMap` may be empty or incorrect, causing the dedup path condition `!columnTypeMap.isEmpty()` to fail.
3. **Sort key resolution**: For Q11/Q13, the ORDER BY `COUNT(DISTINCT UserID)` may not resolve correctly in `applyCoordinatorSort()` because the coordinator agg output column name may differ from the sort key name.
4. **Scalar COUNT(DISTINCT) shard plan**: For Q04/Q05, the shard plan is just scan+filter. The coordinator expects pre-deduped values but gets raw rows. The `mergeCountDistinctValues()` method would still work (it unions all values) but performance would be poor. However, if the shard executor has a special code path for scalar COUNT(DISTINCT) that emits distinct values, it should work.
5. **The actual error**: Need to check actual error messages/stack traces to pinpoint the failure.
