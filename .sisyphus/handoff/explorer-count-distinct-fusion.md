# COUNT(DISTINCT) Dispatch Paths in TransportShardExecuteAction.java

## ClickBench Queries (from queries_trino.sql)
- **Q04** (line 5): `SELECT COUNT(DISTINCT UserID) FROM hits;` — scalar, no GROUP BY
- **Q08** (line 9): `SELECT RegionID, COUNT(DISTINCT UserID) AS u FROM hits GROUP BY RegionID ORDER BY u DESC LIMIT 10;`
- **Q09** (line 10): `SELECT RegionID, SUM(AdvEngineID), COUNT(*), AVG(ResolutionWidth), COUNT(DISTINCT UserID) FROM hits GROUP BY RegionID ORDER BY c DESC LIMIT 10;` — mixed aggs
- **Q11** (line 12): `SELECT MobilePhone, MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM hits WHERE MobilePhoneModel <> '' GROUP BY MobilePhone, MobilePhoneModel ORDER BY u DESC LIMIT 10;` — VARCHAR+numeric keys
- **Q13** (line 14): `SELECT SearchPhrase, COUNT(DISTINCT UserID) AS u FROM hits WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY u DESC LIMIT 10;` — VARCHAR key

## Calcite Two-Level Decomposition

Calcite decomposes `COUNT(DISTINCT col)` into:
- **Inner (shard)**: `AggregationNode(PARTIAL)` with `GROUP BY (groupKeys + distinctCol)` and `COUNT(*)`
- **Outer (coordinator)**: Full aggregation on pre-deduped data

This is done in **PlanFragmenter.java** (`buildShardPlan`, lines 144-157):
```java
// SINGLE with GROUP BY: shard-level dedup for COUNT(DISTINCT)
List<String> distinctColumns = extractCountDistinctColumns(aggNode.getAggregateFunctions());
if (distinctColumns != null) {
    List<String> dedupKeys = new ArrayList<>(aggNode.getGroupByKeys());
    dedupKeys.addAll(distinctColumns);
    return new AggregationNode(aggNode.getChild(), dedupKeys, List.of("COUNT(*)"), AggregationNode.Step.PARTIAL);
}
```

For **Q04** (scalar, no GROUP BY): PlanFragmenter strips the AggregationNode entirely (line 168: `return aggNode.getChild()`), leaving a bare `TableScanNode`.

## Dispatch Paths in TransportShardExecuteAction.java (executePlan, starting ~line 195)

### Path 1: Scalar COUNT(DISTINCT) — Q04
- **Detection**: `isBareSingleNumericColumnScan(plan)` (line 270) — bare TableScanNode with 1 numeric col
- **Execution**: `executeDistinctValuesScanWithRawSet()` (line 3024) — collects into `LongOpenHashSet`, attaches to response
- **VARCHAR variant**: `isBareSingleVarcharColumnScan()` (line 2960) → `executeDistinctValuesScanVarcharWithRawSet()` (line 3054)
- **Coordinator merge**: `TransportTrinoSqlAction.java` lines 2018-2073 — unions `LongOpenHashSet` across shards

### Path 2: GROUP BY + COUNT(DISTINCT) with 2 numeric keys — Q08
- **Detection**: lines 298-302 — `AggregationNode(PARTIAL)` with `groupByKeys.size() >= 2` and `FusedGroupByAggregate.canFuse()`
- **Sub-check**: `isSingleCountStar` (line 305-306) — exactly 1 agg = `COUNT(*)`
- **2-key numeric**: `executeCountDistinctWithHashSets()` (line 1001) — per-group `LongOpenHashSet` accumulators
- **Coordinator merge**: lines 2540-2618 in TransportTrinoSqlAction — unions per-group HashSets across shards

### Path 3: VARCHAR key0 + numeric key1 — Q13
- **Detection**: lines 331-336 — `t0 instanceof VarcharType && t1 is numeric`
- **Execution**: `executeVarcharCountDistinctWithHashSets()` (line 2411)

### Path 4: Mixed-type N-key — Q11
- **Detection**: lines 340-375 — 3+ keys, last key numeric, some VARCHAR keys
- **Execution**: `executeMixedTypeCountDistinctWithHashSets()` (line 1650) — Object-based composite keys

### Path 5: Mixed aggregates (SUM + COUNT + COUNT(DISTINCT)) — Q09
- **Detection**: lines 380-395 — `isMixedDedup` with 2 keys
- **Execution**: `executeMixedDedupWithHashSets()` (line 1932) — per-group HashSet + SUM/COUNT accumulators

## Key Method Signatures & Line Numbers

| Method | Line | Purpose |
|--------|------|---------|
| `executePlan()` | 195 | Main dispatch entry point |
| `executeFusedScanAggregate()` | 949 | Scalar aggs (SUM/AVG/MIN/MAX/COUNT) |
| `executeCountDistinctWithHashSets()` | 1001 | 2 numeric keys, COUNT(*) only |
| `executeNKeyCountDistinctWithHashSets()` | 1330 | 3+ numeric keys |
| `executeMixedTypeCountDistinctWithHashSets()` | 1650 | N keys with VARCHAR mix |
| `executeMixedDedupWithHashSets()` | 1932 | Mixed aggs (SUM+COUNT+COUNT(DISTINCT)) |
| `executeVarcharCountDistinctWithHashSets()` | 2411 | VARCHAR key0 + numeric key1 |
| `executeDistinctValuesScanWithRawSet()` | 3024 | Scalar numeric COUNT(DISTINCT) |
| `executeDistinctValuesScanVarcharWithRawSet()` | 3054 | Scalar VARCHAR COUNT(DISTINCT) |
| `isBareSingleNumericColumnScan()` | 2938 | Detects bare numeric scan |
| `isBareSingleVarcharColumnScan()` | 2960 | Detects bare VARCHAR scan |

## Fusion Intercept Points

To fuse the two-level aggregation into one:

1. **PlanFragmenter.buildShardPlan() lines 144-157**: This is where the decomposition happens. Instead of creating a dedup `AggregationNode(PARTIAL)`, you could mark the node with a `FUSED_COUNT_DISTINCT` step and keep the original aggregation semantics.

2. **TransportShardExecuteAction dispatch (lines 292-400)**: The current code already fuses at the shard level — it detects the dedup pattern (`AggregationNode(PARTIAL)` with N keys + `COUNT(*)`) and builds per-group HashSets directly. The "two levels" are already collapsed into one DocValues pass per shard.

3. **Coordinator merge in TransportTrinoSqlAction (lines 2018-2073 for scalar, 2540-2618 for grouped)**: The coordinator unions HashSets across shards and emits final counts. This is the second "level" — it could be optimized with approximate counting (HyperLogLog) if exact counts aren't needed.

**Key insight**: The current implementation already fuses the inner GROUP BY + COUNT(*) into a single DocValues pass with HashSet accumulators at the shard level. The "two levels" from Calcite's perspective are:
- Level 1 (shard): DocValues scan → per-group HashSet (fused, not actually doing GROUP BY + COUNT(*) separately)
- Level 2 (coordinator): Union HashSets across shards → emit COUNT per group
