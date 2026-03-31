# COUNT(DISTINCT) Dispatch Path Analysis

## 1. executePlan Detection & Routing (TransportShardExecuteAction.java)

**Method**: `executePlan(DqePlanNode plan, ShardExecuteRequest req)` — line 195

The method unwraps `ProjectNode` (line 225-231), then checks fast paths in order:

### Bare scan paths (scalar COUNT(DISTINCT), no GROUP BY):
- **Line 265-273**: `isBareSingleNumericColumnScan(plan)` → `executeDistinctValuesScanWithRawSet()` — collects distinct values into `LongOpenHashSet`, attaches to response
- **Line 275-280**: `isBareSingleVarcharColumnScan(plan)` → `executeDistinctValuesScanVarcharWithRawSet()` — ordinal-based dedup via `FixedBitSet`

### AggDedupNode detection (GROUP BY + COUNT(DISTINCT), line 292-398):
Conditions: `effectivePlan instanceof AggregationNode`, `step == PARTIAL`, `groupByKeys.size() >= 2`, `FusedGroupByAggregate.canFuse()`

Classifies into:
- **isSingleCountStar**: exactly 1 agg = `COUNT(*)` (pure dedup)
- **isMixedDedup**: all aggs match `(sum|count)\(.*\)` (no DISTINCT, no AVG)

### Fast path dispatch (2-key pure dedup):
| Key types | Method | Line |
|-----------|--------|------|
| numeric+numeric | `executeCountDistinctWithHashSets()` | 328 |
| varchar+numeric | `executeVarcharCountDistinctWithHashSets()` | 336 |

### Fast path dispatch (3+ key pure dedup):
| Key types | Method | Line |
|-----------|--------|------|
| all numeric | `executeNKeyCountDistinctWithHashSets()` | 354 |
| mixed (last numeric) | `executeMixedTypeCountDistinctWithHashSets()` | 374 |

### Mixed dedup (2-key, SUM/COUNT + COUNT(DISTINCT)):
| Key types | Method | Line |
|-----------|--------|------|
| numeric+numeric | `executeMixedDedupWithHashSets()` | 395 |

---

## 2. Two-Level Calcite Plan Pattern

**PlanOptimizer.java** (line 370-408): `hasNonDecomposableAgg()` detects `COUNT(DISTINCT)` and forces `Step.SINGLE`.

For `SELECT x, COUNT(DISTINCT y) FROM t GROUP BY x`, the plan is:

```
Coordinator: AggregationNode(SINGLE, groupBy=[x], aggs=[COUNT(DISTINCT y)])
                |
PlanFragmenter transforms to:
                |
Shard:    AggregationNode(PARTIAL, groupBy=[x, y], aggs=[COUNT(*)]) → TableScan
Coord:    AggregationNode(SINGLE, groupBy=[x], aggs=[COUNT(DISTINCT y)])
```

**PlanFragmenter.java** (line 147-160): `extractCountDistinctColumns()` extracts distinct columns, builds dedup GROUP BY = `[original_keys + distinct_columns]` with `COUNT(*)`.

Key: COUNT(DISTINCT) is NOT decomposable into PARTIAL/FINAL. Instead, shards pre-dedup by adding the distinct column to GROUP BY keys.

---

## 3. FusedGroupByAggregate accType=5 Handling

**Flat path rejection** (line 3535-3589):
```java
case "COUNT":
    if (spec.isDistinct) {
        accType[i] = 5;              // COUNT(DISTINCT)
        canUseFlatAccumulators = false;  // REJECTS flat path
    } else {
        accType[i] = 0;
    }
```

Same pattern at line 4089-4101 (derived single-key path):
```java
if (spec.isDistinct) canUseFlat = false;
```

**Fallback**: When `canUseFlatAccumulators = false`, falls to line 3603+ which uses `SingleKeyHashMap` with `AccumulatorGroup` objects (per-group object allocation). The non-flat path handles accType=5 at:
- Line 3732: `case 5: // COUNT(DISTINCT)` in single-key path
- Line 4042: `case 5: // COUNT(DISTINCT)` in another path
- Line 9131, 9189: `case 5: // COUNT(DISTINCT)` in non-flat two-key path

---

## 4. Existing COUNT(DISTINCT) Fast Paths in TransportShardExecuteAction

| Method | Line | Strategy |
|--------|------|----------|
| `executeDistinctValuesScanWithRawSet()` | 3024 | Scalar numeric: raw `LongOpenHashSet` attached to response |
| `executeDistinctValuesScanVarcharWithRawSet()` | 3054 | Scalar varchar: `FixedBitSet` ordinal dedup → string set |
| `executeCountDistinctWithHashSets()` | 1001 | 2-key numeric: `Map<Long, LongOpenHashSet>` per group, parallel segments |
| `executeVarcharCountDistinctWithHashSets()` | 2411 | varchar key0 + numeric key1: per-group HashSets |
| `executeNKeyCountDistinctWithHashSets()` | 1330 | 3+ numeric keys: per-group HashSets |
| `executeMixedTypeCountDistinctWithHashSets()` | 1650 | Mixed-type N keys: Object-based composite keys |
| `executeMixedDedupWithHashSets()` | 1932 | 2-key with SUM/COUNT + COUNT(DISTINCT) |

---

## 5. Coordinator Merge (TransportTrinoSqlAction.java)

### Scalar COUNT(DISTINCT):
- **Line 655**: `mergeCountDistinctValuesViaRawSets()` — unions `LongOpenHashSet` from shard attachments. Finds largest set, merges others into temp set, parallel `contains()` checks. Falls back to Page-based `mergeCountDistinctValues()` (line 1990).
- **Line 659**: `mergeCountDistinctVarcharViaRawSets()` — same pattern for varchar.

### GROUP BY + COUNT(DISTINCT) (dedup merge):
Detection: `isShardDedupCountDistinct()` (line 2215) — checks shard plan is `PARTIAL` with more group-by keys than coordinator's `SINGLE` plan.

Two-stage merge (`mergeDedupCountDistinct()`, line 2255):
1. **Stage 1**: FINAL merge on dedup keys (removes cross-shard duplicates)
2. **Stage 2**: GROUP BY original keys with COUNT(*) → COUNT(DISTINCT)

Optimized paths:
- **Line 2293**: `mergeDedupCountDistinct2Key()` — ultra-fast 2-key fused path with flat `long[]` arrays
- **Line 700**: `mergeDedupCountDistinctViaSets()` — uses HashSet attachments from shards (single original key)
- **Line 686**: `mergeDedupCountDistinctViaVarcharSets()` — varchar variant

### Mixed dedup merge:
- **Line 733**: `mergeMixedDedupViaSets()` — merges SUM/COUNT accumulators + unions HashSets
- **Line 745**: `mergeMixedDedup()` — fallback Page-based merge
