# Coordinator Merge Logic for COUNT(DISTINCT) & FusedGroupByAggregate Accumulator Types

## 1. PlanFragmenter COUNT(DISTINCT) Decomposition

**File**: `PlanFragmenter.java`

### Pure COUNT(DISTINCT) queries (all aggs are COUNT(DISTINCT)):
- **Method**: `extractCountDistinctColumns()` (line ~148)
- **Shard plan**: `AggregationNode(PARTIAL)` with `GROUP BY (original_keys + distinct_columns)` and `COUNT(*)`
- **Example**: `SELECT x, COUNT(DISTINCT y) FROM t GROUP BY x` →
  - Shard: `GROUP BY (x, y) COUNT(*) [PARTIAL]`
  - Coordinator: `AggregationNode(SINGLE)` with original `GROUP BY x, COUNT(DISTINCT y)`

### Mixed aggregates (COUNT(DISTINCT) + SUM/COUNT/AVG):
- **Method**: `buildMixedDedupShardPlan()` (line ~107)
- **Shard plan**: `GROUP BY (original_keys + distinct_cols)` with decomposed partial aggs
- **AVG decomposed**: `AVG(col)` → `SUM(col) + COUNT(col)` at shard level
- **COUNT(DISTINCT)**: omitted from shard aggs (handled by dedup GROUP BY)
- **Example**: `SELECT x, SUM(a), COUNT(DISTINCT y) FROM t GROUP BY x` →
  - Shard: `GROUP BY (x, y) SUM(a) [PARTIAL]`
  - Coordinator: `AggregationNode(SINGLE)` with original aggs

### Coordinator plan:
- **Method**: `buildCoordinatorPlan()` (line ~253)
- For PARTIAL shard plans → coordinator gets `AggregationNode(FINAL)`
- For SINGLE (COUNT DISTINCT) → coordinator gets `AggregationNode(SINGLE)`

## 2. Coordinator Merge Flow (TransportTrinoSqlAction.java)

The coordinator dispatches to different merge paths based on pattern matching:

### Decision tree (line ~230 onwards):

| Condition | Method | Description |
|-----------|--------|-------------|
| `FINAL + no GROUP BY` | `mergeScalarAggregation()` | Fast scalar merge |
| `SINGLE + scalar COUNT(DISTINCT long)` | `mergeCountDistinctValues()` / `mergeCountDistinctValuesViaRawSets()` | Union LongOpenHashSets |
| `SINGLE + scalar COUNT(DISTINCT varchar)` | `mergeCountDistinctVarcharValues()` / `mergeCountDistinctVarcharViaRawSets()` | Union SliceRangeHashSet/String sets |
| `SINGLE + isShardDedupCountDistinct()` | `mergeDedupCountDistinct()` | Two-stage: FINAL dedup → COUNT per group |
| `SINGLE + isShardMixedDedup()` | `mergeMixedDedup()` | Three-stage: FINAL dedup → re-aggregate |
| `SINGLE (fallback)` | `runCoordinatorAggregation()` | Full HashAggregationOperator on raw data |
| `FINAL + HAVING + SORT` | `merger.mergeAggregation()` + `applyCoordinatorHaving()` + `applyCoordinatorSort()` | Standard merge |

### Two-stage dedup merge (`mergeDedupCountDistinct`, line ~780):
1. **Stage 1**: FINAL merge of dedup keys `(x, y)` → removes cross-shard duplicates, sums COUNT(*)
2. **Stage 2**: GROUP BY original key `(x)`, count rows → produces COUNT(DISTINCT y)

### Optimized paths:
- **`mergeDedupCountDistinct2Key`** (line ~920): Ultra-fast 2-key fused path using flat `long[]` arrays
- **`mergeDedupCountDistinctViaSets`** (line ~960): Uses pre-built per-group `LongOpenHashSet` attachments from shard responses
- **`mergeDedupCountDistinctViaVarcharSets`** (line ~1050): VARCHAR-keyed variant using `Map<String, LongOpenHashSet>`
- **`mergeMixedDedupViaSets`** (line ~1200): Native mixed dedup using HashSet attachments (Q10 pattern)

### Local vs Remote paths:
- **Local path** (all shards on coordinator node): Can access `ShardExecuteResponse.getDistinctSets()` / `getScalarDistinctSet()` for zero-copy set merging
- **Remote path** (transport): Falls back to Page-based merge (sets are transient, not serialized)

## 3. FusedGroupByAggregate Accumulator Types

**File**: `FusedGroupByAggregate.java`

### accType encoding (used in switch-based dispatch):

| accType | Meaning | Accumulator Class | Storage |
|---------|---------|-------------------|---------|
| 0 | COUNT(*) or COUNT(col) | `CountStarAccum` | `long count` |
| 1 | SUM(long col) | `SumAccum` | `long longSum` |
| 2 | AVG(long col) | `AvgAccum` | `long longSum + long count` |
| 3 | MIN(any) | `MinAccum` | `long longVal` or `double doubleVal` or `Object objectVal` |
| 4 | MAX(any) | `MaxAccum` | `long longVal` or `double doubleVal` or `Object objectVal` |
| 5 | COUNT(DISTINCT col) | `CountDistinctAccum` | `LongOpenHashSet` (numeric) or `HashSet<Object>` (varchar/double) |
| 6 | SUM(double col) | `SumAccum` | `double doubleSum` |
| 7 | AVG(double col) | `AvgAccum` | `double doubleSum + long count` |

### Why accType=5 forces non-flat path:
- The flat accumulator paths (`FlatSingleKeyMap`, `FlatTwoKeyMap`, `FlatThreeKeyMap`) store all state in contiguous `long[]` arrays
- COUNT(DISTINCT) requires a `LongOpenHashSet` or `HashSet<Object>` per group — cannot be represented as fixed-count longs
- When any agg has `accType=5`, `canUseFlatAccumulators` is set to `false`, falling back to `SingleKeyHashMap`/`TwoKeyHashMap` with `AccumulatorGroup` objects

### Flat vs Object-based paths:

```
canUseFlatAccumulators = true  (COUNT/SUM long/AVG long only)
  → FlatSingleKeyMap / FlatTwoKeyMap / FlatThreeKeyMap
  → Zero per-group object allocation
  → accData[slot * slotsPerGroup + offset]

canUseFlatAccumulators = false  (MIN/MAX/COUNT DISTINCT/double args/varchar args)
  → SingleKeyHashMap / TwoKeyHashMap with AccumulatorGroup[]
  → Per-group: AccumulatorGroup + MergeableAccumulator[] + N accumulator objects
```

## 4. Fusion Opportunity Analysis

### Current architecture:
- Shards produce **deduped tuples** `(original_keys + distinct_cols, COUNT(*))` via `FusedGroupByAggregate`
- Coordinator runs **two-stage merge**: FINAL dedup → re-aggregate by original keys

### Potential optimization — fuse at shard level:
The shard already has all data needed to compute COUNT(DISTINCT) directly via `CountDistinctAccum` (accType=5). The dedup approach was chosen because:

1. **Data volume reduction**: Sending `(x, y, COUNT(*))` tuples is smaller than sending per-group HashSets
2. **Coordinator simplicity**: Two-stage merge uses existing FINAL merge infrastructure

### Where fusion already happens:
- **Local path with HashSet attachments**: When shards are local, `ShardExecuteResponse.getDistinctSets()` provides pre-built `LongOpenHashSet` per group. The coordinator unions these directly (`mergeDedupCountDistinctViaSets`), which IS effectively a fused path — the shard computed per-group distinct sets, and the coordinator just unions them.
- **Scalar COUNT(DISTINCT)**: `mergeCountDistinctValuesViaRawSets()` unions raw `LongOpenHashSet` from shards

### Remaining gap:
- **Remote transport path**: HashSets are transient (not serialized), so remote shards fall back to the two-stage Page-based merge. Serializing HashSets would enable the fused path for distributed clusters.
