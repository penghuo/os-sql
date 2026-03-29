# DQE Aggregation Code Structure Map

## Query Pipeline Flow
```
SQL → LogicalPlanner → PlanOptimizer → PlanFragmenter → [Shard: TransportShardExecuteAction → Fused/Operator execution] → [Coordinator: ResultMerger/TransportTrinoSqlAction merge]
```

## Key Files (by role)

### 1. Planning Layer
| File | Lines | Purpose |
|------|-------|---------|
| `dqe/.../planner/LogicalPlanner.java` | ~720 | SQL AST → DqePlanNode tree. `buildAggregation()` :456, `buildGlobalAggregation()` :417 |
| `dqe/.../planner/plan/AggregationNode.java` | ~65 | Plan node: groupByKeys, aggregateFunctions, Step (PARTIAL/FINAL/SINGLE) |
| `dqe/.../planner/optimizer/PlanOptimizer.java` | ~370 | Predicate pushdown, projection pruning, AggregationSplit visitor :366 |
| `dqe/.../coordinator/fragment/PlanFragmenter.java` | 560 | Splits plan into shard+coordinator fragments. COUNT(DISTINCT) decomposition here |

### 2. Shard Execution (Fused Fast Paths)
| File | Lines | Purpose |
|------|-------|---------|
| `dqe/.../shard/source/FusedScanAggregate.java` | 1829 | Fused scan for scalar aggregates (no GROUP BY). COUNT(DISTINCT) via LongOpenHashSet |
| `dqe/.../shard/source/FusedGroupByAggregate.java` | **12708** | Fused GROUP BY aggregation — the largest file. All fast paths live here |
| `dqe/.../shard/transport/TransportShardExecuteAction.java` | 2601 | Dispatch: checks canFuse() → routes to fused or operator-based execution |
| `dqe/.../shard/executor/LocalExecutionPlanner.java` | ~390 | Visitor: DqePlanNode → Operator pipeline (fallback when fused paths don't apply) |

### 3. Operator Layer (Generic Path)
| File | Lines | Purpose |
|------|-------|---------|
| `dqe/.../operator/HashAggregationOperator.java` | 1222 | Hash-based GROUP BY with accumulators. CountDistinctAccumulator :718 |
| `dqe/.../operator/Operator.java` | ~20 | Interface: `processNextBatch()` → Page, `close()` |
| `dqe/.../operator/LongOpenHashSet.java` | ~160 | Primitive long hash set for COUNT(DISTINCT) on numeric columns |
| `dqe/.../operator/SliceCountMap.java` | ~140 | Slice-based hash map for VARCHAR GROUP BY counting |
| `dqe/.../operator/SliceLongDedupMap.java` | ~? | Slice→Long dedup map for VARCHAR COUNT(DISTINCT) |

### 4. Coordinator Merge
| File | Lines | Purpose |
|------|-------|---------|
| `dqe/.../coordinator/merge/ResultMerger.java` | 3297 | Merges shard Pages. Fast numeric merge :356, varchar merge :1772 |
| `dqe/.../coordinator/transport/TransportTrinoSqlAction.java` | ~2600 | Coordinator entry point. COUNT(DISTINCT) merge dispatch :342-657 |

## COUNT(DISTINCT) Execution Paths

### Path 1: Scalar COUNT(DISTINCT) on numeric column
- **PlanFragmenter** :144-151 — detects SINGLE COUNT(DISTINCT), extracts distinct columns
- **TransportShardExecuteAction** :252 — `executeDistinctValuesScanWithRawSet()` :1867
- **FusedScanAggregate** :1403-1561 — `executeDistinctValues()` collects into LongOpenHashSet per shard
- **TransportTrinoSqlAction** :606 — `mergeCountDistinctValuesViaRawSets()` merges LongOpenHashSets

### Path 2: Scalar COUNT(DISTINCT) on VARCHAR column
- **FusedScanAggregate** :1565-1624 — `executeDistinctStringsRaw()` collects HashSet<String>
- **TransportTrinoSqlAction** :610 — `mergeCountDistinctVarcharViaRawSets()`

### Path 3: GROUP BY + COUNT(DISTINCT) only (shard dedup)
- **PlanFragmenter** :144-170 — builds shard plan with GROUP BY (keys + distinct_cols)
- **TransportTrinoSqlAction** :613-657 — `mergeDedupCountDistinct()` / `mergeDedupCountDistinctViaSets()`

### Path 4: Mixed aggregates (COUNT(DISTINCT) + SUM/COUNT/etc.)
- **PlanFragmenter** :185-253 — `buildMixedDedupShardPlan()`: GROUP BY (keys + distinct_cols) with partial aggs
- **TransportTrinoSqlAction** :371 — coordinator reassembles COUNT(DISTINCT) from deduped groups

## FusedGroupByAggregate Key Methods (12708 lines)

| Method | Line | Description |
|--------|------|-------------|
| `canFuse()` | 196 | Eligibility check for fused GROUP BY |
| `execute()` | 930 | Public entry point |
| `executeInternal()` | 937 | Dispatches to specialized paths |
| `executeNumericOnly()` | 2811 | All-numeric keys path |
| `executeSingleKeyNumeric()` | 3171 | Single numeric key, dispatches to flat if eligible |
| `executeSingleKeyNumericFlat()` | 4350 | **Flat-array fast path**: no hash table, direct array indexing |
| `executeTwoKeyNumeric()` | 4880 | Two numeric keys |
| `executeTwoKeyNumericFlat()` | 5244 | Two-key flat-array path |
| `executeThreeKeyFlat()` | 8401 | Three-key flat-array path |
| `executeSingleVarcharCountStar()` | 1273 | Optimized VARCHAR GROUP BY + COUNT(*) |
| `executeSingleVarcharGeneric()` | 1946 | Generic VARCHAR GROUP BY |
| `executeWithExpressionKey()` | 391 | Expression-based keys (DATE_TRUNC, REGEXP_REPLACE) |
| `executeWithTopN()` | 906 | TopN-aware aggregation |
| `executeWithVarcharKeys()` | 6804 | VARCHAR key paths |
| `executeNKeyVarcharPath()` | 8880 | N-key VARCHAR path |
| `mergePartitionedPages()` | 8216 | Merges hash-partitioned results |

## HashAggregationOperator Key Methods

| Method | Line | Description |
|--------|------|-------------|
| `processNextBatch()` | 103 | Main entry: scalar vs grouped dispatch |
| `processScalarAggregation()` | 122 | No GROUP BY — single-row result |
| `processGroupedAggregation()` | 158 | Dispatches by key type |
| `processSingleLongKeyAggregation()` | 191 | Optimized single-long-key with LongKeyHashMap |
| `processSingleVarcharKeyAggregation()` | 270 | Single VARCHAR key |
| `processGenericGroupedAggregation()` | 455 | Generic multi-key with GroupKey |
| `CountDistinctAccumulator` | 718 | Uses LongOpenHashSet (numeric) or HashSet<Object> (other) |
| `countDistinct()` factory | 1089 | Creates CountDistinctAccumulator |

## Parallelism Infrastructure
- **FusedGroupByAggregate** :116-133 — `PARALLELISM_MODE` system property, `THREADS_PER_SHARD`, shared `ForkJoinPool`
- Parallel execution in VARCHAR distinct paths :1634-1746 using CompletableFuture
- Parallel segment scanning in varchar paths :2528+

## Pattern Compilation (REGEXP_REPLACE)
- **FusedGroupByAggregate** :81-111 — 4 static compiled Patterns: AGG_FUNCTION, DATE_TRUNC_PATTERN, ARITH_EXPR_PATTERN, EXTRACT_PATTERN
- **FusedScanAggregate** :60-69 — AGG_FUNCTION, COL_PLUS_CONST patterns
- `executeWithExpressionKey()` :391 handles REGEXP_REPLACE GROUP BY keys :487
