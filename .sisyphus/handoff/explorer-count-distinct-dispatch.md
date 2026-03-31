# COUNT(DISTINCT) Dispatch Logic in TransportShardExecuteAction.java

## File Overview
- **File**: `TransportShardExecuteAction.java` (3759 lines)
- **Dispatch hub**: `executePlan()` method, lines ~200-780
- **Pattern**: Cascading `if` checks, first match wins, generic `LocalExecutionPlanner` pipeline as final fallback (line 771)

---

## 1. How COUNT(DISTINCT) Queries Are Dispatched

The dispatch in `executePlan()` follows this priority order for COUNT(DISTINCT):

### Path A: Bare TableScanNode (scalar COUNT(DISTINCT) — PlanFragmenter strips the AggNode)
- **Lines 265-283**: For scalar `COUNT(DISTINCT col)`, the `PlanFragmenter` strips the `AggregationNode` entirely, leaving a bare `TableScanNode` with a single column.
- **Numeric**: `isBareSingleNumericColumnScan(plan)` → `executeDistinctValuesScanWithRawSet()` (line 270)
  - Collects distinct values into a `LongOpenHashSet`, attaches raw set to response via `resp.setScalarDistinctSet(rawSet)`
- **VARCHAR**: `isBareSingleVarcharColumnScan(plan)` → `executeDistinctValuesScanVarcharWithRawSet()` (line 278)
  - Uses ordinal-based dedup via `FixedBitSet`, attaches raw string set to response

### Path B: AggregationNode with 2+ GROUP BY keys (Calcite's decomposed COUNT(DISTINCT))
- **Lines 292-397**: Detects `AggregationNode` with `Step.PARTIAL`, 2+ group-by keys, and `FusedGroupByAggregate.canFuse()`.
- **Condition**: `effectivePlan instanceof AggregationNode aggDedupNode && aggDedupNode.getStep() == PARTIAL && aggDedupNode.getGroupByKeys().size() >= 2 && FusedGroupByAggregate.canFuse(...)`
- Sub-dispatch based on key count and types:

| Keys | Types | Method | Line |
|------|-------|--------|------|
| 2 numeric | both non-VARCHAR | `executeCountDistinctWithHashSets()` | 328 |
| 2 (VARCHAR + numeric) | key0=VARCHAR, key1=numeric | `executeVarcharCountDistinctWithHashSets()` | 338 |
| 3+ all numeric | all non-VARCHAR | `executeNKeyCountDistinctWithHashSets()` | 355 |
| 3+ mixed types | last key numeric | `executeMixedTypeCountDistinctWithHashSets()` | 375 |
| 2 keys, mixed aggs | SUM/COUNT only | `executeMixedDedupWithHashSets()` | 393 |

### Path C: Generic Pipeline Fallback
- **Line 771**: `LocalExecutionPlanner` builds operator pipeline (`LucenePageSource → HashAggregationOperator`), much slower.

---

## 2. Two-Level Calcite Decomposition Detection

**Calcite decomposes** `COUNT(DISTINCT y)` into:
- **Outer**: `GROUP BY x, COUNT(*)`
- **Inner**: `GROUP BY x, y`

**The shard plan receives the inner `GROUP BY x, y` with `COUNT(*)`** as a `PARTIAL` `AggregationNode`.

**Detection logic (lines 292-301)**:
```java
if (scanFactory == null
    && effectivePlan instanceof AggregationNode aggDedupNode
    && aggDedupNode.getStep() == AggregationNode.Step.PARTIAL
    && aggDedupNode.getGroupByKeys().size() >= 2
    && FusedGroupByAggregate.canFuse(aggDedupNode, ...))
```

**Fusion**: YES — the code recognizes this two-level pattern and fuses it. Instead of materializing all `(x, y)` tuples, it builds per-group `LongOpenHashSet` accumulators during DocValues iteration. The shard outputs compact `(key0, placeholder=0, local_distinct_count)` rows + attached `HashSet` objects for the coordinator to union across shards.

**Key insight**: The "fusion" happens implicitly — the code doesn't explicitly detect "this is a decomposed COUNT(DISTINCT)". Instead, it matches the structural pattern (PARTIAL AggNode with 2+ keys and COUNT(*)) and applies the HashSet optimization, which effectively fuses the two-level decomposition into a single pass.

---

## 3. Exact Code Paths for Q04, Q08, Q13

### Q04: Global COUNT(DISTINCT UserID) — scalar, no GROUP BY
- **Calcite plan**: No outer GROUP BY, just `COUNT(DISTINCT UserID)`
- **PlanFragmenter**: Strips the AggregationNode, sends bare `TableScanNode(columns=[UserID])` to shards
- **Dispatch**: `isBareSingleNumericColumnScan(plan)` → TRUE (UserID is numeric)
- **Method**: `executeDistinctValuesScanWithRawSet()` (line 270)
- **Mechanism**: Collects all distinct UserID values into a `LongOpenHashSet` via `FusedScanAggregate.collectDistinctValuesRaw()`, attaches raw set to response
- **Coordinator**: Unions raw sets from all shards, returns `set.size()`

### Q08: GROUP BY RegionID + COUNT(DISTINCT UserID)
- **Calcite decomposition**: Inner `GROUP BY (RegionID, UserID)` with `COUNT(*)`
- **Shard plan**: `AggregationNode(step=PARTIAL, keys=[RegionID, UserID], aggs=[COUNT(*)])`
- **Dispatch**: 2 keys, both numeric, single COUNT(*) → `executeCountDistinctWithHashSets()` (line 328)
- **Mechanism**: Iterates DocValues for (RegionID, UserID), builds `Map<Long(RegionID), LongOpenHashSet(UserID)>` with parallel segment scanning
- **Output**: Compact page `(RegionID, 0, local_distinct_count)` + attached `distinctSets` map
- **Coordinator**: Unions per-RegionID HashSets across shards

### Q13: GROUP BY SearchPhrase + COUNT(DISTINCT UserID)
- **Calcite decomposition**: Inner `GROUP BY (SearchPhrase, UserID)` with `COUNT(*)`
- **Shard plan**: `AggregationNode(step=PARTIAL, keys=[SearchPhrase, UserID], aggs=[COUNT(*)])`
- **Dispatch**: 2 keys, key0=VARCHAR (SearchPhrase), key1=numeric (UserID) → `executeVarcharCountDistinctWithHashSets()` (line 338)
- **Mechanism**: Uses `SortedSetDocValues` for SearchPhrase ordinals + `SortedNumericDocValues` for UserID, builds `Map<String, LongOpenHashSet>` with global ordinals optimization for multi-segment
- **Output**: Compact page + attached varchar distinct sets
- **Coordinator**: Unions per-SearchPhrase HashSets across shards

---

## 4. executeCountDistinctWithHashSets — What It Does and Limitations

**Location**: Line 1001

**What it does**:
1. Acquires Lucene searcher for the shard
2. Compiles/caches Lucene query from DSL filter
3. **Parallel segment scanning**: Each segment builds its own `Map<Long(key0), LongOpenHashSet(key1)>` via `scanSegmentForCountDistinct()`
4. **Merge**: Unions per-segment maps (merges smaller set into larger for efficiency)
5. **Output**: Builds compact page with `(key0, placeholder=0, local_distinct_count)` + attaches raw `distinctSets` map to `ShardExecuteResponse`

**Key code (line 1118-1122)**:
```java
for (var entry : finalSets.entrySet()) {
  colTypes.get(0).writeLong(b0, entry.getKey());
  colTypes.get(1).writeLong(b1, 0L);  // placeholder for key1
  BigintType.BIGINT.writeLong(b2, entry.getValue().size());
}
resp.setDistinctSets(finalSets);
```

**Limitations**:
1. **Numeric keys only**: Both key0 and key1 must be non-VARCHAR. VARCHAR key0 routes to `executeVarcharCountDistinctWithHashSets()` instead.
2. **2-key only**: Only handles exactly 2 GROUP BY keys. 3+ keys route to `executeNKeyCountDistinctWithHashSets()`.
3. **COUNT(*) only**: Only handles single `COUNT(*)` aggregate. Mixed aggregates (SUM + COUNT) route to `executeMixedDedupWithHashSets()`.
4. **Memory**: Builds full `LongOpenHashSet` per group in memory — for high-cardinality key0 with high-cardinality key1, this can consume significant heap (e.g., 4.4M groups × large sets).
5. **No spill-to-disk**: All HashSets are in-memory; no overflow mechanism.
6. **Long-only values**: Uses `LongOpenHashSet` which stores `long` values — cannot handle floating-point distinct values without casting.
