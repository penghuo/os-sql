# Shard Dispatch Logic for COUNT(DISTINCT)

## Source Files
- `dqe/src/main/java/org/opensearch/sql/dqe/shard/transport/TransportShardExecuteAction.java`
- `dqe/src/main/java/org/opensearch/sql/dqe/shard/source/FusedScanAggregate.java`
- `dqe/src/main/java/org/opensearch/sql/dqe/coordinator/fragment/PlanFragmenter.java`

---

## Dispatch Decision Tree (TransportShardExecuteAction.executePlan)

The dispatch is a sequential if-else chain in `executePlan()` (line ~195). First match wins.

```
1. ProjectNode unwrap (line 203-210)
   └─ If plan is ProjectNode → AggregationNode, unwrap to effectivePlan

2. isScalarCountStar (line 215)
   └─ COUNT(*) without GROUP BY → executeScalarCountStar

3. extractSortedScanSpec (line 225)
   └─ LimitNode → SortNode → TableScanNode → executeSortedScan

4. FusedScanAggregate.canFuse (line 238)
   └─ AggregationNode with NO GROUP BY, child is TableScanNode
   └─ ALL agg functions match AGG_FUNCTION regex (COUNT/SUM/MIN/MAX/AVG with optional DISTINCT)
   └─ → executeFusedScanAggregate → FusedScanAggregate.execute()
   └─ **This handles scalar COUNT(DISTINCT) when AggregationNode is present (PARTIAL step)**

5. isBareSingleNumericColumnScan (line 251) ← Q04 PATH
   └─ Bare TableScanNode with exactly 1 numeric column
   └─ Result of PlanFragmenter stripping SINGLE AggregationNode
   └─ → executeDistinctValuesScanWithRawSet
   └─ Collects into LongOpenHashSet, attaches raw set to response

6. isBareSingleVarcharColumnScan (line 259) ← Q05 PATH
   └─ Bare TableScanNode with exactly 1 VARCHAR column
   └─ → executeDistinctValuesScanVarcharWithRawSet
   └─ Uses ordinal-based FixedBitSet dedup, attaches raw string set

7. FusedScanAggregate.canFuseWithEval (line 267)
   └─ SUM(col + constant) patterns → executeFusedEvalAggregate

8. AggDedupNode detection (line 280-335) ← GROUP BY COUNT(DISTINCT) PATHS
   └─ Conditions: AggregationNode, PARTIAL step, exactly 2 GROUP BY keys, canFuse
   │
   ├─ isSingleCountStar (line 285): only 1 agg = "COUNT(*)"
   │   ├─ Both keys numeric → executeCountDistinctWithHashSets (Q08/Q09 dedup)
   │   └─ key0=VARCHAR, key1=numeric → executeVarcharCountDistinctWithHashSets (Q14)
   │
   └─ isMixedDedup (line 318): all aggs match (sum|count)\(.*\)
       └─ Both keys numeric → executeMixedDedupWithHashSets (Q10)

9. FusedGroupByAggregate paths (line 345+)
   └─ Expression GROUP BY, ordinal-based GROUP BY, sort+limit variants
```

---

## Method Signatures & Key Logic

### executeCountDistinctWithHashSets (line 936)
```java
private ShardExecuteResponse executeCountDistinctWithHashSets(
    AggregationNode aggNode, ShardExecuteRequest req,
    String keyName0, String keyName1, Type type0, Type type1)
```
- **Pattern**: GROUP BY (key0, key1) with single COUNT(*), both keys numeric
- **Logic**: GROUP BY key0 only, per-group `LongOpenHashSet` for key1 values
- **Parallelism**: Multi-segment parallel scan via ForkJoinPool (single segment = direct scan)
- **Output**: Compact page (~450 rows) + attached HashSets for coordinator union

### executeMixedDedupWithHashSets (line 1224)
```java
private ShardExecuteResponse executeMixedDedupWithHashSets(
    AggregationNode aggNode, ShardExecuteRequest req,
    String keyName0, String keyName1, Type type0, Type type1)
```
- **Pattern**: GROUP BY (key0, key1) with mixed SUM/COUNT aggregates, both keys numeric
- **Logic**: GROUP BY key0 only, per-group HashSet for key1 + long[] accumulators for SUM/COUNT
- **Uses**: Open-addressing hash map with linear probing (grpKeys[], grpSets[], grpAccs[], grpOcc[])
- **Output**: Reduces ~25K (key0×key1) rows to ~400 (key0) rows

### executeVarcharCountDistinctWithHashSets (line 1485)
```java
private ShardExecuteResponse executeVarcharCountDistinctWithHashSets(
    AggregationNode aggNode, ShardExecuteRequest req,
    String varcharKeyName, String numericKeyName, Type numericKeyType)
```
- **Pattern**: VARCHAR key0 + numeric key1, single COUNT(*)
- **Logic**: Per-group LongOpenHashSets indexed by ordinal (no hash for VARCHAR keys)
- **Optimization**: Collect-then-sequential-scan for WHERE-filtered queries

### executeDistinctValuesScanWithRawSet (line 1867) ← Q04
```java
private ShardExecuteResponse executeDistinctValuesScanWithRawSet(
    DqePlanNode plan, ShardExecuteRequest req)
```
- **Pattern**: Bare TableScanNode with 1 numeric column (from SINGLE COUNT(DISTINCT) stripping)
- **Logic**: Calls `FusedScanAggregate.collectDistinctValuesRaw()` → sequential DocValues scan into LongOpenHashSet
- **Output**: 1-row Page with count + `resp.setScalarDistinctSet(rawSet)` attachment

### executeDistinctValuesScanVarcharWithRawSet (line 1900) ← Q05
```java
private ShardExecuteResponse executeDistinctValuesScanVarcharWithRawSet(
    DqePlanNode plan, ShardExecuteRequest req)
```
- **Pattern**: Bare TableScanNode with 1 VARCHAR column
- **Logic**: Calls `FusedScanAggregate.collectDistinctStringsRaw()` → ordinal-based FixedBitSet dedup
- **Output**: 1-row Page with count + `resp.setScalarDistinctStrings(rawStrings)` attachment

---

## FusedScanAggregate Scalar COUNT(DISTINCT) Path

### FusedScanAggregate.execute() (line 472)
- Entry: `AggregationNode` with no GROUP BY, child = `TableScanNode`
- Parses agg functions via `AGG_FUNCTION` regex, creates `DirectAccumulator` per function
- **CountDistinctDirectAccumulator** (line 1766): used when `spec.isDistinct && funcName == "COUNT"`
  - Numeric non-double: `LongOpenHashSet` (primitive, no boxing)
  - VARCHAR: `HashSet<Object>` with `stringDv.lookupOrd().utf8ToString()`
  - Double: `HashSet<Object>` with `Double.longBitsToDouble()`
  - `writeTo()`: outputs `longDistinctValues.size()` or `objectDistinctValues.size()`

### Execution paths within FusedScanAggregate.execute():
1. **COUNT(*) only + MatchAll** → O(1) via `indexReader.numDocs()` (line 507)
2. **MIN/MAX only + MatchAll + no deletes** → O(1) via PointValues (line 536)
3. **tryFlatArrayPath** → rejects DISTINCT (line 772: `if (spec.isDistinct()) return false`)
4. **MatchAll general** → tight per-doc loop calling `acc.accumulate(doc)` (line 628)
5. **Filtered COUNT(*) only** → `engineSearcher.count(query)` (line 656)
6. **General filtered** → Lucene Collector framework calling `acc.accumulate(doc)` (line 672)

**For scalar COUNT(DISTINCT), paths 4 or 6 are used** — CountDistinctDirectAccumulator.accumulate() is called per doc.

---

## Q04/Q05 Actual Path (Scalar COUNT(DISTINCT))

### How PlanFragmenter routes Q04/Q05:
1. `SELECT COUNT(DISTINCT UserID) FROM hits` → AggregationNode(SINGLE, groupBy=[], aggs=["COUNT(DISTINCT UserID)"])
2. PlanFragmenter.buildShardPlan: SINGLE step, no GROUP BY → falls to final line: `return aggNode.getChild()` (line 168)
3. Shard receives bare `TableScanNode` with columns=["UserID"]

### How TransportShardExecuteAction routes Q04/Q05:
- Q04 (numeric UserID): `isBareSingleNumericColumnScan` → `executeDistinctValuesScanWithRawSet` → `FusedScanAggregate.collectDistinctValuesRaw`
- Q05 (varchar SearchPhrase): `isBareSingleVarcharColumnScan` → `executeDistinctValuesScanVarcharWithRawSet` → `FusedScanAggregate.collectDistinctStringsRaw`

### Key: Q04/Q05 do NOT go through FusedScanAggregate.execute()
The `canFuse` check at line 238 never fires because the PlanFragmenter strips the AggregationNode for SINGLE step. The shard plan is a bare TableScanNode, not an AggregationNode.

---

## What's Missing for Q04/Q05

**Nothing is missing** — Q04 and Q05 already have dedicated fast paths:
- Q04 → `executeDistinctValuesScanWithRawSet` with `LongOpenHashSet` + raw set attachment
- Q05 → `executeDistinctValuesScanVarcharWithRawSet` with ordinal-based `FixedBitSet` dedup + raw string set attachment

The `FusedScanAggregate.execute()` path with `CountDistinctDirectAccumulator` is a **separate, alternative path** that would be used if the AggregationNode were preserved (PARTIAL step). Currently it's not used for Q04/Q05 because:
1. PlanFragmenter strips the AggregationNode for SINGLE step (no GROUP BY)
2. The bare scan + raw set attachment approach is more efficient (avoids Page construction for millions of entries)

### Potential improvement areas:
- **CountDistinctDirectAccumulator** uses `advanceExact(doc)` per doc — the raw set path (`collectDistinctValuesRaw`) uses the same approach but is sequential. Neither uses parallel segment scanning for scalar COUNT(DISTINCT).
- **Q05 varchar path** materializes all distinct strings into a `HashSet<String>` — for very high cardinality this could be memory-intensive. The ordinal-based approach in `collectDistinctStringsRaw` mitigates this with `FixedBitSet` per segment.
