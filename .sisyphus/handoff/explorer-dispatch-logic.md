# COUNT(DISTINCT) Fast-Path Dispatch Logic Analysis

## File: `dqe/src/main/java/org/opensearch/sql/dqe/shard/transport/TransportShardExecuteAction.java`

---

## 1. Dispatch Flow Overview (lines ~235–360)

The dispatch logic is a sequential chain of `if` checks. The first matching condition wins. The relevant COUNT(DISTINCT) fast paths are checked in this order:

1. **Fused scalar aggregation** (line 236): `FusedScanAggregate.canFuse(aggNode)` — handles scalar aggs (no GROUP BY) like SUM, AVG, MIN, MAX, COUNT(*). Does NOT handle COUNT(DISTINCT) because `canFuse` accepts any `AGG_FUNCTION` pattern but the fused execution path doesn't have special DISTINCT logic.

2. **Bare numeric column scan** (line 251): `isBareSingleNumericColumnScan(plan)` — catches scalar COUNT(DISTINCT numericCol) after PlanFragmenter strips the AggregationNode.

3. **Bare VARCHAR column scan** (line 258): `isBareSingleVarcharColumnScan(plan)` — catches scalar COUNT(DISTINCT varcharCol) similarly.

4. **Fused eval-aggregate** (line 264): `FusedScanAggregate.canFuseWithEval(aggNode)` — SUM(col + constant) patterns, not CD-related.

5. **COUNT(DISTINCT) dedup with 2 numeric keys** (line 275): Checks `AggregationNode` with `Step.PARTIAL`, exactly 2 group-by keys, and `FusedGroupByAggregate.canFuse()`. Sub-dispatches to:
   - **Pure COUNT(*)-only dedup** → `executeCountDistinctWithHashSets` (both keys numeric)
   - **VARCHAR key0 + numeric key1** → `executeVarcharCountDistinctWithHashSets`
   - **Mixed dedup (SUM/COUNT + CD)** → `executeMixedDedupWithHashSets` (both keys numeric)

6. **Ordinal-cached expression GROUP BY** (line 340): Expression-key GROUP BY, not CD-specific.

7. **Generic fused GROUP BY** (line 351): `FusedGroupByAggregate.canFuse()` — handles GROUP BY with COUNT(DISTINCT) via generic `CountDistinctAccum` accumulators.

---

## 2. How PlanFragmenter Transforms COUNT(DISTINCT) Queries

The PlanOptimizer marks COUNT(DISTINCT) queries as `Step.SINGLE` (non-decomposable). The PlanFragmenter then transforms them:

| Query Shape | PlanFragmenter Output (Shard Plan) |
|---|---|
| Scalar CD (no GROUP BY) | Strips AggregationNode → bare `TableScanNode` with 1 column |
| GROUP BY + CD only | `AggregationNode(PARTIAL)` with keys = [original_keys + distinct_col], aggs = [COUNT(*)] |
| GROUP BY + CD + other aggs | `AggregationNode(PARTIAL)` with keys = [original_keys + distinct_col], aggs = [decomposed partial aggs] |

Key insight: After PlanFragmenter, the shard plan for GROUP BY + CD queries becomes a PARTIAL aggregation with **N+1 group-by keys** (original keys + distinct column).

---

## 3. All Fast-Path Patterns Detected

### Fast Path A: Scalar COUNT(DISTINCT) on Numeric Column
- **Condition**: `plan instanceof TableScanNode` with exactly 1 column of type BigintType, IntegerType, SmallintType, TinyintType, or TimestampType
- **Method**: `executeDistinctValuesScanWithRawSet` (line 1867)
- **Mechanism**: Collects distinct values into `LongOpenHashSet`, attaches raw set to response
- **Detected at**: line 251

### Fast Path B: Scalar COUNT(DISTINCT) on VARCHAR Column
- **Condition**: `plan instanceof TableScanNode` with exactly 1 column of type VarcharType
- **Method**: `executeDistinctValuesScanVarcharWithRawSet` (line 1897)
- **Mechanism**: Ordinal-based dedup via FixedBitSet, attaches raw string set to response
- **Detected at**: line 258

### Fast Path C: GROUP BY (numericKey0, numericKey1) + COUNT(*)
- **Conditions** (ALL must be true):
  - `effectivePlan instanceof AggregationNode`
  - `step == PARTIAL`
  - `groupByKeys.size() == 2`
  - `FusedGroupByAggregate.canFuse()` returns true
  - Exactly 1 aggregate function == `"COUNT(*)"`
  - Both keys are non-VARCHAR types
- **Method**: `executeCountDistinctWithHashSets` (line 936)
- **Mechanism**: GROUP BY key0 with per-group LongOpenHashSet for key1
- **Detected at**: line 275

### Fast Path D: GROUP BY (varcharKey0, numericKey1) + COUNT(*)
- **Conditions**: Same as C, but key0 is VarcharType and key1 is non-VARCHAR
- **Method**: `executeVarcharCountDistinctWithHashSets` (line 1485)
- **Mechanism**: Per-VARCHAR-group LongOpenHashSet for numeric key1
- **Detected at**: line 316

### Fast Path E: GROUP BY (numericKey0, numericKey1) + Mixed SUM/COUNT Aggs
- **Conditions**:
  - Same structural checks as C
  - More than 1 aggregate, NOT all COUNT(*)
  - All aggregates match `(sum|count)\(.*\)` regex (no DISTINCT, no AVG)
  - Both keys are non-VARCHAR types
- **Method**: `executeMixedDedupWithHashSets` (line 1224)
- **Mechanism**: GROUP BY key0 with per-group HashSet for key1 + SUM/COUNT accumulators
- **Detected at**: line 296

### Fallback: Generic Fused GROUP BY
- **Condition**: `FusedGroupByAggregate.canFuse()` — handles any GROUP BY with supported types/aggs
- **Method**: `executeFusedGroupByAggregate` (line 351)
- **Mechanism**: Generic ordinal-based GROUP BY with `CountDistinctAccum` for CD columns
- **Note**: This IS reached for CD queries that don't match fast paths C/D/E

---

## 4. Query Shapes That Do NOT Match Fast Paths (and Why)

### Q04: `SELECT COUNT(DISTINCT UserID) FROM hits` — Scalar BIGINT
- **PlanFragmenter output**: Bare `TableScanNode` with 1 column (UserID, BigintType)
- **Should match**: Fast Path A (`isBareSingleNumericColumnScan`)
- **Potential issue**: Only fails if `scanFactory != null` or if the plan isn't a bare TableScanNode (e.g., FilterNode wrapping it). Verify the actual shard plan structure.

### Q05: `SELECT COUNT(DISTINCT SearchPhrase) FROM hits` — Scalar VARCHAR
- **PlanFragmenter output**: Bare `TableScanNode` with 1 column (SearchPhrase, VarcharType)
- **Should match**: Fast Path B (`isBareSingleVarcharColumnScan`)
- **Same potential issue as Q04**.

### Q08: `GROUP BY RegionID + COUNT(DISTINCT UserID)` — Single key + CD
- **PlanFragmenter output**: `AggregationNode(PARTIAL, keys=[RegionID, UserID], aggs=[COUNT(*)])`
- **Dispatch check**: `groupByKeys.size() == 2` ✅, `canFuse()` ✅, single COUNT(*) ✅
- **Should match**: Fast Path C (both RegionID and UserID are numeric)
- **If not matching**: Check if `canFuse()` returns false for this specific type combination.

### Q09: `GROUP BY RegionID + mixed aggs + COUNT(DISTINCT UserID)`
- **PlanFragmenter output**: `AggregationNode(PARTIAL, keys=[RegionID, UserID], aggs=[SUM(...), COUNT(*), ...])`
- **Dispatch check**: `groupByKeys.size() == 2` ✅, `isMixedDedup` check ✅
- **Should match**: Fast Path E
- **Potential issue**: The `isMixedDedup` regex `(?i)^(sum|count)\(.*\)$` — if any aggregate is AVG, it won't match. Check if PlanFragmenter decomposes AVG into SUM+COUNT for mixed dedup plans.

### Q11: `GROUP BY MobilePhone, MobilePhoneModel + COUNT(DISTINCT UserID) + WHERE`
- **PlanFragmenter output**: `AggregationNode(PARTIAL, keys=[MobilePhone, MobilePhoneModel, UserID], aggs=[COUNT(*)])`
- **Dispatch check**: `groupByKeys.size() == 3` ❌ — **fails the `size() == 2` check**
- **Root cause**: The fast path only handles exactly 2 group-by keys (1 original + 1 distinct). With 2 original keys + 1 distinct column = 3 keys → falls through to generic fused GROUP BY.

### Q13: `GROUP BY SearchPhrase + COUNT(DISTINCT UserID) + WHERE`
- **PlanFragmenter output**: `AggregationNode(PARTIAL, keys=[SearchPhrase, UserID], aggs=[COUNT(*)])`
- **Dispatch check**: `groupByKeys.size() == 2` ✅, `canFuse()` ✅, single COUNT(*) ✅
- **Type check**: SearchPhrase is VARCHAR (key0), UserID is numeric (key1)
- **Should match**: Fast Path D (`executeVarcharCountDistinctWithHashSets`)
- **If not matching**: Check if WHERE clause creates a FilterNode that prevents `findChildTableScan` from finding the TableScanNode in `canFuse()`.

---

## 5. Key Constraints Summary

| Constraint | Value | Impact |
|---|---|---|
| Group-by key count | Exactly 2 | Queries with 0, 1, or 3+ keys skip all HashSet fast paths |
| Key types for Path C/E | Both non-VARCHAR | VARCHAR+VARCHAR or VARCHAR+numeric key1 only handled by Path D |
| Mixed agg regex | `(sum\|count)\(.*\)` | AVG aggregates disqualify mixed dedup fast path |
| `canFuse()` requirement | Child must be TableScanNode (or EvalNode→TableScanNode) | FilterNode between Agg and Scan may block detection |
| Scalar CD detection | Bare TableScanNode with 1 column | Only works after PlanFragmenter strips AggregationNode |

---

## 6. No TODO/FIXME Comments Found

No TODO, FIXME, or HACK comments exist in the dispatch logic area (lines 230-360) or the helper methods.
