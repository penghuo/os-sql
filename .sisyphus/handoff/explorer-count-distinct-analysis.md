# COUNT(DISTINCT) Fast-Path Dispatch Analysis

## Dispatch Order (TransportShardExecuteAction.java:210-380)

1. **Scalar COUNT(*)** (L213) — `isScalarCountStar(effectivePlan)`
2. **Sorted scan** (L225) — `extractSortedScanSpec(plan)`
3. **FusedScanAggregate** (L236) — scalar agg, no GROUP BY, `canFuse(aggNode)`
4. **Bare numeric scan** (L251) — `isBareSingleNumericColumnScan(plan)` → `executeDistinctValuesScanWithRawSet`
5. **Bare varchar scan** (L259) — `isBareSingleVarcharColumnScan(plan)` → `executeDistinctValuesScanVarcharWithRawSet`
6. **Fused eval-aggregate** (L265) — `canFuseWithEval(aggEvalNode)`
7. **2-key COUNT(DISTINCT) HashSet** (L272) — `groupByKeys.size()==2` + `canFuse()` + `isSingleCountStar` or `isMixedDedup`
8. **Expression GROUP BY** (L340) — `canFuseWithExpressionKey()`
9. **Generic fused GROUP BY** (L353) — `FusedGroupByAggregate.canFuse()`
10. **Fused GROUP BY + sort+limit** (L365)
11. **Fallback generic pipeline**

## canFuse() Gate Conditions

### FusedScanAggregate.canFuse() (L78)
- `groupByKeys` must be empty (scalar agg only)
- Child must be `TableScanNode`
- All agg functions match `(COUNT|SUM|MIN|MAX|AVG)((DISTINCT )?(.+?))` — **accepts DISTINCT**
- **Does NOT reject COUNT(DISTINCT)** — but PlanOptimizer forces SINGLE step, so PlanFragmenter strips the AggregationNode before it reaches this check

### FusedGroupByAggregate.canFuse() (L197)
- `groupByKeys` must be non-empty
- Must find a `TableScanNode` descendant
- All keys must be VARCHAR, numeric, timestamp, or supported expression
- All agg functions match same pattern — **accepts DISTINCT keyword in regex but doesn't special-case it**
- Agg argument must be `*`, a physical column, or `length(varchar_col)`

## PlanOptimizer Step Assignment (L370-413)

**Critical**: `hasNonDecomposableAgg()` returns `true` whenever ANY agg contains `COUNT(DISTINCT` → forces `Step.SINGLE` for ALL COUNT(DISTINCT) queries regardless of GROUP BY.

## PlanFragmenter Shard Plan (L110-168)

For **SINGLE + no GROUP BY**: strips AggregationNode → bare `TableScanNode`
For **SINGLE + GROUP BY + all COUNT(DISTINCT)**: `extractCountDistinctColumns()` → dedup plan with `(originalKeys + distinctCols)` as keys + `COUNT(*)` agg
For **SINGLE + GROUP BY + mixed aggs**: `buildMixedDedupShardPlan()` → dedup plan with expanded keys + decomposed aggs (AVG→SUM+COUNT)

## Per-Query Analysis

### Q04: `SELECT COUNT(DISTINCT UserID) FROM hits` — scalar CD on BIGINT
- **Step**: SINGLE (hasCountDistinct=true)
- **Shard plan**: bare `TableScanNode(columns=[UserID])` (AggNode stripped)
- **Dispatch**: `isBareSingleNumericColumnScan(plan)` → UserID is BIGINT → **TRUE**
- **Verdict**: ✅ HITS fast path #4 (`executeDistinctValuesScanWithRawSet`)
- **If not hitting**: Check if a `ProjectNode` wraps the shard plan (bare scan checks use `plan` not `effectivePlan`)

### Q05: `SELECT COUNT(DISTINCT SearchPhrase) FROM hits` — scalar CD on VARCHAR
- **Step**: SINGLE
- **Shard plan**: bare `TableScanNode(columns=[SearchPhrase])`
- **Dispatch**: `isBareSingleVarcharColumnScan(plan)` → VARCHAR → **TRUE**
- **Verdict**: ✅ HITS fast path #5 (`executeDistinctValuesScanVarcharWithRawSet`)
- **If not hitting**: Same ProjectNode concern as Q04

### Q08: `GROUP BY RegionID + COUNT(DISTINCT UserID)` — 1 key + CD
- **Step**: SINGLE
- **Shard plan**: `AggregationNode(keys=[RegionID, UserID], aggs=[COUNT(*)], step=PARTIAL)`
- **Dispatch**: `groupByKeys.size()==2` ✓, `canFuse()` ✓, `isSingleCountStar` ✓, both numeric ✓
- **Verdict**: ✅ HITS fast path #7 (`executeCountDistinctWithHashSets`)
- **If not hitting**: Verify RegionID resolves to numeric type in columnTypeMap

### Q09: `GROUP BY RegionID + SUM, COUNT(*), AVG, COUNT(DISTINCT UserID)` — mixed aggs + CD
- **Step**: SINGLE
- **Shard plan via buildMixedDedupShardPlan**: `AggregationNode(keys=[RegionID, UserID], aggs=[SUM(AdvEngineID), COUNT(*), SUM(ResolutionWidth), COUNT(ResolutionWidth)], step=PARTIAL)`
- **Dispatch**: `groupByKeys.size()==2` ✓, `canFuse()` ✓, `isSingleCountStar` ✗ (4 aggs), `isMixedDedup` regex `(sum|count)\(.*\)` ✓ for all
- **Verdict**: ✅ HITS fast path #7 (`executeMixedDedupWithHashSets`)
- **If not hitting**: Check if AVG decomposition produces unexpected agg names

### Q11: `GROUP BY MobilePhone, MobilePhoneModel + COUNT(DISTINCT UserID) + WHERE` — 2 keys + CD + filter
- **Step**: SINGLE
- **Shard plan**: `AggregationNode(keys=[MobilePhone, MobilePhoneModel, UserID], aggs=[COUNT(*)], step=PARTIAL)`
- **Dispatch**: `groupByKeys.size()==2` → **FALSE (has 3 keys!)**
- **Falls through to**: generic `FusedGroupByAggregate.canFuse()` at step #9 (if types supported) or fallback pipeline
- **Verdict**: ❌ MISSES HashSet fast path — PlanFragmenter adds distinct column as 3rd key
- **Root cause**: The 2-key HashSet path (L280) hardcodes `size()==2`. Original query has 2 GROUP BY keys, but PlanFragmenter adds UserID as 3rd dedup key.

## Suggested Code Changes

### 1. Generalize the 2-key HashSet path to N-key (fixes Q11)
**File**: `TransportShardExecuteAction.java:280`
```java
// BEFORE: hardcoded 2-key check
&& aggDedupNode.getGroupByKeys().size() == 2
// AFTER: support 2+ keys (last key is always the distinct column)
&& aggDedupNode.getGroupByKeys().size() >= 2
```
Then generalize `executeCountDistinctWithHashSets` to use a composite key (e.g., `long[]` or concatenated hash) for N-1 group-by keys with a per-group `LongOpenHashSet` for the Nth (distinct) key.

### 2. Add VARCHAR key support for mixed dedup (currently numeric-only)
**File**: `TransportShardExecuteAction.java:325-335`
The `isMixedDedup` path rejects VARCHAR keys (`!(t0 instanceof VarcharType)`). Add a VARCHAR variant similar to `executeVarcharCountDistinctWithHashSets`.

### 3. Verify Q04/Q05 aren't wrapped by ProjectNode
If a single-shard index produces `ProjectNode -> TableScanNode` as the shard plan, the bare scan checks (L251, L259) fail because they check `plan` not `effectivePlan`. The `effectivePlan` unwrap only handles `ProjectNode -> AggregationNode`. Add:
```java
// After effectivePlan unwrap (L207), also unwrap for bare scans:
DqePlanNode bareScanPlan = (plan instanceof ProjectNode proj 
    && proj.getChild() instanceof TableScanNode) ? proj.getChild() : plan;
```

## Key Code References
| Symbol | File | Line |
|--------|------|------|
| executePlan dispatch | TransportShardExecuteAction.java | 210-380 |
| isBareSingleNumericColumnScan | TransportShardExecuteAction.java | 1781 |
| isBareSingleVarcharColumnScan | TransportShardExecuteAction.java | 1803 |
| 2-key HashSet gate | TransportShardExecuteAction.java | 280 |
| FusedScanAggregate.canFuse | FusedScanAggregate.java | 78 |
| FusedGroupByAggregate.canFuse | FusedGroupByAggregate.java | 197 |
| hasNonDecomposableAgg | PlanOptimizer.java | 395 |
| extractCountDistinctColumns | PlanFragmenter.java | 256 |
| buildMixedDedupShardPlan | PlanFragmenter.java | 185 |
