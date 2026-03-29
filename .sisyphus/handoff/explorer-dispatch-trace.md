# COUNT(DISTINCT) Dispatch Trace — TransportShardExecuteAction.java

## Key Insight: The 3-Stage Pipeline

### Stage 1: PlanOptimizer (optimizer/PlanOptimizer.java:370-406)
- **Any query with COUNT(DISTINCT)** → `hasNonDecomposableAgg()` returns `true` → Step set to **SINGLE**
- This means the AggregationNode arrives at PlanFragmenter with `Step.SINGLE`

### Stage 2: PlanFragmenter (fragment/PlanFragmenter.java:108-170)
For `Step.SINGLE` with GROUP BY + COUNT(DISTINCT)-only aggregates:
- `extractCountDistinctColumns()` extracts distinct columns
- Builds **dedup shard plan**: `AggregationNode(PARTIAL, groupByKeys=[original_keys + distinct_cols], aggs=[COUNT(*)])`
- For mixed (COUNT(DISTINCT) + SUM/COUNT/AVG): `buildMixedDedupShardPlan()` does similar

For `Step.SINGLE` **without GROUP BY** (scalar):
- Falls to line 170: `return aggNode.getChild()` → **bare TableScanNode** sent to shard

### Stage 3: TransportShardExecuteAction.executePlan() Dispatch (lines 194-720)

## Per-Query Dispatch Trace

### Q04: `SELECT COUNT(DISTINCT UserID) FROM hits` — scalar, no GROUP BY
- **Optimizer**: SINGLE (has COUNT(DISTINCT))
- **Fragmenter**: No GROUP BY → strips agg → bare `TableScanNode(columns=[UserID])`
- **Dispatch**: Hits `isBareSingleNumericColumnScan(plan)` at **line 251** ✅
- **Path**: `executeDistinctValuesScanWithRawSet()` — LongOpenHashSet, raw set attached
- **STATUS: HITS FAST PATH** ✅

### Q05: `SELECT COUNT(DISTINCT SearchPhrase) FROM hits` — scalar, no GROUP BY
- **Optimizer**: SINGLE (has COUNT(DISTINCT))
- **Fragmenter**: No GROUP BY → strips agg → bare `TableScanNode(columns=[SearchPhrase])`
- **Dispatch**: Hits `isBareSingleVarcharColumnScan(plan)` at **line 259** ✅
- **Path**: `executeDistinctValuesScanVarcharWithRawSet()` — ordinal-based FixedBitSet dedup
- **STATUS: HITS FAST PATH** ✅

### Q08: `SELECT RegionID, COUNT(DISTINCT UserID) AS u FROM hits GROUP BY RegionID ORDER BY u DESC LIMIT 10`
- **Optimizer**: SINGLE (has COUNT(DISTINCT))
- **Fragmenter**: Has GROUP BY + COUNT(DISTINCT)-only → `extractCountDistinctColumns()` succeeds
  - Shard plan: `AggregationNode(PARTIAL, keys=[RegionID, UserID], aggs=[COUNT(*)])`
  - **But wait**: The original plan is `LimitNode -> SortNode -> AggregationNode(SINGLE)`. The fragmenter's `buildShardPlan()` enters the SINGLE branch (line 143), NOT the PARTIAL branch. So `buildShardPlanWithInflatedLimit()` is **never called** — Sort/Limit are implicitly stripped.
  - Shard plan = `AggregationNode(PARTIAL, keys=[RegionID, UserID], aggs=[COUNT(*)])`
- **Dispatch**: `effectivePlan` = AggregationNode (after ProjectNode unwrap at line 204-209)
  - Checks FusedScanAggregate.canFuse() at line 240 — **NO** (has GROUP BY keys)
  - Checks bare scan at line 251 — **NO** (not a TableScanNode)
  - Checks 2-key dedup at line 277: `aggDedupNode.getGroupByKeys().size() == 2` → **YES** (RegionID, UserID), `Step.PARTIAL` → **YES**, `FusedGroupByAggregate.canFuse()` → likely YES
  - `isSingleCountStar` = true (aggs = [COUNT(*)])
  - Both keys numeric → **executeCountDistinctWithHashSets()** at line 303
- **STATUS: HITS 2-KEY DEDUP FAST PATH** ✅
- **BUT**: No TopN pre-filtering at shard level. All ~400 RegionID groups with full HashSets are sent to coordinator. The Sort/Limit is applied only at coordinator.

### Q09: `SELECT RegionID, SUM(AdvEngineID), COUNT(*), AVG(ResolutionWidth), COUNT(DISTINCT UserID) FROM hits GROUP BY RegionID ORDER BY c DESC LIMIT 10`
- **Optimizer**: SINGLE (has COUNT(DISTINCT))
- **Fragmenter**: Has GROUP BY + mixed aggs → `extractCountDistinctColumns()` returns null (not all COUNT(DISTINCT))
  - `buildMixedDedupShardPlan()` called → succeeds
  - Shard plan: `AggregationNode(PARTIAL, keys=[RegionID, UserID], aggs=[SUM(AdvEngineID), COUNT(*), SUM(ResolutionWidth), COUNT(ResolutionWidth)])`
- **Dispatch**: 2-key dedup check at line 277:
  - `getGroupByKeys().size() == 2` → YES
  - `Step.PARTIAL` → YES
  - `isSingleCountStar` = false (4 aggs, not just COUNT(*))
  - `isMixedDedup` check: all aggs match `(sum|count)\(.*\)` → **YES**
  - Both keys numeric → **executeMixedDedupWithHashSets()** at line 329
- **STATUS: HITS MIXED DEDUP FAST PATH** ✅
- **BUT**: Same issue — no TopN pre-filtering. All groups sent to coordinator.

### Q11: `SELECT MobilePhone, MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM hits WHERE MobilePhoneModel <> '' GROUP BY MobilePhone, MobilePhoneModel ORDER BY u DESC LIMIT 10`
- **Optimizer**: SINGLE (has COUNT(DISTINCT))
- **Fragmenter**: Has GROUP BY + COUNT(DISTINCT)-only → `extractCountDistinctColumns()` succeeds
  - Shard plan: `AggregationNode(PARTIAL, keys=[MobilePhone, MobilePhoneModel, UserID], aggs=[COUNT(*)])`
- **Dispatch**: 2-key dedup check at line 277:
  - `getGroupByKeys().size() == 2` → **NO! It's 3** (MobilePhone, MobilePhoneModel, UserID)
  - **FALLS THROUGH** ❌
- Next checks: expression GROUP BY (line 349) — no, ordinal GROUP BY (line 358) — canFuse with 3 keys?
  - If `FusedGroupByAggregate.canFuse()` supports 3 keys → hits fused GROUP BY at line 362
  - Then falls to sorted-limit path at line 370: `extractAggFromSortedLimit(plan)` — **BUT plan is just AggregationNode** (Sort/Limit were stripped by fragmenter). So `extractAggFromSortedLimit` returns null.
  - Falls to generic fused GROUP BY at line 358-366 → `executeFusedGroupByAggregate()`
- **STATUS: HITS FUSED GROUP BY** (not the dedup-specific path) — produces full GROUP BY (MobilePhone, MobilePhoneModel, UserID) with COUNT(*) for ALL groups
- **PROBLEM**: With 3 GROUP BY keys, this produces potentially millions of rows (all unique combos of MobilePhone × MobilePhoneModel × UserID). No TopN, no dedup optimization. The coordinator must then run the full COUNT(DISTINCT) aggregation on this massive result.

### Q13: `SELECT SearchPhrase, COUNT(DISTINCT UserID) AS u FROM hits WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY u DESC LIMIT 10`
- **Optimizer**: SINGLE (has COUNT(DISTINCT))
- **Fragmenter**: Has GROUP BY + COUNT(DISTINCT)-only → `extractCountDistinctColumns()` succeeds
  - Shard plan: `AggregationNode(PARTIAL, keys=[SearchPhrase, UserID], aggs=[COUNT(*)])`
- **Dispatch**: 2-key dedup check at line 277:
  - `getGroupByKeys().size() == 2` → **YES**
  - `Step.PARTIAL` → YES
  - `isSingleCountStar` = true
  - key0 = SearchPhrase (VARCHAR), key1 = UserID (numeric)
  - First branch (both numeric) → NO
  - Second branch (VARCHAR key0 + numeric key1) at line 308 → **YES**
  - **executeVarcharCountDistinctWithHashSets()** at line 311
- **STATUS: HITS VARCHAR DEDUP FAST PATH** ✅
- **BUT**: No TopN pre-filtering. With ~400K unique SearchPhrase values, each with a HashSet of UserIDs, this sends massive data to coordinator.

## Summary: Why All Queries Are Slow Despite Fast Paths

| Query | Fast Path Hit? | Shard Plan | Root Cause of Slowness |
|-------|---------------|------------|----------------------|
| Q04 | ✅ bare numeric scan | TableScanNode(UserID) | Raw LongOpenHashSet with millions of distinct values serialized/transferred |
| Q05 | ✅ bare varchar scan | TableScanNode(SearchPhrase) | Ordinal dedup but still millions of distinct strings transferred |
| Q08 | ✅ 2-key dedup | AggNode(PARTIAL, [RegionID,UserID], COUNT(*)) | No TopN: all ~400 groups + HashSets sent. HashSets contain millions of UserIDs |
| Q09 | ✅ mixed dedup | AggNode(PARTIAL, [RegionID,UserID], SUM/COUNT) | No TopN: all groups + HashSets + accumulators sent |
| Q11 | ❌ generic fused GB | AggNode(PARTIAL, [MobilePhone,MobilePhoneModel,UserID], COUNT(*)) | **3 keys → misses 2-key dedup**. Produces millions of rows (all unique combos). No TopN. |
| Q13 | ✅ varchar dedup | AggNode(PARTIAL, [SearchPhrase,UserID], COUNT(*)) | No TopN: ~400K groups × HashSets. High-cardinality SearchPhrase = massive data |

## Critical Findings

### 1. No TopN-Aware Path for COUNT(DISTINCT)
The `extractAggFromSortedLimit()` path (line 370) only fires for plans that have `LimitNode -> SortNode -> AggregationNode` structure. But for COUNT(DISTINCT) queries, the PlanFragmenter strips Sort/Limit when building the shard plan (because it enters the SINGLE branch, not PARTIAL). The shard plan is just `AggregationNode(PARTIAL, ...)` — no Sort/Limit wrapper. So the TopN optimization **never applies** to COUNT(DISTINCT) queries.

### 2. Q11 Misses 2-Key Dedup (3 GROUP BY keys)
The 2-key dedup check at line 280 requires exactly `getGroupByKeys().size() == 2`. Q11 has 2 original GROUP BY keys + 1 distinct column = 3 keys after fragmenter decomposition. This falls through to generic fused GROUP BY, producing the full cross-product.

### 3. Scalar Queries (Q04, Q05) Transfer Raw Distinct Sets
Even though they hit fast paths, the raw HashSet/string set is serialized and sent to the coordinator. For columns with millions of distinct values, this is inherently expensive — the fast path just avoids Page construction overhead but doesn't reduce data volume.

### 4. The Fallback Generic Pipeline (lines 680-720)
If ALL fast paths miss, the query falls to `LocalExecutionPlanner` which builds a full operator pipeline: `LucenePageSource → HashAggregationOperator`. This is the slowest path. Q11 likely hits the fused GROUP BY (not this), but the fused path with 3 keys is still slow due to data volume.

## Recommendations

1. **Add TopN to COUNT(DISTINCT) dedup plans**: In PlanFragmenter, wrap the dedup AggregationNode with `LimitNode -> SortNode` (inflated limit) so the shard dispatch can apply TopN pre-filtering.
2. **Extend 2-key dedup to N-key dedup**: Generalize the check at line 280 from `size() == 2` to `size() >= 2`, treating the last key as the distinct column and the rest as GROUP BY keys.
3. **For scalar COUNT(DISTINCT) (Q04/Q05)**: Consider approximate COUNT(DISTINCT) via HyperLogLog, or shard-level pre-aggregation that sends only the count (not the full set) when there's only one shard.
4. **For high-cardinality GROUP BY + COUNT(DISTINCT) (Q13)**: Add shard-level TopN on the dedup result before serialization — only send the top-K groups by HashSet size.
