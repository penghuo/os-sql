# COUNT(DISTINCT) Dispatch Analysis — TransportShardExecuteAction.java

## Architecture: Two-Level Decomposition

Calcite produces a `SINGLE` step `AggregationNode` for COUNT(DISTINCT). The `PlanFragmenter.buildShardPlan()` intercepts this and rewrites it:

1. **Scalar COUNT(DISTINCT)** (no GROUP BY): Strips the AggregationNode entirely → shard receives a bare `TableScanNode` with the single distinct column. Shard collects raw distinct values.
2. **GROUP BY + COUNT(DISTINCT)-only**: Rewrites to `AggregationNode(PARTIAL)` with `GROUP BY (original_keys + distinct_columns)` and `COUNT(*)`. This is the "dedup plan".
3. **GROUP BY + mixed aggs (COUNT(DISTINCT) + SUM/COUNT)**: `buildMixedDedupShardPlan()` — same dedup key expansion but preserves decomposable aggregates as partial SUM/COUNT.

## Per-Query Dispatch Paths

### Q04: `SELECT COUNT(DISTINCT UserID)` (global, numeric)
- **PlanFragmenter**: SINGLE, no GROUP BY → strips AggNode → bare `TableScanNode(UserID)`
- **Dispatch**: `isBareSingleNumericColumnScan(plan)` matches at **line 251**
- **Method**: `executeDistinctValuesScanWithRawSet()` at **line 2842**
- **Mechanism**: Collects all distinct UserID values into a `LongOpenHashSet`, attaches raw set to response. Coordinator unions sets across shards, returns count.
- **Bottleneck**: Memory — entire distinct set held in `LongOpenHashSet` per shard. For high-cardinality columns, this can be large.

### Q05: `SELECT COUNT(DISTINCT SearchPhrase)` (global, VARCHAR)
- **PlanFragmenter**: SINGLE, no GROUP BY → strips AggNode → bare `TableScanNode(SearchPhrase)`
- **Dispatch**: `isBareSingleVarcharColumnScan(plan)` matches at **line 259**
- **Method**: `executeDistinctValuesScanVarcharWithRawSet()` at **line 2872**
- **Mechanism**: Uses ordinal-based dedup via `FixedBitSet` on `SortedSetDocValues`, then attaches raw string set to response.
- **Bottleneck**: String materialization — must convert ordinals to actual strings for cross-shard merge. High-cardinality VARCHAR columns produce large string sets.

### Q08: `SELECT RegionID, COUNT(DISTINCT UserID) FROM t GROUP BY RegionID`
- **PlanFragmenter**: SINGLE + GROUP BY + COUNT(DISTINCT)-only → `extractCountDistinctColumns()` returns `[UserID]` → rewritten to `AggregationNode(PARTIAL, GROUP BY [RegionID, UserID], [COUNT(*)])`
- **Dispatch**: Matches the "COUNT(DISTINCT) dedup plan" block at **line 280**:
  - `effectivePlan instanceof AggregationNode` ✓
  - `step == PARTIAL` ✓
  - `groupByKeys.size() >= 2` ✓ (RegionID, UserID = 2 keys)
  - `FusedGroupByAggregate.canFuse()` ✓
  - `isSingleCountStar` ✓ (only `COUNT(*)`)
  - `numKeys == 2` ✓
  - Both keys numeric → enters **line 306** branch
- **Method**: `executeCountDistinctWithHashSets()` at **line 982**
- **Mechanism**: GROUP BY RegionID only, with per-group `LongOpenHashSet` collecting UserID values. Outputs ~450 compact rows + attached HashSets. Coordinator unions HashSets per group.
- **Bottleneck**: Per-group HashSet memory. With ~230 regions × ~17K distinct users each, manageable.

### Q09: `SELECT RegionID, SUM(AdvEngineID), COUNT(*), AVG(ResolutionWidth), COUNT(DISTINCT UserID) FROM t GROUP BY RegionID`
- **PlanFragmenter**: SINGLE + GROUP BY + mixed aggs → `buildMixedDedupShardPlan()` → rewritten to `AggregationNode(PARTIAL, GROUP BY [RegionID, UserID], [SUM(AdvEngineID), COUNT(*), SUM(ResolutionWidth), COUNT(ResolutionWidth)])`
- **Dispatch**: Matches the "COUNT(DISTINCT) dedup plan" block at **line 280**:
  - `isSingleCountStar` = false (multiple aggs)
  - `isMixedDedup` = true (all aggs match `SUM|COUNT` pattern)
  - `groupByKeys.size() == 2` ✓
  - Both keys numeric → enters **line 365** branch
- **Method**: `executeMixedDedupWithHashSets()` at **line 1872**
- **Mechanism**: GROUP BY RegionID with per-group `LongOpenHashSet` for UserID + `long[]` accumulators for SUM/COUNT. Outputs ~400 rows instead of ~25K.
- **Bottleneck**: Same as Q08 plus accumulator arrays. Still manageable for ~230 groups.

### Q11: `SELECT MobilePhone, MobilePhoneModel, COUNT(DISTINCT UserID) FROM t GROUP BY MobilePhone, MobilePhoneModel`
- **PlanFragmenter**: SINGLE + GROUP BY + COUNT(DISTINCT)-only → rewritten to `AggregationNode(PARTIAL, GROUP BY [MobilePhone, MobilePhoneModel, UserID], [COUNT(*)])`
- **Dispatch**: Matches the "COUNT(DISTINCT) dedup plan" block at **line 280**:
  - `isSingleCountStar` ✓
  - `numKeys == 3` → enters N-key path at **line 327**
  - `allNumeric` = false (MobilePhoneModel is VARCHAR) → falls through to mixed-type path at **line 341**
  - Last key (UserID) is numeric ✓, all keys resolved ✓
- **Method**: `executeMixedTypeCountDistinctWithHashSets()` at **line 1590**
- **Mechanism**: Uses Object-based composite keys (Long for numeric, String for VARCHAR) with `LongOpenHashSet` for the dedup key (UserID). Outputs full dedup tuples for coordinator's generic merge.
- **Bottleneck**: Object-based composite keys are slower than primitive-only paths. High cardinality of (MobilePhone × MobilePhoneModel) combinations means many groups, each with a HashSet.

### Q13: `SELECT SearchPhrase, COUNT(DISTINCT UserID) FROM t GROUP BY SearchPhrase`
- **PlanFragmenter**: SINGLE + GROUP BY + COUNT(DISTINCT)-only → rewritten to `AggregationNode(PARTIAL, GROUP BY [SearchPhrase, UserID], [COUNT(*)])`
- **Dispatch**: Matches the "COUNT(DISTINCT) dedup plan" block at **line 280**:
  - `isSingleCountStar` ✓
  - `numKeys == 2` ✓
  - key0 (SearchPhrase) is VARCHAR, key1 (UserID) is numeric → enters VARCHAR+numeric branch at **line 312**
- **Method**: `executeVarcharCountDistinctWithHashSets()` at **line 2351**
- **Mechanism**: Per-group `LongOpenHashSet` indexed by VARCHAR key (SearchPhrase). Uses collect-then-sequential-scan for WHERE-filtered queries. Ordinal-indexed array within segments, HashMap across segments.
- **Bottleneck**: High cardinality of SearchPhrase (~400K+ unique values) means many HashMap entries, each with a HashSet. Memory pressure from string keys + HashSets.

## Summary Table

| Query | Plan Rewrite | Dispatch Line | Execute Method | Key Types | Bottleneck |
|-------|-------------|---------------|----------------|-----------|------------|
| Q04 | Bare TableScan (numeric) | 251 | `executeDistinctValuesScanWithRawSet` (2842) | numeric | Raw HashSet memory |
| Q05 | Bare TableScan (VARCHAR) | 259 | `executeDistinctValuesScanVarcharWithRawSet` (2872) | VARCHAR | String materialization |
| Q08 | 2-key dedup, both numeric | 306 | `executeCountDistinctWithHashSets` (982) | numeric+numeric | Per-group HashSet memory |
| Q09 | 2-key mixed dedup, both numeric | 365 | `executeMixedDedupWithHashSets` (1872) | numeric+numeric | Per-group HashSet + accumulators |
| Q11 | 3-key mixed-type dedup | 341 | `executeMixedTypeCountDistinctWithHashSets` (1590) | mixed (VARCHAR+numeric) | Object composite keys, high group count |
| Q13 | 2-key VARCHAR+numeric | 312 | `executeVarcharCountDistinctWithHashSets` (2351) | VARCHAR+numeric | High-cardinality string HashMap |

## Key Files
- `TransportShardExecuteAction.java` (3576 lines) — dispatch + all execute methods
- `PlanFragmenter.java` — Calcite plan decomposition (SINGLE → dedup rewrite)
- `FusedGroupByAggregate.canFuse()` — eligibility check for the dedup fast path
