# COUNT(DISTINCT) Dispatch Logic in TransportShardExecuteAction.java

## 1. executePlan Dispatch Logic (line 194)

The `executePlan` method at line 194 unwraps `ProjectNode` → `AggregationNode` (line 204-210), then dispatches through these fast paths in order:

| Priority | Lines | Pattern | Method |
|----------|-------|---------|--------|
| 1 | 213-217 | Scalar COUNT(*) | `executeScalarCountStar` |
| 2 | 222-232 | LimitNode→SortNode→TableScanNode | `executeSortedScan` |
| 3 | 236-244 | Scalar agg (no GROUP BY), FusedScanAggregate.canFuse | `executeFusedScanAggregate` (line 930) |
| 4 | 248-254 | Bare single numeric column scan | `executeDistinctValuesScanWithRawSet` (line 3005) |
| 5 | 258-263 | Bare single VARCHAR column scan | `executeDistinctValuesScanVarcharWithRawSet` (line 3035) |
| 6 | 267-274 | SUM(col+const) eval-agg | `executeFusedEvalAggregate` (line 952) |
| 7 | 278-380 | **COUNT(DISTINCT) dedup plan** (N>=2 keys, PARTIAL step) | Multiple HashSet paths (see §3) |
| 8 | 382-396 | Expression GROUP BY (REGEXP_REPLACE etc.) | `executeFusedExprGroupByAggregate` (line 2849) |
| 9 | 399-407 | Ordinal-based GROUP BY | `executeFusedGroupByAggregate` (line 2816) |
| 10 | 412-425 | GROUP BY with sort+limit | `executeFusedGroupByAggregateWithTopN` (line 2872) |
| fallback | ~430+ | Generic operator pipeline | LucenePageSource → HashAggregationOperator |

## 2. Two-Level Calcite Plan Pattern for COUNT(DISTINCT)

Calcite decomposes `COUNT(DISTINCT x)` into two aggregation levels:
- **Inner (PARTIAL)**: `GROUP BY (groupKeys..., x)` with `COUNT(*)` — produces dedup tuples
- **Outer (FINAL)**: `GROUP BY (groupKeys...)` with `COUNT(*)` — counts distinct values

The shard receives the **inner** plan (Step.PARTIAL) with N keys where the last key is the dedup key. The dispatch at line 278 checks:
- `effectivePlan instanceof AggregationNode`
- `aggNode.getStep() == Step.PARTIAL`
- `aggNode.getGroupByKeys().size() >= 2`
- `FusedGroupByAggregate.canFuse()`

## 3. Existing Fast Paths for COUNT(DISTINCT)

### A. Scalar COUNT(DISTINCT numeric) — LongOpenHashSet (line 248-254)
- Bare TableScanNode with single numeric column
- Collects into `LongOpenHashSet`, attaches raw set to response
- Method: `executeDistinctValuesScanWithRawSet` (line 3005)

### B. Scalar COUNT(DISTINCT varchar) — FixedBitSet ordinals (line 258-263)
- Bare TableScanNode with single VARCHAR column
- Uses `FixedBitSet` for ordinal-based dedup
- Method: `executeDistinctValuesScanVarcharWithRawSet` (line 3035)

### C. 2-key numeric dedup — HashSet-per-group (line 307)
- GROUP BY (key0, key1) where both numeric
- Builds `Map<Long, LongOpenHashSet>` — key0 → set of key1 values
- Method: `executeCountDistinctWithHashSets` (line 982)

### D. 2-key VARCHAR+numeric dedup (line 316-322)
- key0=VARCHAR, key1=numeric
- Method: `executeVarcharCountDistinctWithHashSets` (line 2392)

### E. N-key all-numeric dedup (line 330-342)
- 3+ keys, all numeric
- Outputs full dedup tuples for coordinator generic merge
- Method: `executeNKeyCountDistinctWithHashSets` (line 1311)

### F. N-key mixed-type dedup (line 344-360)
- Last key (dedup) is numeric, some GROUP BY keys are VARCHAR
- Uses `ObjectArrayKey` composite keys
- Method: `executeMixedTypeCountDistinctWithHashSets` (line 1631)

### G. Mixed dedup with SUM/COUNT accumulators (line 362-378)
- 2-key with mixed SUM/COUNT aggregates (not just COUNT(*))
- Method: `executeMixedDedupWithHashSets` (line 1913)

## 4. Fusion Intercept Point

The COUNT(DISTINCT) dispatch block spans **lines 278-380**. The key decision tree:
- Line 278: Entry guard (`AggregationNode`, `Step.PARTIAL`, `size >= 2`, `canFuse`)
- Line 289: Branch on `isSingleCountStar` vs `isMixedDedup`
- Line 296-342: Single COUNT(*) paths (2-key numeric → N-key numeric → mixed-type)
- Line 362-378: Mixed dedup path

**Best intercept point for new fused paths**: Around **line 278-295** (before the existing sub-dispatch), or as new branches within the `isSingleCountStar` block at lines 296-342.

## 5. ClickBench SQL for Target Queries

| Query | SQL | COUNT(DISTINCT) Pattern |
|-------|-----|------------------------|
| Q04 | `SELECT AVG(UserID) FROM hits;` | No COUNT(DISTINCT) — scalar AVG |
| Q05 | `SELECT COUNT(DISTINCT UserID) FROM hits;` | Scalar COUNT(DISTINCT numeric) — bare scan path |
| Q08 | `SELECT AdvEngineID, COUNT(*) FROM hits WHERE AdvEngineID <> 0 GROUP BY AdvEngineID ORDER BY COUNT(*) DESC;` | No COUNT(DISTINCT) — GROUP BY with COUNT(*) |
| Q09 | `SELECT RegionID, COUNT(DISTINCT UserID) AS u FROM hits GROUP BY RegionID ORDER BY u DESC LIMIT 10;` | 2-key numeric dedup: GROUP BY (RegionID, UserID) |
| Q11 | `SELECT MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM hits WHERE MobilePhoneModel <> '' GROUP BY MobilePhoneModel ORDER BY u DESC LIMIT 10;` | VARCHAR+numeric dedup: GROUP BY (MobilePhoneModel, UserID) |
| Q13 | `SELECT SearchPhrase, COUNT(*) AS c FROM hits WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10;` | No COUNT(DISTINCT) — GROUP BY VARCHAR with COUNT(*) |

### Queries with actual COUNT(DISTINCT): Q05, Q09, Q11
- **Q05**: Scalar path (bare scan → LongOpenHashSet)
- **Q09**: 2-key numeric path → `executeCountDistinctWithHashSets` (line 982)
- **Q11**: VARCHAR key0 + numeric key1 → `executeVarcharCountDistinctWithHashSets` (line 2392)

### Queries without COUNT(DISTINCT): Q04, Q08, Q13
- **Q04**: Scalar AVG → `executeFusedScanAggregate` (line 930)
- **Q08**: GROUP BY + COUNT(*) → `executeFusedGroupByAggregate` or sorted-limit path
- **Q13**: GROUP BY VARCHAR + COUNT(*) → ordinal-based GROUP BY path
