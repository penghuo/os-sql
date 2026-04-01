# TransportShardExecuteAction Dispatch Logic

File: `dqe/src/main/java/org/opensearch/sql/dqe/shard/transport/TransportShardExecuteAction.java`

## executePlan method (line 195)

The dispatch is a linear chain of if-else checks. First match wins, with a generic operator pipeline fallback at the end.

### Dispatch Order (lines 195-787)

1. **Unwrap ProjectNode** (line 224-232): If `plan instanceof ProjectNode` wrapping an `AggregationNode`, unwrap to `effectivePlan` for fused path matching.

2. **Scalar COUNT(*)** (line 234): `isScalarCountStar(effectivePlan)` → `executeScalarCountStar()` (line 915)

3. **Lucene-native sorted scan** (line 241): `extractSortedScanSpec(plan)` → `executeSortedScan()` (line 3782)

4. **Fused scalar aggregation (no GROUP BY)** (line 252): `effectivePlan instanceof AggregationNode && FusedScanAggregate.canFuse(aggNode)` → `executeFusedScanAggregate()` (line 948)
   - **Q04 route**: `SELECT COUNT(DISTINCT UserID)` does NOT match here. canFuse requires no GROUP BY + child is TableScanNode + supported agg functions. But COUNT(DISTINCT) with SINGLE step gets its AggNode stripped by PlanFragmenter.

5. **Bare single numeric column scan** (line 268): `isBareSingleNumericColumnScan(plan)` → `executeDistinctValuesScanWithRawSet()` (line 3312)
   - **Q04 route**: This IS the Q04 path. PlanFragmenter strips the AggNode for SINGLE COUNT(DISTINCT), leaving bare `TableScanNode[UserID]`. Shard collects distinct values into `LongOpenHashSet`, coordinator counts the union.

6. **Bare single VARCHAR column scan** (line 275): `isBareSingleVarcharColumnScan(plan)` → `executeDistinctValuesScanVarcharWithRawSet()` (line 3342)

7. **Fused eval-aggregate** (line 282): `FusedScanAggregate.canFuseWithEval(aggEvalNode)` → `executeFusedEvalAggregate()` (line 970). For SUM(col + constant) patterns.

8. **N-key COUNT(DISTINCT) dedup with HashSets** (line 291-400):
   Condition: `effectivePlan instanceof AggregationNode aggDedupNode && step==PARTIAL && groupByKeys.size()>=2 && FusedGroupByAggregate.canFuse()`
   - **Q08 route**: `GROUP BY RegionID, COUNT(DISTINCT UserID)` → PlanFragmenter transforms to `GROUP BY (RegionID, UserID) COUNT(*)` with step=PARTIAL. This has 2 keys, isSingleCountStar=true, both numeric → `executeCountDistinctWithHashSets()` (line 1000)
   - Sub-dispatch by key count and types:
     - 2 numeric keys → `executeCountDistinctWithHashSets()` (line 1000)
     - VARCHAR key0 + numeric key1 → `executeVarcharCountDistinctWithHashSets()` (line 2699)
     - 3+ all-numeric keys → `executeNKeyCountDistinctWithHashSets()` (line 1387)
     - Mixed-type keys → `executeMixedTypeCountDistinctWithHashSets()` (line 1841)
   - Mixed dedup (Q10 pattern, SUM+COUNT+COUNT(DISTINCT)) → `executeMixedDedupWithHashSets()` (line 2234)

9. **Expression GROUP BY** (line 403): `FusedGroupByAggregate.canFuseWithExpressionKey()` → `executeFusedExprGroupByAggregate()` (line 3156). For REGEXP_REPLACE etc.

10. **Fused GROUP BY aggregate** (line 414): `FusedGroupByAggregate.canFuse(aggGroupNode)` → `executeFusedGroupByAggregate()` (line 3123)
    - **Q35 route**: `GROUP BY 1, URL` with full-table scan. AggregationNode with step=PARTIAL, has GROUP BY keys, canFuse=true → hits this path.

11. **Fused GROUP BY + Sort + Limit** (line 424-640): `extractAggFromSortedLimit(plan)` detects `LimitNode -> [ProjectNode] -> SortNode -> AggregationNode`. Includes HAVING filter support, top-N optimization.

12. **Limit-only GROUP BY** (line 643): `extractAggFromLimit(plan)` for GROUP BY + LIMIT without ORDER BY.

13. **Sort + Filter (HAVING) without Limit** (line 660): `extractAggFromSortedFilter(plan)` for HAVING queries.

14. **Generic operator pipeline fallback** (line 744-787): Builds `LocalExecutionPlanner` → operator pipeline → drains pages. This is the slow path.

## Q39 (filtered query, CounterID=62)

Filter handling is NOT in the dispatch logic itself. Filters are pushed down into `TableScanNode.getDslFilter()` as OpenSearch DSL JSON. Every execution path calls `compileOrCacheLuceneQuery(scanNode.getDslFilter(), fieldTypeMap)` to get a Lucene Query object. For Q39, the WHERE CounterID=62 becomes a DSL filter on the TableScanNode, and whichever fused path matches the plan structure uses that Lucene query to restrict the doc iteration.

## executeSingleKeyNumericFlat

This method does NOT exist in the file. No match found.

## Key Method Signatures

- `executeCountDistinctWithHashSets(AggregationNode, ShardExecuteRequest, String keyName0, String keyName1, Type t0, Type t1)` → line 1000
- `executeNKeyCountDistinctWithHashSets(AggregationNode, ShardExecuteRequest, List<String> keys, Type[] keyTypes)` → line 1387
- `executeMixedTypeCountDistinctWithHashSets(AggregationNode, ShardExecuteRequest, List<String> keys, Type[] mixedKeyTypes)` → line 1841
- `executeMixedDedupWithHashSets(AggregationNode, ShardExecuteRequest, String keyName0, String keyName1, Type t0, Type t1)` → line 2234
- `executeVarcharCountDistinctWithHashSets(AggregationNode, ShardExecuteRequest, String keyName0, String keyName1, Type t1)` → line 2699
- `executeFusedGroupByAggregate(AggregationNode, ShardExecuteRequest)` → line 3123
- `executeFusedScanAggregate(AggregationNode, ShardExecuteRequest)` → line 948
- `executeDistinctValuesScanWithRawSet(DqePlanNode, ShardExecuteRequest)` → line 3312

## PlanFragmenter COUNT(DISTINCT) Decomposition (PlanFragmenter.java)

- **SINGLE + no GROUP BY** (Q04): Strips AggNode entirely, shard gets bare `TableScanNode[col]`
- **SINGLE + GROUP BY + only COUNT(DISTINCT)** (Q08): Transforms to `AggregationNode(PARTIAL, groupBy=[originalKeys + distinctCols], aggs=[COUNT(*)])`
- **SINGLE + GROUP BY + mixed aggs** (Q10): `buildMixedDedupShardPlan()` → GROUP BY (keys + distinctCols) with partial decomposable aggs
- **PARTIAL step**: Keeps AggNode, strips Sort/Limit above it
