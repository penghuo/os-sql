# Expression GROUP BY Dispatch & Execution Analysis

## 1. Dispatch Path in TransportShardExecuteAction.java

### Expression GROUP BY dispatch (lines ~270-280 in executePlan)

The dispatch follows a priority chain. Expression GROUP BY is handled by **two** paths:

**Path A: Ordinal-cached expression key (REGEXP_REPLACE case)**
```java
// Line ~270: This check MUST come before generic canFuse()
if (scanFactory == null
    && effectivePlan instanceof AggregationNode aggExprNode
    && FusedGroupByAggregate.canFuseWithExpressionKey(
        aggExprNode, getOrBuildIndexMeta(findIndexName(plan)).columnTypeMap())) {
  List<Page> pages = executeFusedExprGroupByAggregate(aggExprNode, req);
  // ...
}
```
Calls `FusedGroupByAggregate.executeWithExpressionKey()`.

**Path B: Generic fused GROUP BY with EvalNode (fallback)**
```java
// Line ~280: Generic fused path — handles eval keys via executeWithEvalKeys()
if (scanFactory == null
    && effectivePlan instanceof AggregationNode aggGroupNode
    && FusedGroupByAggregate.canFuse(aggGroupNode, ...)) {
  List<Page> pages = executeFusedGroupByAggregate(aggGroupNode, req);
}
```
Inside `canFuse()`, eval keys are detected and routed to `executeWithEvalKeys()`.

**Path C: HAVING clause path (lines ~300-370)**
When a FilterNode sits between SortNode and AggregationNode:
```java
FilterNode havingFilter = extractFilterFromSortedLimit(plan);
if (havingFilter != null) {
  boolean isExprKey = FusedGroupByAggregate.canFuseWithExpressionKey(innerAgg, colTypeMap);
  List<Page> aggPages = isExprKey
      ? executeFusedExprGroupByAggregate(innerAgg, req)
      : executeFusedGroupByAggregate(innerAgg, req);
  aggPages = applyHavingFilter(havingFilter, aggPages, innerAgg, colTypeMap);
  // sort + project...
}
```

### HAVING clause handling (TransportShardExecuteAction ~line 300+)
`applyHavingFilter()` method:
1. Builds column index map from aggregation output columns
2. Compiles the HAVING predicate using `ExpressionCompiler`
3. Applies filter using `FilterOperator` on the aggregated pages
4. Returns filtered pages

Also handled in the `extractAggFromSortedFilter()` path (lines ~430-480) for HAVING without LIMIT.

## 2. Expression Evaluation in FusedGroupByAggregate.java

### canFuseWithExpressionKey() — line ~195
Checks:
- Exactly 1 group-by key
- Child is `EvalNode -> TableScanNode`
- Group-by key is NOT a physical column (must be computed)
- Key is in EvalNode's output column names
- Expression references a single VARCHAR source column
- All aggregate functions are supported

### executeWithExpressionKey() — line ~240
**This is the ordinal-based optimization path.** Key steps:

1. **Compile expressions** (lines ~280-310):
```java
ExpressionCompiler compiler = new ExpressionCompiler(registry, colIndexMap, columnTypeMap);
BlockExpression keyBlockExpr = compiler.compile(keyAst);
// Also compiles computed aggregate arg expressions (e.g., length(Referer))
```

2. **Pre-compute per ordinal** (inside `executeWithExpressionKeyImpl`, lines ~350-430):
```java
// Process ordinals in batches of 4096
for (int batchStart = 0; batchStart < ordCountInt; batchStart += batchSize) {
    // Build input Block from ordinal bytes
    Block keyResultBlock = keyBlockExpr.evaluate(inputPage);
    // Cache: ordToGroupKey[ord] = expression result string
}
```
Expression is evaluated **N times** (once per unique ordinal, ~16K) instead of **M times** (once per doc, ~921K).

3. **Scan loop uses cached results** (lines ~440-500):
```java
return new LeafCollector() {
    public void collect(int doc) throws IOException {
        if (!dv.advanceExact(doc)) return;
        int ord = (int) dv.nextOrd();
        AccumulatorGroup accGroup = ordGroups[ord]; // O(1) lookup!
        // accumulate aggregates...
    }
};
```
**Expression is NOT evaluated per-doc.** Only ordinal → pre-computed group lookup.

### executeWithEvalKeys() — line ~1050 (generic eval path)
For CASE WHEN and other complex expressions:
- Collects matching doc IDs per segment
- Processes in micro-batches of 256 rows
- Builds Block objects for eval columns
- Evaluates expressions via `BlockExpression.evaluate(Page)`
- Groups using `ProbeGroupKey` → `MergedGroupKey` HashMap

**Expression IS evaluated per-doc** in this path (no ordinal caching).

## 3. Ordinal-Based Optimization

**YES, it exists.** The `canFuseWithExpressionKey()` + `executeWithExpressionKey()` path:

| Aspect | Value |
|--------|-------|
| Method | `executeWithExpressionKeyImpl()` |
| Optimization | Evaluate expression once per unique SortedSetDocValues ordinal |
| Batch size | 4096 ordinals per batch |
| Cache structure | `String[] ordToGroupKey` (ordinal → expression result) |
| Per-doc cost | `advanceExact() + nextOrd() + array[ord]` — no expression eval |
| Documented reduction | ~16K ordinals vs ~921K docs = ~58x for Q29 REGEXP_REPLACE |

## 4. Pattern Caching / Regex Optimization

### Compiled Pattern constants in FusedGroupByAggregate (class level):
```java
private static final Pattern AGG_FUNCTION = Pattern.compile("^\\s*(COUNT|SUM|MIN|MAX|AVG)\\(...)");
private static final Pattern DATE_TRUNC_PATTERN = Pattern.compile("^date_trunc\\(...)");
private static final Pattern ARITH_EXPR_PATTERN = Pattern.compile("^\\(?(\\w+)\\s*([+\\-*/])...)");
private static final Pattern EXTRACT_PATTERN = Pattern.compile("^EXTRACT\\(...)");
```
These are **static final** — compiled once at class load.

### For REGEXP_REPLACE specifically:
- The regex pattern inside the SQL expression (e.g., `'^http://.*'`) is compiled by the `ExpressionCompiler` → `BlockExpression` chain
- The `BlockExpression` for REGEXP_REPLACE likely uses `java.util.regex.Pattern` internally (in the function registry)
- The ordinal-caching optimization means the compiled Pattern is reused across all ordinal evaluations within a batch
- **No explicit Pattern caching at the FusedGroupByAggregate level** — caching is implicit via the ordinal-based approach (evaluate once per unique value)

### Lucene Query caching:
```java
// TransportShardExecuteAction line ~170
private static final ConcurrentHashMap<String, Query> LUCENE_QUERY_CACHE = new ConcurrentHashMap<>();
```
DSL filter → compiled Lucene Query is cached across concurrent shard executions.

## 5. Key Files & Line References

| Component | File | Key Lines |
|-----------|------|-----------|
| Expression dispatch | TransportShardExecuteAction.java | ~270-280 (canFuseWithExpressionKey check) |
| HAVING dispatch | TransportShardExecuteAction.java | ~300-370 (extractFilterFromSortedLimit + applyHavingFilter) |
| HAVING filter apply | TransportShardExecuteAction.java | applyHavingFilter() method |
| Ordinal cache check | FusedGroupByAggregate.java | canFuseWithExpressionKey() ~line 195 |
| Ordinal cache exec | FusedGroupByAggregate.java | executeWithExpressionKey() ~line 240 |
| Per-ordinal eval | FusedGroupByAggregate.java | executeWithExpressionKeyImpl() ~line 350 |
| Scan loop (cached) | FusedGroupByAggregate.java | LeafCollector.collect() ~line 440 |
| Generic eval path | FusedGroupByAggregate.java | executeWithEvalKeys() ~line 1050 |
| Static Patterns | FusedGroupByAggregate.java | lines 70-100 (AGG_FUNCTION, DATE_TRUNC, etc.) |
