# Q39 Analysis: Query Text, Dispatch Path, and CASE WHEN Handling

## 1. Q39 Query Text (line 40 of `benchmarks/clickbench/queries/queries_trino.sql`)

```sql
SELECT TraficSourceID, SearchEngineID, AdvEngineID,
       CASE WHEN (SearchEngineID = 0 AND AdvEngineID = 0) THEN Referer ELSE '' END AS Src,
       URL AS Dst,
       COUNT(*) AS PageViews
FROM hits
WHERE CounterID = 62
  AND EventDate >= DATE '2013-07-01'
  AND EventDate <= DATE '2013-07-31'
  AND IsRefresh = 0
GROUP BY TraficSourceID, SearchEngineID, AdvEngineID,
         CASE WHEN (SearchEngineID = 0 AND AdvEngineID = 0) THEN Referer ELSE '' END,
         URL
ORDER BY PageViews DESC
OFFSET 1000 LIMIT 10;
```

## 2. Plan Shape Q39 Would Produce

The Trino planner produces:
```
LimitNode(offset=1000, count=10)
  └─ SortNode(ORDER BY PageViews DESC)
       └─ ProjectNode (optional, for column aliasing)
            └─ AggregationNode
                 groupByKeys: [TraficSourceID, SearchEngineID, AdvEngineID, <case_expr_name>, URL]
                 aggregateFunctions: [COUNT(*)]
                 └─ EvalNode
                      outputColumnNames: [<case_expr_name>]
                      expressions: ["CASE WHEN (SearchEngineID = 0 AND AdvEngineID = 0) THEN Referer ELSE '' END"]
                      └─ TableScanNode(filter: CounterID=62 AND EventDate>=... AND IsRefresh=0)
```

**Key characteristics:**
- 5 GROUP BY keys (3 numeric + 1 CASE WHEN expression + 1 VARCHAR URL)
- CASE WHEN expression produces a VARCHAR result (Referer or '')
- COUNT(*) as the only aggregate
- ORDER BY + OFFSET 1000 + LIMIT 10
- WHERE filter on CounterID=62, EventDate range, IsRefresh=0

## 3. Dispatch Path in TransportShardExecuteAction.java

### 3.1 `canFuseWithExpressionKey()` — DOES NOT MATCH Q39

**Location:** `FusedGroupByAggregate.java:280-340` (the `canFuseWithExpressionKey` method)

```java
// FusedGroupByAggregate.java:280
public static boolean canFuseWithExpressionKey(
    AggregationNode aggNode, Map<String, Type> columnTypeMap) {
  // Must have exactly one group-by key   <--- Q39 has 5 keys, FAILS HERE
  if (aggNode.getGroupByKeys().size() != 1) {
    return false;
  }
```

**Q39 has 5 GROUP BY keys**, so `canFuseWithExpressionKey()` returns `false` immediately. This path is only for single-expression-key queries like Q29 (REGEXP_REPLACE on one VARCHAR column).

### 3.2 `canFuse()` — MATCHES Q39

**Location:** `FusedGroupByAggregate.java:186-278`

The `canFuse()` method checks each GROUP BY key:
- `TraficSourceID` → numeric type → OK (line 222: `isNumericOrTimestamp(type)`)
- `SearchEngineID` → numeric type → OK
- `AdvEngineID` → numeric type → OK
- `<case_expr_name>` → NOT in columnTypeMap, NOT DATE_TRUNC, NOT EXTRACT, NOT ARITH → falls to eval check:

```java
// FusedGroupByAggregate.java:247-262
} else if (evalOutputNames.contains(key)) {
  // EvalNode-computed key (e.g., CASE WHEN expression).
  // The expression will be compiled and evaluated per-document
  // using the ExpressionCompiler. Try to compile it now to verify.
  try {
    EvalNode evalNode = (EvalNode) aggNode.getChild();
    int exprIdx = evalNode.getOutputColumnNames().indexOf(key);
    if (exprIdx < 0) {
      return false;
    }
    String exprStr = evalNode.getExpressions().get(exprIdx);
    // Quick check: the expression should not be a plain column pass-through
    if (columnTypeMap.containsKey(exprStr)) {
      return false; // Should have been handled as a plain column
    }
  } catch (Exception e) {
    return false;
  }
```

- `URL` → VARCHAR type → OK

**Result: `canFuse()` returns `true` for Q39.**

### 3.3 Dispatch in TransportShardExecuteAction.java

**Location:** `TransportShardExecuteAction.java:335-350`

The `canFuseWithExpressionKey` check at line 345 is tried FIRST but fails (5 keys ≠ 1).

Then the generic `canFuse` check at line 354 succeeds:

```java
// TransportShardExecuteAction.java:354-362
if (scanFactory == null
    && effectivePlan instanceof AggregationNode aggGroupNode
    && FusedGroupByAggregate.canFuse(
        aggGroupNode, getOrBuildIndexMeta(findIndexName(plan)).columnTypeMap())) {
  List<Page> pages = executeFusedGroupByAggregate(aggGroupNode, req);
  ...
}
```

**BUT WAIT** — Q39 has `LimitNode → SortNode → AggregationNode`, not a bare `AggregationNode`. The `effectivePlan` would be the `LimitNode`, not the `AggregationNode`. So the bare `effectivePlan instanceof AggregationNode` check at line 354 **FAILS**.

The next check at line 365 tries `extractAggFromSortedLimit(plan)`:

```java
// TransportShardExecuteAction.java:365-380
if (scanFactory == null) {
  AggregationNode innerAgg = extractAggFromSortedLimit(plan);
  if (innerAgg != null
      && FusedGroupByAggregate.canFuse(
          innerAgg, getOrBuildIndexMeta(findIndexName(plan)).columnTypeMap())) {
    ...
    SortNode sortNode = extractSortNode(plan);
    LimitNode limitNode = extractLimitNode(plan);
    if (sortNode != null && limitNode != null) {
      // topN = limitNode.getCount() + limitNode.getOffset() = 10 + 1000 = 1010
      long topN = limitNode.getCount() + limitNode.getOffset();
```

This path extracts the inner AggregationNode, calls `canFuse()` (which succeeds), then calls `FusedGroupByAggregate.executeWithTopN()` with `topN=1010`.

### 3.4 Inside `executeInternal()` — Route to `executeWithEvalKeys()`

**Location:** `FusedGroupByAggregate.java:1078-1098`

Inside `executeInternal()`, after classifying keys:
- `TraficSourceID` → KeyInfo(numeric, no expr)
- `SearchEngineID` → KeyInfo(numeric, no expr)
- `AdvEngineID` → KeyInfo(numeric, no expr)
- `<case_expr>` → KeyInfo(type=VARCHAR, isVarchar=true, exprFunc="eval", exprUnit="<idx>")
- `URL` → KeyInfo(VARCHAR, isVarchar=true, no expr)

Since `hasVarchar=true` and `hasEvalKey=true`:

```java
// FusedGroupByAggregate.java:1078-1098
boolean hasEvalKey = false;
for (KeyInfo ki : keyInfos) {
  if ("eval".equals(ki.exprFunc)) {
    hasEvalKey = true;
    break;
  }
}
if (hasEvalKey) {
  return executeWithEvalKeys(
      aggNode, shard, query, keyInfos, specs, columnTypeMap,
      groupByKeys, sortAggIndex, sortAscending, topN);
}
```

**Q39 routes to `executeWithEvalKeys()` — the fused eval-key path.**

## 4. `executeWithEvalKeys()` Method

**Location:** `FusedGroupByAggregate.java:5813-6400+`

```java
private static List<Page> executeWithEvalKeys(
    AggregationNode aggNode,
    IndexShard shard,
    Query query,
    List<KeyInfo> keyInfos,
    List<AggSpec> specs,
    Map<String, Type> columnTypeMap,
    List<String> groupByKeys,
    int sortAggIndex,
    boolean sortAscending,
    long topN) throws Exception
```

**What it does for Q39:**

1. **Compiles CASE WHEN expression** using `ExpressionCompiler` (lines ~5850-5900)
2. **Detects inline CASE WHEN optimization** (lines ~5920-6020):
   - Pattern: `CASE WHEN (SearchEngineID = 0 AND AdvEngineID = 0) THEN Referer ELSE '' END`
   - Extracts equality comparisons: `SearchEngineID == 0`, `AdvEngineID == 0`
   - Maps condition columns to GROUP BY key indices (SearchEngineID=key[1], AdvEngineID=key[2])
   - Result column: `Referer` (VARCHAR, read from DocValues)
   - Else value: `''` (empty string)
   - **Sets `inlineEvalKey[k] = true`** — bypasses Block-based expression evaluation entirely
3. **Single-segment flat long map path** (lines ~6100+):
   - All keys resolve to longs: numeric keys → raw values, VARCHAR keys → ordinals, inline CASE WHEN → ordinal or sentinel
   - Uses open-addressing hash map with `long[] flatKeys` (zero allocation per row)
   - For inline CASE WHEN: checks condition using already-resolved numeric key values, reads Referer ordinal only when condition is true, uses sentinel `-2L` for else branch
4. **Top-N selection** with heap sort for ORDER BY PageViews DESC LIMIT 1010

## 5. Why Q39 is 16.9x Slow — Analysis

**Q39 DOES hit the fused path** (`executeWithEvalKeys`), NOT the generic pipeline. The slowness likely comes from:

1. **5 GROUP BY keys** — the N-key path uses `LinkedHashMap<Object, AccumulatorGroup>` or flat long map, but with 5 keys the hash computation and comparison cost is high
2. **High cardinality**: URL has ~700K+ unique values, Referer has ~16K+ unique values → potentially millions of unique group combinations
3. **OFFSET 1000** — requires computing ALL groups (not just top 10), sorting, then skipping 1000. The `topN=1010` helps limit shard output but all groups must still be aggregated
4. **Filtered query** (CounterID=62 + date range + IsRefresh=0) — not MatchAllDocsQuery, so uses Weight+Scorer path which is slower than direct DocValues iteration
5. **Per-document cost**: 5 DocValues reads (3 numeric + 1 VARCHAR URL + conditional Referer) + hash computation + hash probe per matching doc

**The inline CASE WHEN optimization IS applied** — it avoids Block materialization and expression evaluation per doc. But the fundamental cost is the 5-key grouping over high-cardinality VARCHAR columns.
