# Q29 Expression Key Caching Analysis

## Q29 SQL
```sql
SELECT REGEXP_REPLACE(Referer, '^https?://(?:www\\.)?([^/]+)/.*$', '\\1') AS k,
       AVG(length(Referer)) AS l, COUNT(*) AS c, MIN(Referer)
FROM hits WHERE Referer <> ''
GROUP BY REGEXP_REPLACE(Referer, '^https?://(?:www\\.)?([^/]+)/.*$', '\\1')
HAVING COUNT(*) > 100000 ORDER BY l DESC LIMIT 25
```

## 1. Dispatch Flow: Branch 10a IS Taken and DOES Check Expression Key

**File:** `TransportShardExecuteAction.java:401-458`

Q29 has HAVING + ORDER BY + LIMIT, so it matches Branch 10 (`extractAggFromSortedLimit`). Within Branch 10, the HAVING sub-path (10a) is taken because `extractFilterFromSortedLimit(plan)` returns a non-null `FilterNode`.

**Lines 401-405 (Branch 10a):**
```java
FilterNode havingFilter = extractFilterFromSortedLimit(plan);
if (havingFilter != null) {
  boolean isExprKey = FusedGroupByAggregate.canFuseWithExpressionKey(innerAgg, colTypeMap);
  List<Page> aggPages = isExprKey
      ? executeFusedExprGroupByAggregate(innerAgg, req)
      : executeFusedGroupByAggregate(innerAgg, req);
```

**Conclusion:** Branch 10a explicitly checks `canFuseWithExpressionKey()`. If it returns `true`, `executeFusedExprGroupByAggregate()` is called, which delegates to `FusedGroupByAggregate.executeWithExpressionKey()`.

## 2. canFuseWithExpressionKey() — Does Q29 Pass?

**File:** `FusedGroupByAggregate.java:312-378`

The method checks:
1. ✅ **Exactly one group-by key** — Q29 groups by `REGEXP_REPLACE(Referer, ...)` (one key)
2. ✅ **Child is EvalNode → TableScanNode** — The planner wraps `REGEXP_REPLACE` in an EvalNode
3. ✅ **Group-by key is NOT a physical column** — `k` is a computed expression
4. ✅ **Key is in EvalNode output names** — The EvalNode produces the REGEXP_REPLACE result
5. ✅ **Expression references a single VARCHAR source column** — Only `Referer` (VARCHAR)
6. **Aggregate function check (lines 363-376):**

```java
for (String func : aggNode.getAggregateFunctions()) {
  Matcher m = AGG_FUNCTION.matcher(func);
  if (!m.matches()) return false;
  String arg = m.group(3).trim();
  if (!"*".equals(arg)
      && !columnTypeMap.containsKey(arg)
      && !evalNode.getOutputColumnNames().contains(arg)) {
    return false;
  }
}
```

Q29's aggregates:
- `COUNT(*)` → arg is `*` → ✅ passes
- `AVG(length(Referer))` → The planner decomposes this. The arg `length(Referer)` is an **EvalNode output** (the EvalNode computes `length(Referer)` as a named column). The check `evalNode.getOutputColumnNames().contains(arg)` → ✅ passes
- `MIN(Referer)` → arg is `Referer`, which is in `columnTypeMap` → ✅ passes

**Conclusion: `canFuseWithExpressionKey()` returns `true` for Q29. The expression key path IS active.**

## 3. executeWithExpressionKey() — Ordinal Caching Logic

**File:** `FusedGroupByAggregate.java:391-520` (setup) and `527-700` (impl)

### How Expression Key Caching Works

The method operates in two phases per segment:

**Phase 1: Pre-compute expressions per ordinal (lines 612-650)**
```java
// Process ordinals in batches for expression evaluation
int batchSize = Math.min(ordCountInt, 4096);
for (int batchStart = 0; batchStart < ordCountInt; batchStart += batchSize) {
    // Build a Page with one VARCHAR block containing ordinal values
    // Evaluate REGEXP_REPLACE on the batch → ordToGroupKey[ord]
    // Evaluate length(Referer) on the batch → ordToComputedArg[i][ord]
}
```

For each unique ordinal in `SortedSetDocValues`:
- `ordToGroupKey[ord]` = result of `REGEXP_REPLACE(ordinal_value, ...)`
- `ordToComputedArg[i][ord]` = result of `length(ordinal_value)` (for AVG's arg)

**Phase 2: Per-doc accumulation using cached values (lines 670-700)**
```java
public void collect(int doc) throws IOException {
    if (!dv.advanceExact(doc)) return;
    int ord = (int) dv.nextOrd();
    AccumulatorGroup accGroup = ordGroups[ord];
    // For computed args like length(Referer):
    if (isComputedArg[i]) {
        long val = finalOrdToComputedArg[i][ord];  // O(1) array lookup!
        // accumulate into AVG (longSum += val, count++)
    }
    // For VARCHAR args like MIN(Referer):
    if (finalIsVarcharSameCol[i]) {
        String val = finalOrdToRawString[ord];  // O(1) cached string lookup!
        // accumulate into MIN
    }
}
```

### Evaluation Count
- **REGEXP_REPLACE**: Evaluated once per unique ordinal (N ordinals), NOT once per doc (M docs)
- **length(Referer)**: Also evaluated once per unique ordinal and cached in `ordToComputedArg`
- **Per-doc cost**: Just `advanceExact(doc)` + `nextOrd()` + array index lookup — no expression evaluation

### For Q29 specifically:
- ~81M docs with non-empty Referer
- Referer has millions of unique ordinals (let's say ~16M)
- Without caching: 81M regex evaluations
- With caching: ~16M regex evaluations (one per ordinal)
- **Reduction: ~5x fewer regex evaluations**

## 4. AVG(length(Referer)) Compatibility

**AVG(length(Referer)) IS compatible** with the expression key path. Here's why:

The planner creates an EvalNode that computes `length(Referer)` as a named output column. In `executeWithExpressionKey()` (lines 445-453):

```java
// Check if the arg is a computed EvalNode column
int evalIdx = evalNode.getOutputColumnNames().indexOf(spec.arg);
if (evalIdx >= 0 && !columnTypeMap.containsKey(spec.arg)) {
    isComputedArg[i] = true;
    computedArgExpr[i] = evalNode.getExpressions().get(evalIdx);
    // Computed args like length(Referer) produce BigintType
}
```

The `length(Referer)` expression is:
1. Recognized as an EvalNode-computed arg (`isComputedArg[i] = true`)
2. Compiled into a `BlockExpression` (`computedArgBlockExprs[i]`)
3. Pre-evaluated per ordinal in the batch loop (line 641-648)
4. Cached in `ordToComputedArg[i]` as a `long[]` array
5. During per-doc accumulation, the cached value is used directly: `long val = ordToComputedArg[i][ord]`

The AVG accumulator receives the cached `long` value and accumulates `longSum += val; count++`.

## 5. MIN(Referer) Compatibility

**MIN(Referer) IS compatible.** Since `Referer` is the same source VARCHAR column used for the group-by key, the code detects this (lines 590-600):

```java
if (!isCountStar[i] && !isComputedArg[i] && isVarcharArg[i]
    && specs.get(i).arg.equals(srcCol)) {
    isVarcharSameCol[i] = true;
}
```

The raw string is cached per ordinal in `ordToRawString[ord]`, and MIN comparison uses the cached string directly during per-doc accumulation.

## Summary

| Question | Answer |
|----------|--------|
| Does Branch 10a check `canFuseWithExpressionKey()`? | **Yes** (line 403) |
| Does Q29 pass `canFuseWithExpressionKey()`? | **Yes** — all checks pass |
| Is `executeFusedExprGroupByAggregate()` used? | **Yes** — dispatched at line 404 |
| Is expression key caching active? | **Yes** — REGEXP_REPLACE evaluated per ordinal, not per doc |
| Is `AVG(length(Referer))` compatible? | **Yes** — `length(Referer)` is an EvalNode output, cached per ordinal |
| Is `MIN(Referer)` compatible? | **Yes** — same-column VARCHAR arg, cached per ordinal |
| Evaluation model | Once per unique ordinal (not once per doc) |
| Expected speedup from caching | ~5x (81M docs → ~16M ordinal evaluations for regex) |

## Performance Note

If Q29 still takes 35s, the bottleneck is likely NOT the expression evaluation (which is already cached). Possible bottlenecks:
1. **Ordinal count is very high** (millions of unique Referer values) — the pre-computation phase itself is expensive
2. **HashMap overhead** — `globalGroups` is a `HashMap<String, AccumulatorGroup>`, and with millions of groups, hash collisions and GC pressure from String keys are significant
3. **Per-doc advanceExact cost** — 81M `SortedSetDocValues.advanceExact()` calls are inherently expensive
4. **HAVING filter** — must aggregate ALL groups before filtering, so no early termination
