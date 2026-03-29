# DQE Borderline Query Handling & REGEXP_REPLACE Patterns

## Q1: REGEXP_REPLACE Evaluation — Pattern Compiled Per-Row?

**File**: `dqe/src/main/java/org/opensearch/sql/dqe/function/scalar/StringFunctions.java`
**Method**: `regexpReplace()` (line ~290-470)

**Answer: NO — Pattern is NOT compiled per-row in the common case.**

The implementation has a **3-tier optimization**:

1. **Cross-batch cache** (lines ~295-298): A closure-captured `cachedPattern[]` array persists the compiled `Pattern` across `processNextBatch()` calls. If the pattern string matches the cached one, recompilation is skipped entirely.

2. **Constant-pattern fast path** (lines ~310-320): When the pattern block has the same value for all positions (typical for SQL like `REGEXP_REPLACE(col, 'literal', 'repl')`), the pattern is compiled once and a single `Matcher` is reused via `matcher.reset(input)` for each row.

3. **Ultra-fast anchored path** (lines ~330-360): For anchored patterns (`^...$`) with a simple group reference (`$1`), it uses `matcher.group()` directly instead of `replaceAll()`, avoiding StringBuffer allocation per row.

4. **Slow fallback** (lines ~420-440): Per-row `Pattern.compile()` only happens when the pattern varies across positions (rare in SQL).

**Existing caching**: `cachedPatternStr[0]`, `cachedPattern[0]`, `cachedReplacementStr[0]` — closure-level cache, survives across batches.

## Q2: AVG Decomposition — What's Still Slow?

**File**: `dqe/src/main/java/org/opensearch/sql/dqe/function/aggregate/AvgAccumulator.java`
**File**: `dqe/src/main/java/org/opensearch/sql/dqe/coordinator/merge/ResultMerger.java`

**Shard-level**: `AvgAccumulator` (lines 1-70) computes `sum/count` per shard. Simple, no issues.

**Coordinator merge**: AVG is decomposed as weighted average: `sum(avg_i * count_i) / sum(count_i)`. This requires a companion `COUNT(*)` column.

**What's still slow for Q02-type queries**:
- AVG presence **disables the ultra-fast single-key path** (`ResultMerger.java` line ~87): `if (numGroupByCols == 1 && !hasAvgAgg(aggregateFunctions))` — AVG queries fall through to the slower `mergeAggregationFastNumeric` which uses `long[][]` + `double[][]` arrays instead of flat `long[]` arrays.
- AVG requires **double arithmetic** (multiply + divide) per row during merge vs simple long addition for COUNT/SUM.
- The `canUseFastNumericMerge` check (line ~200) requires AVG to have a companion COUNT column; if missing, it falls all the way to the generic `HashAggregationOperator` path.

## Q3: Coordinator Merge Logic for Q30/Q31

**File**: `dqe/src/main/java/org/opensearch/sql/dqe/coordinator/transport/TransportTrinoSqlAction.java`

The merge dispatch (lines ~400-475, ~720-775) follows this decision tree:

```
if SINGLE agg → coordinator runs full aggregation on raw passthrough data
elif AggregationNode (FINAL step):
  if no HAVING && has ORDER BY && has LIMIT:
    → mergeAggregationAndSort()  [FUSED: merge + top-N in one pass]
  elif no HAVING && no ORDER BY && has LIMIT:
    → mergeAggregationCapped()   [caps tracked groups to LIMIT]
  else:
    → mergeAggregation() + applyCoordinatorHaving() + applyCoordinatorSort()
else (non-aggregate):
  if has ORDER BY:
    → mergeSorted()
  else:
    → mergePassthrough()
```

**Q30/Q31 bottleneck analysis**:
- Q31 (22K groups, LIMIT 10): Uses `mergeAggregationFastNumericWithSort()` — fused merge+top-N. The comment at line ~310 of ResultMerger.java explicitly mentions Q31: "For Q31 (22K groups, LIMIT 10), this avoids building 22K BlockBuilder entries."
- Q30 (GROUP BY with HAVING): Falls to the **fallback path** — separate `mergeAggregation()` → `applyCoordinatorHaving()` → `applyCoordinatorSort()`. HAVING presence blocks the fused path. This means ALL groups are materialized before filtering.

## Q4: Existing Regex/Pattern Caching Summary

| Location | Caching Mechanism | Scope |
|---|---|---|
| `StringFunctions.regexpReplace()` | Closure-captured `cachedPattern[0]` | Cross-batch (per operator instance) |
| `LikeBlockExpression` (line 27) | `compiledPattern` field compiled at construction | Per-expression lifetime |
| `ResultMerger.AGG_PATTERN` (line 53) | `static final Pattern` | Class-level singleton |

**No global Pattern cache exists** (e.g., no `ConcurrentHashMap<String, Pattern>`). Each `regexpReplace()` lambda instance has its own single-entry cache.
