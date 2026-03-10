# DQE Predicate Pushdown Enhancement — Design

**Goal:** Push WHERE predicates from `FilterNode` (Java-side row filtering) into `LucenePageSource` (index-level filtering via Lucene queries) so that non-matching rows are never read from doc values.

**Motivation:** Perf-lite baseline shows queries with un-pushed predicates scanning all 1M rows through `FilterOperator`, then discarding 99%+. Q21 (`WHERE URL LIKE '%google%'`) takes 5.6s because `LucenePageSource` reads every row with `MatchAllDocsQuery`.

## Architecture

The existing coordinator→shard flow stays the same:

```
Coordinator: PlanOptimizer.tryConvertToDsl(predicate) → DSL JSON → TableScanNode.dslFilter
Shard:       LuceneQueryCompiler.compile(dslJson) → Lucene Query → LucenePageSource
```

`PlanOptimizer` gains field type awareness and handles more predicate types. `LuceneQueryCompiler` gains wildcard query support. `FilterNode` is removed when the predicate is fully pushed.

## Predicate Types

| SQL Predicate | DSL Output | ClickBench Queries |
|---|---|---|
| `col = value` | `{"term":{"col":value}}` | (existing) |
| `col <> value` | `{"bool":{"must_not":[{"term":{"col":value}}]}}` | Q2, Q6, Q8, Q11, Q12, Q22, Q25-Q27, Q37-Q41 |
| `col >= value` | `{"range":{"col":{"gte":value}}}` | Q37-Q41 (EventDate) |
| `col <= value` | `{"range":{"col":{"lte":value}}}` | Q37-Q41 (EventDate) |
| `col > value` / `col < value` | `{"range":{"col":{"gt":value}}}` | general |
| `A AND B` | `{"bool":{"must":[dslA, dslB]}}` | Q37-Q41, Q22-Q23, Q39-Q40 |
| `A OR B` | `{"bool":{"should":[dslA, dslB]}}` | general |
| `NOT A` | `{"bool":{"must_not":[dslA]}}` | general |
| `col LIKE '%pat%'` | `{"wildcard":{"col":{"value":"*pat*"}}}` | Q21-Q24 |
| `col IN (v1, v2)` | `{"terms":{"col":[v1, v2]}}` | general |
| `col BETWEEN a AND b` | `{"range":{"col":{"gte":a,"lte":b}}}` | general |

## Field Type Awareness

`PlanOptimizer` receives a `Map<String, String>` mapping field names to OpenSearch types (e.g., `"URL" → "keyword"`, `"SocialAction" → "text"`). This enables:

- **LIKE pushdown**: Only for `keyword` fields (inverted index stores exact values). Text fields are analyzed/tokenized, so wildcard queries match tokens, not original strings.
- **Numeric range/term queries**: Use `LongPoint` for integer/long/short/byte/date types.
- **Value formatting**: Date literals converted to epoch millis for range queries.

The coordinator already resolves `TableInfo` from `OpenSearchMetadata`. The field type map is extracted from `TableInfo.columns()` and passed to `PlanOptimizer`.

## Pushdown Rules

1. Parse the predicate string into a Trino `Expression` AST using `DqeSqlParser`.
2. Recursively attempt to convert each AST node to DSL JSON.
3. If the entire expression converts successfully, store the DSL in `TableScanNode.dslFilter` and remove the `FilterNode`.
4. If any sub-expression cannot be converted (unsupported function, text-field LIKE, etc.), keep the `FilterNode` with the full predicate. No partial pushdown — keeps correctness simple.

## Files Changed

- **`PlanOptimizer.java`** — Accept field type map. Replace regex-based `tryConvertToDsl` with AST-based recursive converter.
- **`TransportTrinoSqlAction.java`** (coordinator) — Pass field type map to `PlanOptimizer`.
- **`LuceneQueryCompiler.java`** — Add `{"wildcard":{"field":{"value":"*pat*"}}}` support.
- **`LuceneQueryCompilerTest.java`** — Add wildcard test.
- **`PlanOptimizerTest.java`** — Add tests for new predicate types.

## Expected Impact

| Query | Before | After (estimated) | Reason |
|---|---|---|---|
| Q21 `COUNT(*) WHERE URL LIKE` | 5.6s | ~0.1s | WildcardQuery skips 99%+ rows |
| Q37-Q41 (compound WHERE) | 4-12s | ~0.5-2s | Range + equality pushed to Lucene |
| Q22-Q23 (LIKE + GROUP BY) | 6-18s | ~2-5s | Scan reduction, GROUP BY still expensive |
| Q2 `COUNT(*) WHERE <> 0` | 0.072s | ~0.06s | Minor — already fast |
| Q25-Q27 (`<>` + SORT) | 0.5s | ~0.3s | Minor scan reduction |
