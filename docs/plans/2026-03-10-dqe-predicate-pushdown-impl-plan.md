# DQE Predicate Pushdown Enhancement Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Push WHERE predicates (`<>`, `>=`, `<=`, `LIKE`, `AND`/`OR`, `IN`, `BETWEEN`) from `FilterNode` into Lucene queries via DSL, reducing full-table scans on the shard.

**Architecture:** Extend `PlanOptimizer.tryConvertToDsl()` from regex-based equality-only to AST-based recursive conversion of Trino `Expression` nodes. The coordinator parses the predicate string into a Trino AST, walks it, and emits DSL JSON. The shard-side `LuceneQueryCompiler` (already supports term, range, bool, terms) compiles DSL to Lucene `Query`. Add field type map to `PlanOptimizer` for safe LIKE-on-keyword-only pushdown.

**Tech Stack:** Trino SQL parser (`io.trino.sql.tree.*`), Lucene 10.4.0 (`WildcardQuery`), Jackson (DSL JSON), OpenSearch `TableInfo` for field types.

---

### Task 1: Add Field Types to PlanOptimizer

**Files:**
- Modify: `dqe/src/main/java/org/opensearch/sql/dqe/planner/optimizer/PlanOptimizer.java`
- Modify: `dqe/src/main/java/org/opensearch/sql/dqe/coordinator/transport/TransportTrinoSqlAction.java`
- Modify: `dqe/src/test/java/org/opensearch/sql/dqe/planner/optimizer/PlanOptimizerTest.java`

**Context:** `PlanOptimizer` currently takes no constructor args. We need to pass a `Map<String, String>` (field name → OpenSearch type like "integer", "keyword", "text") so `tryConvertToDsl` can decide whether LIKE is safe to push. `TransportTrinoSqlAction` already has `TableInfo` with field types at line 139.

**Step 1: Write failing test for field-type-aware optimizer**

```java
// In PlanOptimizerTest.java, update the optimizer instance in the class
// and add a new test that uses field types
@Test
@DisplayName("Pushes <> predicate as must_not term query")
void pushNotEqual() {
    PlanOptimizer typed = new PlanOptimizer(Map.of("AdvEngineID", "integer"));
    TableScanNode scan = new TableScanNode("hits", List.of("AdvEngineID"));
    FilterNode filter = new FilterNode(scan, "AdvEngineID <> 0");

    DqePlanNode result = typed.optimize(filter);

    assertInstanceOf(TableScanNode.class, result);
    TableScanNode optimized = (TableScanNode) result;
    assertNotNull(optimized.getDslFilter());
    // Should be {"bool":{"must_not":[{"term":{"AdvEngineID":0}}]}}
    assertTrue(optimized.getDslFilter().contains("must_not"));
}
```

**Step 2: Run test to verify it fails**

```bash
./gradlew :dqe:test --tests "*PlanOptimizerTest*" -x spotlessCheck
```
Expected: FAIL — `PlanOptimizer` has no constructor accepting field types.

**Step 3: Add field type constructor to PlanOptimizer**

In `PlanOptimizer.java`, add:

```java
private final Map<String, String> fieldTypes;

public PlanOptimizer() {
    this.fieldTypes = Map.of();
}

public PlanOptimizer(Map<String, String> fieldTypes) {
    this.fieldTypes = fieldTypes;
}
```

**Step 4: Wire field types in TransportTrinoSqlAction**

At line ~122 of `TransportTrinoSqlAction.java`, change:

```java
// Before:
PlanOptimizer optimizer = new PlanOptimizer();

// After:
String indexName = findIndexName(plan);
TableInfo tableInfo = metadata.getTableInfo(indexName);
Map<String, String> fieldTypeMap = tableInfo.columns().stream()
    .collect(Collectors.toMap(
        TableInfo.ColumnInfo::name, TableInfo.ColumnInfo::openSearchType));
PlanOptimizer optimizer = new PlanOptimizer(fieldTypeMap);
```

Note: Move the `tableInfo` resolution before the optimizer call. The existing `tableInfo` resolution at line 139 can be deduplicated.

**Step 5: Run test to verify compilation passes (the <> test will still fail — that's Task 2)**

```bash
./gradlew :dqe:compileJava :dqe:compileTestJava -x spotlessCheck
```
Expected: PASS (compiles)

**Step 6: Commit**

```bash
git add dqe/src/main/java/org/opensearch/sql/dqe/planner/optimizer/PlanOptimizer.java \
        dqe/src/main/java/org/opensearch/sql/dqe/coordinator/transport/TransportTrinoSqlAction.java \
        dqe/src/test/java/org/opensearch/sql/dqe/planner/optimizer/PlanOptimizerTest.java
git commit -m "feat(dqe): add field type map to PlanOptimizer"
```

---

### Task 2: Replace Regex tryConvertToDsl with AST-Based Converter

**Files:**
- Modify: `dqe/src/main/java/org/opensearch/sql/dqe/planner/optimizer/PlanOptimizer.java`
- Modify: `dqe/src/test/java/org/opensearch/sql/dqe/planner/optimizer/PlanOptimizerTest.java`

**Context:** Replace the regex-based `tryConvertToDsl(String)` with `tryConvertToDsl(String, Map<String, String>)` that parses the predicate into a Trino `Expression` AST using `DqeSqlParser`, then recursively converts each node to DSL JSON. Returns `null` if any sub-expression is not convertible.

**Step 1: Write failing tests for all new predicate types**

Add these tests to the `PredicatePushdown` nested class:

```java
@Test
@DisplayName("Pushes <> predicate as must_not term query")
void pushNotEqual() {
    PlanOptimizer typed = new PlanOptimizer(Map.of("AdvEngineID", "integer"));
    TableScanNode scan = new TableScanNode("hits", List.of("AdvEngineID"));
    FilterNode filter = new FilterNode(scan, "AdvEngineID <> 0");
    DqePlanNode result = typed.optimize(filter);
    assertInstanceOf(TableScanNode.class, result);
    assertTrue(((TableScanNode) result).getDslFilter().contains("must_not"));
}

@Test
@DisplayName("Pushes >= range predicate")
void pushGreaterOrEqual() {
    PlanOptimizer typed = new PlanOptimizer(Map.of("EventDate", "date"));
    TableScanNode scan = new TableScanNode("hits", List.of("EventDate"));
    FilterNode filter = new FilterNode(scan, "EventDate >= DATE '2013-07-01'");
    DqePlanNode result = typed.optimize(filter);
    assertInstanceOf(TableScanNode.class, result);
    assertTrue(((TableScanNode) result).getDslFilter().contains("range"));
    assertTrue(((TableScanNode) result).getDslFilter().contains("gte"));
}

@Test
@DisplayName("Pushes compound AND predicate as bool must")
void pushAnd() {
    PlanOptimizer typed = new PlanOptimizer(
        Map.of("CounterID", "integer", "IsRefresh", "short"));
    TableScanNode scan = new TableScanNode("hits", List.of("CounterID", "IsRefresh"));
    FilterNode filter = new FilterNode(scan, "CounterID = 62 AND IsRefresh = 0");
    DqePlanNode result = typed.optimize(filter);
    assertInstanceOf(TableScanNode.class, result);
    String dsl = ((TableScanNode) result).getDslFilter();
    assertTrue(dsl.contains("bool"));
    assertTrue(dsl.contains("must"));
}

@Test
@DisplayName("Pushes LIKE on keyword field as wildcard query")
void pushLikeKeyword() {
    PlanOptimizer typed = new PlanOptimizer(Map.of("URL", "keyword"));
    TableScanNode scan = new TableScanNode("hits", List.of("URL"));
    FilterNode filter = new FilterNode(scan, "URL LIKE '%google%'");
    DqePlanNode result = typed.optimize(filter);
    assertInstanceOf(TableScanNode.class, result);
    assertTrue(((TableScanNode) result).getDslFilter().contains("wildcard"));
}

@Test
@DisplayName("Does NOT push LIKE on text field")
void rejectLikeOnText() {
    PlanOptimizer typed = new PlanOptimizer(Map.of("SocialAction", "text"));
    TableScanNode scan = new TableScanNode("hits", List.of("SocialAction"));
    FilterNode filter = new FilterNode(scan, "SocialAction LIKE '%test%'");
    DqePlanNode result = typed.optimize(filter);
    // FilterNode should remain — LIKE on text is not pushable
    assertInstanceOf(FilterNode.class, result);
}

@Test
@DisplayName("Pushes IN predicate as terms query")
void pushIn() {
    PlanOptimizer typed = new PlanOptimizer(Map.of("TraficSourceID", "integer"));
    TableScanNode scan = new TableScanNode("hits", List.of("TraficSourceID"));
    FilterNode filter = new FilterNode(scan, "TraficSourceID IN (-1, 6)");
    DqePlanNode result = typed.optimize(filter);
    assertInstanceOf(TableScanNode.class, result);
    assertTrue(((TableScanNode) result).getDslFilter().contains("terms"));
}

@Test
@DisplayName("Existing equality tests still pass with no-arg constructor")
void backwardCompatEquality() {
    // Verify existing no-arg constructor still works (empty field types)
    PlanOptimizer noTypes = new PlanOptimizer();
    TableScanNode scan = new TableScanNode("logs", List.of("status"));
    FilterNode filter = new FilterNode(scan, "status = 200");
    DqePlanNode result = noTypes.optimize(filter);
    assertInstanceOf(TableScanNode.class, result);
    assertEquals("{\"term\":{\"status\":200}}", ((TableScanNode) result).getDslFilter());
}
```

**Step 2: Run tests to verify they fail**

```bash
./gradlew :dqe:test --tests "*PlanOptimizerTest*" -x spotlessCheck
```
Expected: Most new tests FAIL — `tryConvertToDsl` only handles `column = value`.

**Step 3: Implement AST-based tryConvertToDsl**

Replace the body of `tryConvertToDsl` and add the recursive converter. The key change: parse the predicate string with `DqeSqlParser`, then walk the Trino AST recursively.

```java
import io.trino.sql.tree.BetweenPredicate;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.GenericLiteral;
import io.trino.sql.tree.InListExpression;
import io.trino.sql.tree.InPredicate;
import io.trino.sql.tree.LikePredicate;
import io.trino.sql.tree.LogicalExpression;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.DoubleLiteral;
import io.trino.sql.tree.DecimalLiteral;
import io.trino.sql.tree.NotExpression;
import io.trino.sql.tree.StringLiteral;

// Replace tryConvertToDsl:
String tryConvertToDsl(String predicateString) {
    try {
        DqeSqlParser parser = new DqeSqlParser();
        Expression expr = parser.parseExpression(predicateString);
        return convertExprToDsl(expr);
    } catch (Exception e) {
        // Fallback: if AST parsing fails, return null (not pushable)
        return null;
    }
}

private String convertExprToDsl(Expression expr) {
    if (expr instanceof ComparisonExpression cmp) {
        return convertComparison(cmp);
    } else if (expr instanceof LogicalExpression logical) {
        return convertLogical(logical);
    } else if (expr instanceof NotExpression not) {
        String inner = convertExprToDsl(not.getValue());
        if (inner == null) return null;
        return "{\"bool\":{\"must_not\":[" + inner + "]}}";
    } else if (expr instanceof LikePredicate like) {
        return convertLike(like);
    } else if (expr instanceof InPredicate in) {
        return convertIn(in);
    } else if (expr instanceof BetweenPredicate between) {
        return convertBetween(between);
    }
    return null; // not pushable
}
```

**Comparison handler:**

```java
private String convertComparison(ComparisonExpression cmp) {
    String column = extractColumnName(cmp.getLeft());
    if (column == null) return null;
    String value = extractLiteralValue(cmp.getRight());
    if (value == null) return null;

    switch (cmp.getOperator()) {
        case EQUAL:
            return buildTermDsl(column, value);
        case NOT_EQUAL:
            String term = buildTermDsl(column, value);
            if (term == null) return null;
            return "{\"bool\":{\"must_not\":[" + term + "]}}";
        case GREATER_THAN:
            return "{\"range\":{\"" + column + "\":{\"gt\":" + value + "}}}";
        case GREATER_THAN_OR_EQUAL:
            return "{\"range\":{\"" + column + "\":{\"gte\":" + value + "}}}";
        case LESS_THAN:
            return "{\"range\":{\"" + column + "\":{\"lt\":" + value + "}}}";
        case LESS_THAN_OR_EQUAL:
            return "{\"range\":{\"" + column + "\":{\"lte\":" + value + "}}}";
        default:
            return null;
    }
}
```

**Logical AND/OR handler:**

```java
private String convertLogical(LogicalExpression logical) {
    List<String> clauses = new ArrayList<>();
    for (Expression term : logical.getTerms()) {
        String dsl = convertExprToDsl(term);
        if (dsl == null) return null; // ALL must be pushable
        clauses.add(dsl);
    }
    String joined = String.join(",", clauses);
    switch (logical.getOperator()) {
        case AND:
            return "{\"bool\":{\"must\":[" + joined + "]}}";
        case OR:
            return "{\"bool\":{\"should\":[" + joined + "]}}";
        default:
            return null;
    }
}
```

**LIKE handler (keyword only):**

```java
private String convertLike(LikePredicate like) {
    String column = extractColumnName(like.getValue());
    if (column == null) return null;
    // Only push LIKE on keyword fields
    String osType = fieldTypes.getOrDefault(column, "keyword");
    if ("text".equals(osType)) return null;
    if (!(like.getPattern() instanceof StringLiteral pattern)) return null;
    // Convert SQL LIKE pattern (% → *, _ → ?) to wildcard
    String wildcardPattern = pattern.getValue().replace('%', '*').replace('_', '?');
    return "{\"wildcard\":{\"" + column + "\":{\"value\":\""
        + escapeJsonString(wildcardPattern) + "\"}}}";
}
```

**IN handler:**

```java
private String convertIn(InPredicate in) {
    String column = extractColumnName(in.getValue());
    if (column == null) return null;
    if (!(in.getValueList() instanceof InListExpression list)) return null;
    List<String> values = new ArrayList<>();
    for (Expression val : list.getValues()) {
        String v = extractLiteralValue(val);
        if (v == null) return null;
        values.add(v);
    }
    return "{\"terms\":{\"" + column + "\":[" + String.join(",", values) + "]}}";
}
```

**BETWEEN handler:**

```java
private String convertBetween(BetweenPredicate between) {
    String column = extractColumnName(between.getValue());
    if (column == null) return null;
    String min = extractLiteralValue(between.getMin());
    String max = extractLiteralValue(between.getMax());
    if (min == null || max == null) return null;
    return "{\"range\":{\"" + column + "\":{\"gte\":" + min + ",\"lte\":" + max + "}}}";
}
```

**Helpers:**

```java
private static String extractColumnName(Expression expr) {
    if (expr instanceof Identifier id) return id.getValue();
    return null;
}

private static String extractLiteralValue(Expression expr) {
    if (expr instanceof LongLiteral lit) return String.valueOf(lit.getValue());
    if (expr instanceof DoubleLiteral lit) return String.valueOf(lit.getValue());
    if (expr instanceof DecimalLiteral lit) return lit.getValue();
    if (expr instanceof StringLiteral lit)
        return "\"" + escapeJsonString(lit.getValue()) + "\"";
    if (expr instanceof GenericLiteral lit && lit.getType().equalsIgnoreCase("DATE")) {
        // Convert DATE '2013-07-01' to epoch millis
        long epochMillis = java.time.LocalDate.parse(lit.getValue())
            .atStartOfDay(java.time.ZoneOffset.UTC).toInstant().toEpochMilli();
        return String.valueOf(epochMillis);
    }
    return null;
}

private static String buildTermDsl(String column, String value) {
    return "{\"term\":{\"" + column + "\":" + value + "}}";
}
```

**Step 4: Run tests to verify they pass**

```bash
./gradlew :dqe:test --tests "*PlanOptimizerTest*" -x spotlessCheck
```
Expected: ALL PASS including both new and existing tests.

**Step 5: Commit**

```bash
git commit -m "feat(dqe): AST-based predicate pushdown for <>, range, AND, LIKE, IN"
```

---

### Task 3: Add Wildcard Query to LuceneQueryCompiler

**Files:**
- Modify: `dqe/src/main/java/org/opensearch/sql/dqe/shard/source/LuceneQueryCompiler.java`
- Modify: `dqe/src/test/java/org/opensearch/sql/dqe/shard/source/LuceneQueryCompilerTest.java`

**Context:** The DSL `{"wildcard":{"URL":{"value":"*google*"}}}` needs to compile to a Lucene `WildcardQuery`. The compiler already handles term, range, terms, bool — just need to add the wildcard case.

**Step 1: Write failing test**

```java
@Test
@DisplayName("wildcard query compiles to WildcardQuery")
void wildcardQuery() {
    LuceneQueryCompiler compiler = new LuceneQueryCompiler(fieldTypes);
    String dsl = "{\"wildcard\":{\"URL\":{\"value\":\"*google*\"}}}";
    Query q = compiler.compile(dsl);
    assertInstanceOf(WildcardQuery.class, q);
    WildcardQuery wq = (WildcardQuery) q;
    assertEquals("URL", wq.getTerm().field());
    assertEquals("*google*", wq.getTerm().text());
}
```

**Step 2: Run test, verify failure**

```bash
./gradlew :dqe:test --tests "*LuceneQueryCompilerTest*" -x spotlessCheck
```

**Step 3: Implement wildcard in LuceneQueryCompiler**

In `compileNode()`, add:
```java
} else if (node.has("wildcard")) {
    return compileWildcard(node.get("wildcard"));
}
```

Add the method:
```java
private Query compileWildcard(JsonNode wildcardNode) {
    Iterator<String> fields = wildcardNode.fieldNames();
    if (!fields.hasNext()) return new MatchAllDocsQuery();
    String field = fields.next();
    JsonNode config = wildcardNode.get(field);
    String pattern = config.has("value") ? config.get("value").asText() : config.asText();
    return new WildcardQuery(new Term(field, pattern));
}
```

Add import:
```java
import org.apache.lucene.search.WildcardQuery;
```

**Step 4: Run test, verify pass**

```bash
./gradlew :dqe:test --tests "*LuceneQueryCompilerTest*" -x spotlessCheck
```

**Step 5: Commit**

```bash
git commit -m "feat(dqe): add wildcard query support to LuceneQueryCompiler"
```

---

### Task 4: Integration — Build, Deploy, Correctness

**Files:** None new — verification only.

**Step 1: Run all DQE unit tests**

```bash
./gradlew :dqe:test -x spotlessCheck
```
Expected: All pass (pre-existing DqeSettings failures are unrelated).

**Step 2: Build plugin**

```bash
./gradlew :opensearch-sql-plugin:bundlePlugin -x test -x integTest -x spotlessCheck
```

**Step 3: Reload plugin**

```bash
cd /home/ec2-user/oss/wukong/benchmarks/clickbench
bash ./run/run_all.sh reload-plugin
```

**Step 4: Run full correctness suite**

```bash
bash ./run/run_all.sh correctness
```
Target: 43/43 PASS (no regression).

**Step 5: Debug any failures**

Common issues:
- DATE literal conversion: epoch millis might be off-by-one-day due to timezone. Check if OpenSearch stores dates as UTC epoch millis.
- LIKE wildcard escaping: special characters in URL patterns may need escaping.
- Compound predicate with non-pushable sub-expression: should fall back to FilterNode entirely.

**Step 6: Commit any fixes**

```bash
git commit -m "fix(dqe): correctness fixes for expanded predicate pushdown"
```

---

### Task 5: Perf-Lite — Measure Improvement

**Files:** Results in `benchmarks/clickbench/results/perf-lite/`

**Step 1: Run perf-lite**

```bash
bash ./run/run_all.sh perf-lite
```

**Step 2: Analyze results**

```bash
python3 -c "
import json
with open('results/perf-lite/summary.json') as f:
    data = json.load(f)
for q in data['queries']:
    ch = min(q['ch_times'])
    os = min(q['os_times'])
    r = os/ch if ch > 0 else 9999
    status = 'OK' if r < 5 else 'WARN' if r < 10 else 'SLOW'
    print(f'Q{q[\"q\"]:>2}: {os:.3f}s / {ch:.3f}s = {r:.1f}x  {status}')
"
```

**Step 3: Compare with baseline**

Key queries to watch:
- Q21 (`LIKE '%google%'`): expect 5.6s → <0.5s
- Q37-Q41 (compound WHERE): expect 4-12s → <2s
- Q2 (`<> 0`): expect 0.072s → ~0.06s

**Step 4: Commit results**

```bash
git add benchmarks/clickbench/results/
git commit -m "perf(dqe): perf-lite after predicate pushdown enhancement"
```

---

## Build Commands Reference

```bash
# Compile check
./gradlew :dqe:compileJava

# Unit tests
./gradlew :dqe:test -x spotlessCheck

# Specific test
./gradlew :dqe:test --tests "*PlanOptimizerTest*" -x spotlessCheck

# Build plugin
./gradlew :opensearch-sql-plugin:bundlePlugin -x test -x integTest -x spotlessCheck

# Reload + correctness
cd /home/ec2-user/oss/wukong/benchmarks/clickbench
bash ./run/run_all.sh reload-plugin
bash ./run/run_all.sh correctness
bash ./run/run_all.sh perf-lite
```
