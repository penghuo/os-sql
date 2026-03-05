# Trino Functions & Operators Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add a vectorized function framework with ~80 standard SQL functions, a function registry with plan-time resolution, and expression evaluation support in all SQL clauses (SELECT, WHERE, GROUP BY, ORDER BY, HAVING).

**Architecture:** A `FunctionRegistry` maps function names+types to implementations. `BlockExpression` is a tree of vectorized evaluators (operating on Trino Blocks/Pages). An `ExpressionCompiler` converts Trino AST `Expression` nodes into `BlockExpression` trees. A new `EvalOperator` handles SELECT-list computed columns. Existing `FilterOperator` and `HashAggregationOperator` are updated to use the framework.

**Tech Stack:** Java 11+, Trino SPI 442 (Page/Block/Type), Trino Parser 442 (AST), JUnit 5, Lombok, Google Java Format (spotless).

**Key paths:**
- Main source: `dqe/src/main/java/org/opensearch/sql/dqe/`
- Test source: `dqe/src/test/java/org/opensearch/sql/dqe/`
- Build: `./gradlew :dqe:test`
- Format: `./gradlew :dqe:spotlessApply`
- Compile check: `./gradlew :dqe:compileJava`

**License header** (required on every new `.java` file):
```java
/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
```

---

### Task 1: FunctionKind enum and ScalarFunctionImplementation interface

**Files:**
- Create: `dqe/src/main/java/org/opensearch/sql/dqe/function/FunctionKind.java`
- Create: `dqe/src/main/java/org/opensearch/sql/dqe/function/scalar/ScalarFunctionImplementation.java`

**Step 1: Create FunctionKind enum**

```java
package org.opensearch.sql.dqe.function;

/** Classifies a registered function by its evaluation mode. */
public enum FunctionKind {
  SCALAR,
  AGGREGATE,
  WINDOW
}
```

**Step 2: Create ScalarFunctionImplementation interface**

```java
package org.opensearch.sql.dqe.function.scalar;

import io.trino.spi.block.Block;

/**
 * Vectorized scalar function contract. Receives input Blocks (one per argument) and the row count,
 * returns an output Block covering all positions.
 */
@FunctionalInterface
public interface ScalarFunctionImplementation {
  Block evaluate(Block[] arguments, int positionCount);
}
```

**Step 3: Verify compilation**

Run: `./gradlew :dqe:compileJava`
Expected: BUILD SUCCESSFUL

**Step 4: Commit**

```bash
git add dqe/src/main/java/org/opensearch/sql/dqe/function/FunctionKind.java \
        dqe/src/main/java/org/opensearch/sql/dqe/function/scalar/ScalarFunctionImplementation.java
git commit -m "feat(dqe): add FunctionKind enum and ScalarFunctionImplementation interface"
```

---

### Task 2: FunctionMetadata, ResolvedFunction, and TypeSignatures

**Files:**
- Create: `dqe/src/main/java/org/opensearch/sql/dqe/function/FunctionMetadata.java`
- Create: `dqe/src/main/java/org/opensearch/sql/dqe/function/ResolvedFunction.java`
- Create: `dqe/src/main/java/org/opensearch/sql/dqe/common/types/TypeSignatures.java`
- Create: `dqe/src/test/java/org/opensearch/sql/dqe/function/ResolvedFunctionTest.java`

**Step 1: Create FunctionMetadata**

A Lombok `@Builder` class with fields: `name`, `argumentTypes` (List of Type), `returnType` (Type), `kind` (FunctionKind), `deterministic` (boolean, default true), `nullable` (boolean, default true), `scalarImplementation` (ScalarFunctionImplementation, nullable).

**Step 2: Create TypeSignatures utility**

Maps Trino type signature strings (e.g. "bigint", "varchar", "timestamp(3)") back to their `Type` singleton instances. Used by `ResolvedFunction` deserialization.

**Step 3: Create ResolvedFunction with Writeable serialization**

Implements `org.opensearch.core.common.io.stream.Writeable`. Serializes as: name (string) + argument type signatures (string list) + return type signature (string) + kind (enum). Shards deserialize and re-resolve from their local `FunctionRegistry`. Implements `equals`/`hashCode` based on name + argument types + kind.

**Step 4: Write tests for round-trip serialization and equality**

Test that `writeTo` → `StreamInput` → constructor produces an equal `ResolvedFunction`.

**Step 5: Run tests**

Run: `./gradlew :dqe:spotlessApply && ./gradlew :dqe:test --tests "org.opensearch.sql.dqe.function.ResolvedFunctionTest"`
Expected: PASS

**Step 6: Commit**

```bash
git commit -m "feat(dqe): add FunctionMetadata, ResolvedFunction, and TypeSignatures"
```

---

### Task 3: FunctionRegistry with resolution and coercion

**Files:**
- Create: `dqe/src/main/java/org/opensearch/sql/dqe/function/FunctionRegistry.java`
- Create: `dqe/src/test/java/org/opensearch/sql/dqe/function/FunctionRegistryTest.java`

**Step 1: Write the failing test**

Tests for:
- Exact match resolution (resolve "upper" with VARCHAR arg)
- Case-insensitive lookup ("UPPER" resolves same as "upper")
- Implicit coercion: INTEGER arg matches BIGINT parameter
- Implicit coercion: BIGINT arg matches DOUBLE parameter
- Prefers exact match over coercion when both overloads exist
- Throws for unknown function name
- Throws when no overload matches argument types

**Step 2: Run test to verify it fails**

Run: `./gradlew :dqe:test --tests "org.opensearch.sql.dqe.function.FunctionRegistryTest"`
Expected: FAIL (class not found)

**Step 3: Implement FunctionRegistry**

- Internal state: `Map<String, List<FunctionMetadata>>` keyed by lowercase name
- `register(FunctionMetadata)`: adds to the overload list
- `resolve(String name, List<Type> argumentTypes)`: iterates overloads, counts coercions needed, picks best match (fewest coercions, 0 = exact)
- Coercion rules: TINYINT/SMALLINT/INTEGER -> BIGINT, BIGINT -> DOUBLE, REAL -> DOUBLE
- `getMetadata(ResolvedFunction)`: retrieves full metadata (including implementation) for a resolved reference

**Step 4: Run tests**

Run: `./gradlew :dqe:spotlessApply && ./gradlew :dqe:test --tests "org.opensearch.sql.dqe.function.FunctionRegistryTest"`
Expected: PASS

**Step 5: Commit**

```bash
git commit -m "feat(dqe): add FunctionRegistry with overload resolution and implicit coercion"
```

---

### Task 4: BlockExpression interface and leaf expressions

**Files:**
- Create: `dqe/src/main/java/org/opensearch/sql/dqe/function/expression/BlockExpression.java`
- Create: `dqe/src/main/java/org/opensearch/sql/dqe/function/expression/ColumnReference.java`
- Create: `dqe/src/main/java/org/opensearch/sql/dqe/function/expression/ConstantExpression.java`
- Create: `dqe/src/test/java/org/opensearch/sql/dqe/function/expression/ColumnReferenceTest.java`
- Create: `dqe/src/test/java/org/opensearch/sql/dqe/function/expression/ConstantExpressionTest.java`

**Step 1: Write failing tests**

ColumnReference tests:
- Read BIGINT column from a Page
- Read VARCHAR column from a Page
- Null positions return null in output Block

ConstantExpression tests:
- Produce BIGINT constant Block across all positions
- Produce VARCHAR constant Block
- Produce all-null Block for null constant

Use `BlockBuilder` pattern from existing `ExpressionEvaluatorTest` to build test Pages.

**Step 2: Implement BlockExpression interface**

```java
public interface BlockExpression {
  Block evaluate(Page page);
  Type getType();
}
```

**Step 3: Implement ColumnReference**

Returns `page.getBlock(columnIndex)` directly (zero-copy).

**Step 4: Implement ConstantExpression**

Uses `RunLengthEncodedBlock.create(singleValueBlock, positionCount)` for efficient constant representation. Builds a single-position Block containing the constant value, then wraps it in RLE.

**Step 5: Run tests, format, commit**

```bash
git commit -m "feat(dqe): add BlockExpression interface with ColumnReference and ConstantExpression"
```

---

### Task 5: ComparisonBlockExpression and ArithmeticBlockExpression

**Files:**
- Create: `dqe/src/main/java/org/opensearch/sql/dqe/function/expression/ComparisonBlockExpression.java`
- Create: `dqe/src/main/java/org/opensearch/sql/dqe/function/expression/ArithmeticBlockExpression.java`
- Create: `dqe/src/test/java/org/opensearch/sql/dqe/function/expression/ComparisonBlockExpressionTest.java`
- Create: `dqe/src/test/java/org/opensearch/sql/dqe/function/expression/ArithmeticBlockExpressionTest.java`

**Step 1: Write failing tests**

Comparison tests: BIGINT `=`, `<>`, `<`, `<=`, `>`, `>=` producing BOOLEAN Blocks. VARCHAR comparisons. DOUBLE comparisons. NULL propagation (either side null -> output null).

Arithmetic tests: BIGINT `+`, `-`, `*`, `/`, `%`. DOUBLE arithmetic. Mixed BIGINT+DOUBLE promotion. NULL propagation.

**Step 2: Implement ComparisonBlockExpression**

Takes `ComparisonExpression.Operator`, two child `BlockExpression`s. Evaluates children, loops over positions, reads values, writes BOOLEAN result. Handles nulls.

**Step 3: Implement ArithmeticBlockExpression**

Takes `ArithmeticBinaryExpression.Operator`, two children, output Type. Similar loop with type promotion.

**Step 4: Run tests, format, commit**

```bash
git commit -m "feat(dqe): add vectorized ComparisonBlockExpression and ArithmeticBlockExpression"
```

---

### Task 6: Logical expressions (And, Or, Not) and IsNull

**Files:**
- Create: `dqe/src/main/java/org/opensearch/sql/dqe/function/expression/LogicalAndExpression.java`
- Create: `dqe/src/main/java/org/opensearch/sql/dqe/function/expression/LogicalOrExpression.java`
- Create: `dqe/src/main/java/org/opensearch/sql/dqe/function/expression/NotBlockExpression.java`
- Create: `dqe/src/main/java/org/opensearch/sql/dqe/function/expression/IsNullExpression.java`
- Create: `dqe/src/test/java/org/opensearch/sql/dqe/function/expression/LogicalExpressionsTest.java`

**Step 1: Write failing tests**

- AND: true+true=true, true+false=false, true+null=null, false+null=false
- OR: false+false=false, true+false=true, false+null=null, true+null=true
- NOT: true->false, false->true, null->null
- IS NULL: null->true, non-null->false
- IS NOT NULL: null->false, non-null->true

All tests operate on BOOLEAN Blocks with mixed null/non-null positions.

**Step 2: Implement all four expression types**

Each follows SQL three-valued NULL logic, operating on entire Blocks.

**Step 3: Run tests, format, commit**

```bash
git commit -m "feat(dqe): add vectorized logical expressions (AND, OR, NOT, IS NULL)"
```

---

### Task 7: ScalarFunctionExpression and ExpressionCompiler (basic)

**Files:**
- Create: `dqe/src/main/java/org/opensearch/sql/dqe/function/expression/ScalarFunctionExpression.java`
- Create: `dqe/src/main/java/org/opensearch/sql/dqe/function/expression/ExpressionCompiler.java`
- Create: `dqe/src/test/java/org/opensearch/sql/dqe/function/expression/ExpressionCompilerTest.java`

**Step 1: Write failing tests**

Tests for `ExpressionCompiler.compile(Expression)`:
- `Identifier` -> ColumnReference
- `LongLiteral` -> ConstantExpression BIGINT
- `DoubleLiteral` -> ConstantExpression DOUBLE
- `StringLiteral` -> ConstantExpression VARCHAR
- `BooleanLiteral` -> ConstantExpression BOOLEAN
- `NullLiteral` -> ConstantExpression null
- `ComparisonExpression` -> ComparisonBlockExpression
- `ArithmeticBinaryExpression` -> ArithmeticBlockExpression
- `LogicalExpression(AND)` -> LogicalAndExpression
- `LogicalExpression(OR)` -> LogicalOrExpression
- `NotExpression` -> NotBlockExpression
- `FunctionCall("upper", [Identifier])` -> ScalarFunctionExpression

Use `DqeSqlParser.parseExpression()` to build AST nodes, compile them, and evaluate against test Pages.

**Step 2: Implement ScalarFunctionExpression**

Takes `ScalarFunctionImplementation`, child `BlockExpression` list, and return Type. Evaluates children to get argument Blocks, delegates to implementation.

**Step 3: Implement ExpressionCompiler**

Constructor takes `FunctionRegistry`, `columnIndexMap`, `columnTypeMap`. `compile(Expression)` method pattern-matches on AST node type (same instanceof chain as `ExpressionEvaluator`) but returns `BlockExpression` tree nodes. For `FunctionCall`, resolves via registry and wraps in `ScalarFunctionExpression`.

**Step 4: Run tests, format, commit**

```bash
git commit -m "feat(dqe): add ExpressionCompiler and ScalarFunctionExpression"
```

---

### Task 8: Conditional and pattern expressions (CAST, CASE, COALESCE, NULLIF, LIKE, IN)

**Files:**
- Create: `dqe/src/main/java/org/opensearch/sql/dqe/function/expression/CastBlockExpression.java`
- Create: `dqe/src/main/java/org/opensearch/sql/dqe/function/expression/CaseBlockExpression.java`
- Create: `dqe/src/main/java/org/opensearch/sql/dqe/function/expression/CoalesceBlockExpression.java`
- Create: `dqe/src/main/java/org/opensearch/sql/dqe/function/expression/NullIfBlockExpression.java`
- Create: `dqe/src/main/java/org/opensearch/sql/dqe/function/expression/LikeBlockExpression.java`
- Create: `dqe/src/main/java/org/opensearch/sql/dqe/function/expression/InBlockExpression.java`
- Create: `dqe/src/test/java/org/opensearch/sql/dqe/function/expression/ConditionalExpressionsTest.java`

**Step 1: Write failing tests**

- `CAST(42 AS DOUBLE)` -> DOUBLE Block with 42.0
- `CAST('123' AS BIGINT)` -> BIGINT Block with 123
- `CASE WHEN status > 200 THEN 'error' ELSE 'ok' END` -> VARCHAR Block
- `COALESCE(null_col, default_col)` -> first non-null
- `NULLIF(a, b)` -> null if equal
- `x LIKE '%error%'` -> BOOLEAN Block (compile pattern to regex)
- `status IN (200, 404, 500)` -> BOOLEAN Block

**Step 2: Implement each expression type**

Each follows the `BlockExpression` contract:
- `CastBlockExpression`: type conversion with supported paths (VARCHAR<->numeric, numeric widening)
- `CaseBlockExpression`: evaluates WHEN conditions in order, picks first matching THEN
- `CoalesceBlockExpression`: evaluates arguments left-to-right, picks first non-null
- `NullIfBlockExpression`: compares two values, returns null if equal
- `LikeBlockExpression`: compiles SQL LIKE pattern to `java.util.regex.Pattern` at construction
- `InBlockExpression`: checks value membership in a set

**Step 3: Add handling to ExpressionCompiler**

Handle AST node types: `Cast`, `SearchedCaseExpression`, `CoalesceExpression`, `InPredicate`, `LikePredicate`, `NullIfExpression`, `IsNullPredicate`, `IsNotNullPredicate`.

**Step 4: Run tests, format, commit**

```bash
git commit -m "feat(dqe): add CAST, CASE, COALESCE, NULLIF, LIKE, IN vectorized expressions"
```

---

### Task 9: String scalar functions

**Files:**
- Create: `dqe/src/main/java/org/opensearch/sql/dqe/function/scalar/StringFunctions.java`
- Create: `dqe/src/test/java/org/opensearch/sql/dqe/function/scalar/StringFunctionsTest.java`

**Step 1: Write failing tests**

Test each function with normal input, NULL input, and edge cases:
- `upper("hello")` -> "HELLO", `upper(null)` -> null
- `lower("HELLO")` -> "hello"
- `length("hello")` -> 5, `length("")` -> 0
- `concat("a", "b")` -> "ab"
- `substring("hello", 2, 3)` -> "ell" (1-indexed)
- `trim("  hi  ")` -> "hi"
- `replace("aabb", "aa", "cc")` -> "ccbb"
- `position("world", "lo")` -> 4
- `starts_with("hello", "hel")` -> true
- `reverse("abc")` -> "cba"

Each test builds a VARCHAR Block, calls the implementation's `evaluate(Block[], positionCount)`, and asserts output Block values.

**Step 2: Implement StringFunctions**

A class with static methods returning `ScalarFunctionImplementation`. Each implementation:
1. Reads `Slice` values from input Block using `VarcharType.VARCHAR.getSlice(block, pos)`
2. Performs the string operation via Java's `String` methods
3. Writes result to `BlockBuilder` using `VarcharType.VARCHAR.writeSlice(builder, Slices.utf8Slice(result))`
4. Handles NULLs: if any input is null, writes null to output

Functions: `upper`, `lower`, `length`, `concat`, `substring`, `trim`, `ltrim`, `rtrim`, `replace`, `position`, `reverse`, `lpad`, `rpad`, `starts_with`, `regexp_like`, `regexp_extract`, `chr`, `codepoint`, `split`.

**Step 3: Run tests, format, commit**

```bash
git commit -m "feat(dqe): add vectorized string scalar functions"
```

---

### Task 10: Math scalar functions

**Files:**
- Create: `dqe/src/main/java/org/opensearch/sql/dqe/function/scalar/MathFunctions.java`
- Create: `dqe/src/test/java/org/opensearch/sql/dqe/function/scalar/MathFunctionsTest.java`

**Step 1: Write failing tests**

- `abs(-5.0)` -> 5.0, `abs(null)` -> null
- `ceil(3.2)` -> 4, `floor(3.8)` -> 3
- `round(3.14159, 2)` -> 3.14
- `power(2.0, 10.0)` -> 1024.0
- `sqrt(16.0)` -> 4.0
- `ln(1.0)` -> 0.0
- `mod(10, 3)` -> 1
- `sign(-5.0)` -> -1.0, `sign(0.0)` -> 0.0
- `pi()` -> 3.14159..., `e()` -> 2.71828...

**Step 2: Implement MathFunctions**

Static factory methods returning `ScalarFunctionImplementation`. Most delegate to `java.lang.Math`. Null propagation follows the same pattern as string functions.

Functions: `abs`, `ceil`/`ceiling`, `floor`, `round`, `truncate`, `power`/`pow`, `sqrt`, `cbrt`, `exp`, `ln`, `log2`, `log10`, `log`, `mod`, `sign`, `pi`, `e`, `random`, `radians`, `degrees`.

**Step 3: Run tests, format, commit**

```bash
git commit -m "feat(dqe): add vectorized math scalar functions"
```

---

### Task 11: Date/time scalar functions

**Files:**
- Create: `dqe/src/main/java/org/opensearch/sql/dqe/function/scalar/DateTimeFunctions.java`
- Create: `dqe/src/test/java/org/opensearch/sql/dqe/function/scalar/DateTimeFunctionsTest.java`

**Step 1: Write failing tests**

Trino `TIMESTAMP(3)` stores epoch micros (millis * 1000) as `long` in Blocks via `TimestampType.TIMESTAMP_MILLIS`.

- `year(timestamp)` -> extract year field
- `month(timestamp)` -> extract month
- `day(timestamp)` -> extract day
- `hour(timestamp)`, `minute(timestamp)`, `second(timestamp)`
- `date_trunc('day', timestamp)` -> truncate to midnight
- `from_unixtime(epoch_seconds)` -> TIMESTAMP
- `to_unixtime(timestamp)` -> DOUBLE
- `now()` -> current timestamp (test returns non-zero)
- NULL propagation for all

**Step 2: Implement DateTimeFunctions**

Uses `java.time.Instant`, `java.time.ZonedDateTime`, `java.time.ZoneOffset.UTC` to convert epoch micros to calendar fields. Division by 1000 to get millis from Trino's microsecond storage.

Functions: `current_timestamp`/`now`, `current_date`, `year`, `month`, `day`, `hour`, `minute`, `second`, `day_of_week`/`dow`, `day_of_year`/`doy`, `date_trunc`, `date_format`, `date_parse`, `date_add`, `date_diff`, `from_unixtime`, `to_unixtime`.

**Step 3: Run tests, format, commit**

```bash
git commit -m "feat(dqe): add vectorized date/time scalar functions"
```

---

### Task 12: Trigonometric scalar functions

**Files:**
- Create: `dqe/src/main/java/org/opensearch/sql/dqe/function/scalar/TrigFunctions.java`
- Create: `dqe/src/test/java/org/opensearch/sql/dqe/function/scalar/TrigFunctionsTest.java`

**Step 1: Write failing tests**

- `sin(0)` -> 0.0, `cos(0)` -> 1.0, `tan(0)` -> 0.0
- `asin(0)` -> 0.0, `acos(1)` -> 0.0, `atan(0)` -> 0.0
- `atan2(1, 1)` -> pi/4

**Step 2: Implement TrigFunctions**

Thin wrappers around `java.lang.Math.sin()`, `Math.cos()`, etc. All follow the same Block-level null-propagating pattern.

**Step 3: Run tests, format, commit**

```bash
git commit -m "feat(dqe): add trigonometric scalar functions"
```

---

### Task 13: BuiltinFunctions — register all functions in the registry

**Files:**
- Create: `dqe/src/main/java/org/opensearch/sql/dqe/function/BuiltinFunctions.java`
- Create: `dqe/src/test/java/org/opensearch/sql/dqe/function/BuiltinFunctionsTest.java`

**Step 1: Write failing test**

```java
@Test
void registersAllBuiltins() {
  FunctionRegistry registry = BuiltinFunctions.createRegistry();
  // Spot-check key functions resolve without error
  assertDoesNotThrow(() -> registry.resolve("upper", List.of(VarcharType.VARCHAR)));
  assertDoesNotThrow(() -> registry.resolve("abs", List.of(DoubleType.DOUBLE)));
  assertDoesNotThrow(() -> registry.resolve("year", List.of(TimestampType.TIMESTAMP_MILLIS)));
  assertDoesNotThrow(() -> registry.resolve("sin", List.of(DoubleType.DOUBLE)));
  assertDoesNotThrow(() -> registry.resolve("concat", List.of(VarcharType.VARCHAR, VarcharType.VARCHAR)));
}
```

**Step 2: Implement BuiltinFunctions**

Static `createRegistry()` method that registers every scalar function from `StringFunctions`, `MathFunctions`, `DateTimeFunctions`, and `TrigFunctions`. Each registration is a `FunctionMetadata.builder()...build()` call.

**Step 3: Run tests, format, commit**

```bash
git commit -m "feat(dqe): add BuiltinFunctions registry initialization"
```

---

### Task 14: EvalOperator (physical operator for computed columns)

**Files:**
- Create: `dqe/src/main/java/org/opensearch/sql/dqe/operator/EvalOperator.java`
- Create: `dqe/src/test/java/org/opensearch/sql/dqe/operator/EvalOperatorTest.java`

**Step 1: Write failing test**

```java
@Test
void computesExpressions() {
  // Input: col0 = [10, 20, 30]
  // Expression: col0 * 2
  // Output: single column with [20, 40, 60]
}

@Test
void multipleExpressions() {
  // Input: col0 = [10, 20]
  // Expressions: [col0, col0 + 1]
  // Output: two columns [[10,20], [11,21]]
}

@Test
void returnsNullWhenSourceExhausted() {
  // Empty source -> null
}
```

**Step 2: Implement EvalOperator**

```java
public class EvalOperator implements Operator {
  private final Operator source;
  private final List<BlockExpression> outputExpressions;

  public Page processNextBatch() {
    Page input = source.processNextBatch();
    if (input == null) return null;
    Block[] blocks = new Block[outputExpressions.size()];
    for (int i = 0; i < outputExpressions.size(); i++) {
      blocks[i] = outputExpressions.get(i).evaluate(input);
    }
    return new Page(blocks);
  }
}
```

**Step 3: Run tests, format, commit**

```bash
git commit -m "feat(dqe): add EvalOperator for SELECT-list computed columns"
```

---

### Task 15: Update FilterOperator to accept BlockExpression

**Files:**
- Modify: `dqe/src/main/java/org/opensearch/sql/dqe/operator/FilterOperator.java`
- Modify: `dqe/src/test/java/org/opensearch/sql/dqe/operator/FilterOperatorTest.java`

**Step 1: Add second constructor accepting BlockExpression**

Keep existing `BiFunction` constructor for backward compatibility during migration. Add:

```java
private final BlockExpression blockPredicate; // nullable

public FilterOperator(Operator source, BlockExpression predicate) {
  this.source = source;
  this.blockPredicate = predicate;
  this.predicate = null;
}
```

Update `processNextBatch()` to use `blockPredicate` when present: evaluate to get BOOLEAN Block, extract matching positions, use `page.copyPositions()`.

**Step 2: Write test using BlockExpression predicate**

Verify that filtering via `BlockExpression` produces same results as the `BiFunction` path.

**Step 3: Run all FilterOperator tests to verify no regression**

Run: `./gradlew :dqe:spotlessApply && ./gradlew :dqe:test --tests "org.opensearch.sql.dqe.operator.FilterOperatorTest"`
Expected: All existing tests PASS, new test also PASS

**Step 4: Commit**

```bash
git commit -m "feat(dqe): extend FilterOperator to accept BlockExpression predicate"
```

---

### Task 16: EvalNode plan node and DqePlanVisitor update

**Files:**
- Create: `dqe/src/main/java/org/opensearch/sql/dqe/planner/plan/EvalNode.java`
- Modify: `dqe/src/main/java/org/opensearch/sql/dqe/planner/plan/DqePlanVisitor.java` — add `visitEval`
- Modify: `dqe/src/main/java/org/opensearch/sql/dqe/planner/plan/DqePlanNode.java:36-52` — add EvalNode to readPlanNode dispatch
- Modify: `dqe/src/test/java/org/opensearch/sql/dqe/planner/plan/PlanNodeTest.java` — add round-trip test

**Step 1: Create EvalNode**

Fields: `child` (DqePlanNode), `expressions` (List of String, serialized expression strings), `outputColumnNames` (List of String). Implements Writeable via `writeStringCollection` for both lists.

**Step 2: Add visitEval to DqePlanVisitor**

```java
public R visitEval(EvalNode node, C context) {
  return visitPlan(node, context);
}
```

**Step 3: Add EvalNode to readPlanNode dispatch in DqePlanNode**

Add `else if (className.equals(EvalNode.class.getName())) { return new EvalNode(in); }`.

**Step 4: Write round-trip serialization test**

**Step 5: Run tests, format, commit**

```bash
git commit -m "feat(dqe): add EvalNode plan node for computed columns"
```

---

### Task 17: Aggregate framework — standalone Accumulator classes

**Files:**
- Create: `dqe/src/main/java/org/opensearch/sql/dqe/function/aggregate/AggregateAccumulatorFactory.java`
- Create: `dqe/src/main/java/org/opensearch/sql/dqe/function/aggregate/Accumulator.java`
- Create: `dqe/src/main/java/org/opensearch/sql/dqe/function/aggregate/CountAccumulator.java`
- Create: `dqe/src/main/java/org/opensearch/sql/dqe/function/aggregate/SumAccumulator.java`
- Create: `dqe/src/main/java/org/opensearch/sql/dqe/function/aggregate/AvgAccumulator.java`
- Create: `dqe/src/main/java/org/opensearch/sql/dqe/function/aggregate/MinMaxAccumulator.java`
- Create: `dqe/src/test/java/org/opensearch/sql/dqe/function/aggregate/AccumulatorTest.java`

**Step 1: Define interfaces**

```java
public interface AggregateAccumulatorFactory {
  Accumulator createAccumulator();
  Type getIntermediateType();
  Type getOutputType();
}

public interface Accumulator {
  void addBlock(Block block, int positionCount);
  void writeFinalTo(BlockBuilder builder);
}
```

**Step 2: Write failing tests**

Port logic from existing `HashAggregationOperatorTest`:
- CountAccumulator: count all rows, including pages with nulls
- SumAccumulator: sum BIGINT, sum DOUBLE, skip nulls, empty -> null
- AvgAccumulator: avg of [2, 4, 6] -> 4.0, empty -> null
- MinMaxAccumulator: min/max BIGINT, VARCHAR, with nulls

**Step 3: Implement accumulators**

Migrate logic from inner classes of `HashAggregationOperator` into standalone classes. The `addBlock(Block, positionCount)` method loops over positions (similar to current `add(Page, position)` but accepts a Block directly).

**Step 4: Run tests, format, commit**

```bash
git commit -m "feat(dqe): add standalone aggregate Accumulator classes"
```

---

### Task 18: StddevVariance and BoolAgg accumulators

**Files:**
- Create: `dqe/src/main/java/org/opensearch/sql/dqe/function/aggregate/StddevVarianceAccumulator.java`
- Create: `dqe/src/main/java/org/opensearch/sql/dqe/function/aggregate/BoolAggAccumulator.java`
- Create: `dqe/src/test/java/org/opensearch/sql/dqe/function/aggregate/StddevVarianceAccumulatorTest.java`
- Create: `dqe/src/test/java/org/opensearch/sql/dqe/function/aggregate/BoolAggAccumulatorTest.java`

**Step 1: Write failing tests**

- `stddev([2,4,4,4,5,5,7,9])` -> approx 2.138 (sample std dev)
- `variance([2,4,4,4,5,5,7,9])` -> approx 4.571 (sample variance)
- `bool_and([true,true,true])` -> true, `bool_and([true,false])` -> false
- `bool_or([false,false,true])` -> true
- NULL handling for all

**Step 2: Implement**

`StddevVarianceAccumulator` uses Welford's online algorithm for numerical stability. `BoolAggAccumulator` tracks running AND/OR.

**Step 3: Run tests, format, commit**

```bash
git commit -m "feat(dqe): add stddev, variance, bool_and, bool_or accumulators"
```

---

### Task 19: Register aggregates in BuiltinFunctions

**Files:**
- Modify: `dqe/src/main/java/org/opensearch/sql/dqe/function/BuiltinFunctions.java`
- Modify: `dqe/src/test/java/org/opensearch/sql/dqe/function/BuiltinFunctionsTest.java`

**Step 1: Add aggregate registrations to BuiltinFunctions.createRegistry()**

Register COUNT, SUM, AVG, MIN, MAX, STDDEV, VARIANCE, BOOL_AND, BOOL_OR with appropriate overloads (BIGINT, DOUBLE, VARCHAR where applicable).

**Step 2: Add aggregate resolution tests**

```java
assertDoesNotThrow(() -> registry.resolve("count", List.of()));
assertDoesNotThrow(() -> registry.resolve("sum", List.of(BigintType.BIGINT)));
assertDoesNotThrow(() -> registry.resolve("avg", List.of(DoubleType.DOUBLE)));
assertDoesNotThrow(() -> registry.resolve("stddev", List.of(DoubleType.DOUBLE)));
assertDoesNotThrow(() -> registry.resolve("bool_and", List.of(BooleanType.BOOLEAN)));
```

**Step 3: Run tests, format, commit**

```bash
git commit -m "feat(dqe): register aggregate functions in BuiltinFunctions"
```

---

### Task 20: Wire ExpressionCompiler into LocalExecutionPlanner

**Files:**
- Modify: `dqe/src/main/java/org/opensearch/sql/dqe/shard/executor/LocalExecutionPlanner.java`
- Modify: `dqe/src/test/java/org/opensearch/sql/dqe/shard/executor/LocalExecutionPlannerTest.java`

**Step 1: Add FunctionRegistry to constructor**

Add a third constructor that takes `FunctionRegistry`. Keep existing constructors.

**Step 2: Update visitFilter to use ExpressionCompiler**

When `FunctionRegistry` is available, use `ExpressionCompiler` to produce `BlockExpression`, pass to `FilterOperator(source, blockPredicate)`. Fall back to old `BiFunction` path when registry is null.

**Step 3: Add visitEval**

```java
@Override
public Operator visitEval(EvalNode node, Void context) {
  Operator child = node.getChild().accept(this, context);
  List<String> inputColumns = resolveInputColumns(node.getChild());
  Map<String, Integer> columnIndexMap = buildColumnIndexMap(inputColumns);
  ExpressionCompiler compiler = new ExpressionCompiler(registry, columnIndexMap, columnTypeMap);
  List<BlockExpression> outputExprs = node.getExpressions().stream()
      .map(exprStr -> compiler.compile(parser.parseExpression(exprStr)))
      .toList();
  return new EvalOperator(child, outputExprs);
}
```

**Step 4: Run all LocalExecutionPlanner tests**

Run: `./gradlew :dqe:spotlessApply && ./gradlew :dqe:test --tests "org.opensearch.sql.dqe.shard.executor.LocalExecutionPlannerTest"`
Expected: All PASS

**Step 5: Commit**

```bash
git commit -m "refactor(dqe): wire ExpressionCompiler into LocalExecutionPlanner"
```

---

### Task 21: Update LogicalPlanner to emit EvalNode for computed columns

**Files:**
- Modify: `dqe/src/main/java/org/opensearch/sql/dqe/planner/LogicalPlanner.java`
- Modify: `dqe/src/test/java/org/opensearch/sql/dqe/planner/LogicalPlannerTest.java`

**Step 1: Detect computed columns in SELECT**

In `LogicalPlanner.plan()`, after building `FilterNode`/`AggregationNode`, check if any `SingleColumn` in SELECT has a non-`Identifier` expression (FunctionCall, arithmetic, Cast, etc.). If so, insert an `EvalNode` between the data source and the `ProjectNode`.

**Step 2: Write test**

```java
@Test
void computedColumnProducesEvalNode() {
  Statement stmt = parser.parse("SELECT status * 2 AS doubled FROM logs");
  DqePlanNode plan = LogicalPlanner.plan(stmt, tableName -> logsTableInfo);
  // Verify EvalNode is present in the plan tree
}
```

**Step 3: Run tests, format, commit**

```bash
git commit -m "feat(dqe): emit EvalNode for computed SELECT columns in LogicalPlanner"
```

---

### Task 22: Wire aggregate AccumulatorFactory into HashAggregationOperator

**Files:**
- Modify: `dqe/src/main/java/org/opensearch/sql/dqe/operator/HashAggregationOperator.java`
- Modify: `dqe/src/test/java/org/opensearch/sql/dqe/operator/HashAggregationOperatorTest.java`

**Step 1: Add constructor accepting AggregateAccumulatorFactory list**

Keep existing constructor/interfaces for backward compatibility. Add a second path that uses `AggregateAccumulatorFactory` from the `function/aggregate/` package.

**Step 2: Write test using the factory path**

Verify COUNT, SUM, AVG produce same results through the factory path as through old static method path.

**Step 3: Run all HashAggregationOperator tests**

Expected: All PASS (old and new paths)

**Step 4: Commit**

```bash
git commit -m "refactor(dqe): wire AggregateAccumulatorFactory into HashAggregationOperator"
```

---

### Task 23: End-to-end tests for functions and expressions

**Files:**
- Modify: `dqe/src/test/java/org/opensearch/sql/dqe/DqeEndToEndTest.java`

**Step 1: Add end-to-end test cases**

These exercise the full pipeline: parse -> plan -> optimize -> execute.

```java
@Test
void selectWithUpperFunction() {
  // SELECT UPPER(category) FROM test_index
}

@Test
void whereWithLengthFunction() {
  // SELECT category FROM test_index WHERE LENGTH(category) > 4
}

@Test
void selectWithArithmeticExpression() {
  // SELECT status * 2 AS doubled FROM test_index
}

@Test
void selectWithCaseExpression() {
  // SELECT CASE WHEN status > 200 THEN 'error' ELSE 'ok' END FROM test_index
}
```

**Step 2: Run the full test suite**

Run: `./gradlew :dqe:spotlessApply && ./gradlew :dqe:test`
Expected: All PASS

**Step 3: Commit**

```bash
git commit -m "test(dqe): add end-to-end tests for scalar functions and expressions"
```

---

### Task 24: Remove deprecated ExpressionEvaluator and old aggregate parsing

**Files:**
- Delete: `dqe/src/main/java/org/opensearch/sql/dqe/shard/executor/ExpressionEvaluator.java`
- Delete: `dqe/src/test/java/org/opensearch/sql/dqe/shard/executor/ExpressionEvaluatorTest.java`
- Modify: `dqe/src/main/java/org/opensearch/sql/dqe/operator/FilterOperator.java` — remove BiFunction constructor
- Modify: `dqe/src/main/java/org/opensearch/sql/dqe/shard/executor/LocalExecutionPlanner.java` — remove regex AGG_FUNCTION pattern and old paths
- Modify: `dqe/src/main/java/org/opensearch/sql/dqe/operator/HashAggregationOperator.java` — remove inner AggregateFunction/Accumulator interfaces and static factory methods

**Step 1: Remove ExpressionEvaluator and its test**

**Step 2: Remove old FilterOperator constructor (BiFunction variant)**

Only keep the `BlockExpression` constructor.

**Step 3: Remove regex AGG_FUNCTION pattern from LocalExecutionPlanner**

Replace `parseAggregateFunction` with registry-based resolution.

**Step 4: Remove old inner interfaces from HashAggregationOperator**

Replace with standalone `AggregateAccumulatorFactory`/`Accumulator`.

**Step 5: Run full test suite**

Run: `./gradlew :dqe:spotlessApply && ./gradlew :dqe:test`
Expected: All PASS

**Step 6: Commit**

```bash
git commit -m "refactor(dqe): remove deprecated ExpressionEvaluator and regex aggregate parsing"
```

---

### Task 25: Final verification

**Step 1: Run complete DQE test suite**

Run: `./gradlew :dqe:spotlessApply && ./gradlew :dqe:test`
Expected: All PASS, jacoco coverage >= 50%

**Step 2: Run compile check for full project**

Run: `./gradlew :dqe:compileJava :dqe:compileTestJava`
Expected: BUILD SUCCESSFUL

**Step 3: Commit any final cleanups**

```bash
git commit -m "chore(dqe): final cleanup after function framework implementation"
```
