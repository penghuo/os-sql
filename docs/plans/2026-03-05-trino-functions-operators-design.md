# Trino Functions & Operators for DQE

**Date**: 2026-03-05
**Status**: Proposed
**Depends on**: [Native Trino DQE Integration](2026-03-04-trino-native-integration-design.md)

## Problem Statement

The DQE module supports basic SQL queries (SELECT, WHERE, GROUP BY, ORDER BY, LIMIT) but lacks a function framework. The current state:

- **No scalar functions**: No string, math, date/time, conditional, or CAST support. `SELECT UPPER(name)` fails.
- **No function registry**: Aggregate functions are regex-matched strings in `LocalExecutionPlanner`. No type checking, no overload resolution, no extensibility.
- **Row-at-a-time evaluation**: `ExpressionEvaluator` processes one row at a time against Pages. Poor cache behavior and no vectorization potential.
- **Predicates only**: Expression evaluation is limited to WHERE clauses. SELECT-list computed columns (`SELECT price * qty AS total`) are not supported.

## Design Decisions

| Decision | Choice | Rationale |
|---|---|---|
| Evaluation model | Vectorized (column-at-a-time) | Functions operate on entire Blocks. Better CPU cache behavior, SIMD potential, matches Trino's native model. |
| Function scope | Standard SQL (~80 functions) | Covers ~95% of analytical queries. String, math, date/time, conditional, CAST. |
| Resolution timing | Plan-time | Analyzer resolves FunctionCall AST nodes to typed ResolvedFunction handles. Type errors caught early. Optimizer can reason about function properties. |
| Expression scope | Full: SELECT, WHERE, GROUP BY, ORDER BY, HAVING | New EvalOperator for computed columns. BlockExpression used everywhere. |
| Architecture | Thin registry wrapping Trino SPI types | Uses existing `trino-spi` Block/Type system. Custom FunctionRegistry with Trino-compatible resolution. No `trino-main` dependency. |

## Function Registry & Resolution

### FunctionRegistry

Central registry mapping function names to typed implementations:

```java
public class FunctionRegistry {
    // name (lowercase) → list of overloads
    private final Map<String, List<FunctionMetadata>> functions;

    public void register(FunctionMetadata metadata);
    public ResolvedFunction resolve(String name, List<Type> argumentTypes);
}
```

### FunctionMetadata

```java
public class FunctionMetadata {
    private final String name;
    private final List<Type> argumentTypes;
    private final Type returnType;
    private final FunctionKind kind;               // SCALAR, AGGREGATE, WINDOW
    private final boolean deterministic;
    private final boolean nullable;
    private final ScalarFunctionImplementation scalarImpl;
    private final AggregateAccumulatorFactory aggregateImpl;
}

public enum FunctionKind { SCALAR, AGGREGATE, WINDOW }
```

### ResolvedFunction

Carried through plan nodes, serialized across the wire:

```java
public class ResolvedFunction implements Writeable {
    private final String name;
    private final List<Type> argumentTypes;   // after coercion
    private final Type returnType;
    private final FunctionKind kind;
}
```

Serialized as `name + argumentTypeSignatures + returnTypeSignature` (strings). Shards re-resolve from local `FunctionRegistry` instance.

### Resolution Rules

1. Exact type match preferred.
2. Implicit numeric coercion: `INTEGER` → `BIGINT` → `DOUBLE`.
3. Literal string coercion to target type if unambiguous.
4. Multiple matches: prefer fewest coercions.
5. Ambiguous match → error at plan time.

## Vectorized Expression Evaluation

### BlockExpression — Core Abstraction

Replaces the row-at-a-time `ExpressionEvaluator`. Each node produces an output Block for the entire Page:

```java
public interface BlockExpression {
    Block evaluate(Page page);
    Type getType();
}
```

### ExpressionCompiler

Compiles Trino `Expression` AST into a `BlockExpression` tree at operator build time:

```java
public class ExpressionCompiler {
    private final FunctionRegistry registry;
    private final Map<String, Integer> columnIndexMap;
    private final Map<String, Type> columnTypeMap;

    public BlockExpression compile(Expression expr) { ... }
}
```

Handles: `Identifier` → `ColumnReference`, literals → `ConstantExpression`, `FunctionCall` → `ScalarFunctionExpression`, `ComparisonExpression` → `ComparisonBlockExpression`, `ArithmeticBinaryExpression` → `ArithmeticBlockExpression`, logical operators, CASE, CAST, LIKE, IN, IS NULL, COALESCE, NULLIF.

### BlockExpression Hierarchy

```
BlockExpression (interface)
├── ColumnReference          — reads Block from Page by column index
├── ConstantExpression       — RunLengthEncoded Block of constant value
├── ScalarFunctionExpression — dispatches to ScalarFunctionImplementation
├── ComparisonExpression     — vectorized =, <>, <, <=, >, >=
├── ArithmeticExpression     — vectorized +, -, *, /, %
├── LogicalAndExpression     — short-circuit AND over boolean Blocks
├── LogicalOrExpression      — short-circuit OR over boolean Blocks
├── NotExpression            — boolean negation
├── CastExpression           — type coercion
├── CaseExpression           — CASE WHEN ... THEN ... ELSE ... END
├── CoalesceExpression       — first non-null
├── NullIfExpression         — NULL if equal
├── IsNullExpression         — NULL check
├── LikeExpression           — pattern matching (compiled regex)
└── InExpression             — value IN (list)
```

### ScalarFunctionImplementation

The contract every scalar function provides:

```java
@FunctionalInterface
public interface ScalarFunctionImplementation {
    Block evaluate(Block[] arguments, int positionCount);
}
```

Functions receive input Blocks and row count, return an output Block.

### Operator Integration

**FilterOperator** (updated):

```java
public class FilterOperator implements Operator {
    private final Operator source;
    private final BlockExpression predicate;  // produces BOOLEAN Block

    public Page processNextBatch() {
        Page input = source.processNextBatch();
        Block mask = predicate.evaluate(input);
        int[] positions = filterPositions(mask);
        return input.copyPositions(positions);
    }
}
```

**EvalOperator** (new — for SELECT-list expressions):

```java
public class EvalOperator implements Operator {
    private final Operator source;
    private final List<BlockExpression> outputExpressions;

    public Page processNextBatch() {
        Page input = source.processNextBatch();
        Block[] outputBlocks = new Block[outputExpressions.size()];
        for (int i = 0; i < outputExpressions.size(); i++) {
            outputBlocks[i] = outputExpressions.get(i).evaluate(input);
        }
        return new Page(outputBlocks);
    }
}
```

## Scalar Function Library

### String Functions

| Function | Signature | Description |
|---|---|---|
| `concat` | `(VARCHAR, VARCHAR) → VARCHAR` | Concatenate |
| `length` | `(VARCHAR) → BIGINT` | Character count |
| `lower` | `(VARCHAR) → VARCHAR` | Lowercase |
| `upper` | `(VARCHAR) → VARCHAR` | Uppercase |
| `trim` | `(VARCHAR) → VARCHAR` | Strip whitespace |
| `ltrim` | `(VARCHAR) → VARCHAR` | Strip leading whitespace |
| `rtrim` | `(VARCHAR) → VARCHAR` | Strip trailing whitespace |
| `substring` | `(VARCHAR, BIGINT, BIGINT) → VARCHAR` | Extract substring (1-indexed) |
| `replace` | `(VARCHAR, VARCHAR, VARCHAR) → VARCHAR` | Replace occurrences |
| `position` | `(VARCHAR, VARCHAR) → BIGINT` | Find substring position |
| `reverse` | `(VARCHAR) → VARCHAR` | Reverse string |
| `lpad` | `(VARCHAR, BIGINT, VARCHAR) → VARCHAR` | Left-pad |
| `rpad` | `(VARCHAR, BIGINT, VARCHAR) → VARCHAR` | Right-pad |
| `starts_with` | `(VARCHAR, VARCHAR) → BOOLEAN` | Prefix check |
| `regexp_like` | `(VARCHAR, VARCHAR) → BOOLEAN` | Regex match |
| `regexp_extract` | `(VARCHAR, VARCHAR) → VARCHAR` | First regex match |
| `chr` | `(BIGINT) → VARCHAR` | Codepoint to character |
| `codepoint` | `(VARCHAR) → BIGINT` | Character to codepoint |
| `split` | `(VARCHAR, VARCHAR) → ARRAY(VARCHAR)` | Split by delimiter |

`LIKE` is handled as `LikeExpression` (compiled to regex at plan time), not a registry function.

### Math Functions

| Function | Signature | Description |
|---|---|---|
| `abs` | `(DOUBLE) → DOUBLE` | Absolute value |
| `ceil` / `ceiling` | `(DOUBLE) → BIGINT` | Ceiling |
| `floor` | `(DOUBLE) → BIGINT` | Floor |
| `round` | `(DOUBLE, BIGINT) → DOUBLE` | Round to N places |
| `truncate` | `(DOUBLE) → DOUBLE` | Truncate toward zero |
| `power` / `pow` | `(DOUBLE, DOUBLE) → DOUBLE` | Exponentiation |
| `sqrt` | `(DOUBLE) → DOUBLE` | Square root |
| `cbrt` | `(DOUBLE) → DOUBLE` | Cube root |
| `exp` | `(DOUBLE) → DOUBLE` | e^x |
| `ln` | `(DOUBLE) → DOUBLE` | Natural log |
| `log2` | `(DOUBLE) → DOUBLE` | Base-2 log |
| `log10` | `(DOUBLE) → DOUBLE` | Base-10 log |
| `log` | `(DOUBLE, DOUBLE) → DOUBLE` | Arbitrary base log |
| `mod` | `(BIGINT, BIGINT) → BIGINT` | Modulus |
| `sign` | `(DOUBLE) → DOUBLE` | Sign (-1, 0, 1) |
| `pi` | `() → DOUBLE` | Constant π |
| `e` | `() → DOUBLE` | Constant e |
| `random` | `() → DOUBLE` | Random [0,1) |
| `radians` | `(DOUBLE) → DOUBLE` | Degrees to radians |
| `degrees` | `(DOUBLE) → DOUBLE` | Radians to degrees |
| `sin`, `cos`, `tan` | `(DOUBLE) → DOUBLE` | Trig functions |
| `asin`, `acos`, `atan`, `atan2` | `(DOUBLE) → DOUBLE` | Inverse trig |

### Date/Time Functions

| Function | Signature | Description |
|---|---|---|
| `current_timestamp` | `() → TIMESTAMP` | Current time |
| `current_date` | `() → DATE` | Current date |
| `now` | `() → TIMESTAMP` | Alias for current_timestamp |
| `date_add` | `(VARCHAR, BIGINT, TIMESTAMP) → TIMESTAMP` | Add interval |
| `date_diff` | `(VARCHAR, TIMESTAMP, TIMESTAMP) → BIGINT` | Difference in units |
| `extract` | `(field FROM TIMESTAMP) → BIGINT` | Extract year/month/day/etc. |
| `year`, `month`, `day` | `(TIMESTAMP) → BIGINT` | Extract field shortcuts |
| `hour`, `minute`, `second` | `(TIMESTAMP) → BIGINT` | Extract time fields |
| `day_of_week` / `dow` | `(TIMESTAMP) → BIGINT` | Day of week (1=Monday) |
| `day_of_year` / `doy` | `(TIMESTAMP) → BIGINT` | Day of year |
| `date_trunc` | `(VARCHAR, TIMESTAMP) → TIMESTAMP` | Truncate to unit |
| `date_format` | `(TIMESTAMP, VARCHAR) → VARCHAR` | Format timestamp |
| `date_parse` | `(VARCHAR, VARCHAR) → TIMESTAMP` | Parse string to timestamp |
| `from_unixtime` | `(BIGINT) → TIMESTAMP` | Epoch seconds to timestamp |
| `to_unixtime` | `(TIMESTAMP) → DOUBLE` | Timestamp to epoch seconds |

### Conditional Functions

| Function | Signature | Description |
|---|---|---|
| `CASE` | (built-in expression) | Conditional branching |
| `coalesce` | `(T, T, ...) → T` | First non-null |
| `nullif` | `(T, T) → T` | NULL if equal |
| `if` | `(BOOLEAN, T, T) → T` | Ternary conditional |
| `try` | `(T) → T` | Suppress errors, return NULL |

### Type Cast

`CAST(expr AS type)` is a built-in `CastExpression`. Supported paths:

- `VARCHAR` ↔ `BIGINT`, `INTEGER`, `DOUBLE`, `BOOLEAN`, `TIMESTAMP`
- `BIGINT` ↔ `INTEGER`, `DOUBLE`, `REAL`
- `TIMESTAMP` ↔ `VARCHAR`, `BIGINT` (epoch millis)

**Total: ~80 functions.**

## Aggregate Function Framework

### Interfaces

```java
public interface AggregateAccumulatorFactory {
    Accumulator createAccumulator();
    Type getIntermediateType();
    Type getOutputType();
}

public interface Accumulator {
    void addBlock(Block block, int positionCount);      // vectorized input
    void serialize(BlockBuilder builder);                // partial → intermediate
    void mergeWith(Block intermediateBlock, int pos);    // merge partials
    void writeFinalTo(BlockBuilder builder);             // final output
}
```

### Aggregate Functions

| Function | Intermediate Type | Output Type |
|---|---|---|
| `count(*)` | `BIGINT` | `BIGINT` |
| `count(col)` | `BIGINT` | `BIGINT` |
| `count(DISTINCT col)` | `ARRAY(T)` or HLL | `BIGINT` |
| `sum(col)` | Same as input | Same as input |
| `avg(col)` | `ROW(DOUBLE, BIGINT)` | `DOUBLE` |
| `min(col)` | Same as input | Same as input |
| `max(col)` | Same as input | Same as input |
| `stddev(col)` | `ROW(DOUBLE, DOUBLE, BIGINT)` | `DOUBLE` |
| `variance(col)` | `ROW(DOUBLE, DOUBLE, BIGINT)` | `DOUBLE` |
| `approx_distinct(col)` | `VARBINARY` (HLL) | `BIGINT` |
| `arbitrary(col)` | Same as input | Same as input |
| `bool_and(col)` | `BOOLEAN` | `BOOLEAN` |
| `bool_or(col)` | `BOOLEAN` | `BOOLEAN` |
| `array_agg(col)` | `ARRAY(T)` | `ARRAY(T)` |

### Partial/Final Aggregation

The optimizer splits aggregation into PARTIAL (shard) and FINAL (coordinator):

- **PARTIAL**: `addBlock()` accumulates rows → `serialize()` writes intermediate state
- **FINAL**: `mergeWith()` merges partial states from shards → `writeFinalTo()` produces result

### Replacing Regex Parsing

**Before**: `AggregationNode` stores `List<String>`, parsed via regex at execution time.
**After**: `AggregationNode` stores `List<ResolvedFunction>` with argument column references. `LocalExecutionPlanner` gets `AggregateAccumulatorFactory` from the registry.

## Package Layout

```
dqe/src/main/java/org/opensearch/sql/dqe/
├── function/                              # Function framework
│   ├── FunctionRegistry.java
│   ├── FunctionMetadata.java
│   ├── ResolvedFunction.java
│   ├── FunctionKind.java
│   ├── BuiltinFunctions.java             # Registers all builtins
│   │
│   ├── expression/                        # BlockExpression tree
│   │   ├── BlockExpression.java
│   │   ├── ExpressionCompiler.java
│   │   ├── ColumnReference.java
│   │   ├── ConstantExpression.java
│   │   ├── ScalarFunctionExpression.java
│   │   ├── ComparisonExpression.java
│   │   ├── ArithmeticExpression.java
│   │   ├── LogicalAndExpression.java
│   │   ├── LogicalOrExpression.java
│   │   ├── NotExpression.java
│   │   ├── CastExpression.java
│   │   ├── CaseExpression.java
│   │   ├── CoalesceExpression.java
│   │   ├── NullIfExpression.java
│   │   ├── IsNullExpression.java
│   │   ├── LikeExpression.java
│   │   └── InExpression.java
│   │
│   ├── scalar/                            # Scalar implementations
│   │   ├── ScalarFunctionImplementation.java
│   │   ├── StringFunctions.java
│   │   ├── MathFunctions.java
│   │   ├── DateTimeFunctions.java
│   │   └── TrigFunctions.java
│   │
│   └── aggregate/                         # Aggregate implementations
│       ├── AggregateAccumulatorFactory.java
│       ├── Accumulator.java
│       ├── CountAccumulator.java
│       ├── SumAccumulator.java
│       ├── AvgAccumulator.java
│       ├── MinMaxAccumulator.java
│       ├── StddevVarianceAccumulator.java
│       ├── ApproxDistinctAccumulator.java
│       ├── ArrayAggAccumulator.java
│       └── BoolAggAccumulator.java
│
├── operator/
│   ├── FilterOperator.java               # MODIFIED: BlockExpression predicate
│   ├── EvalOperator.java                 # NEW: computed columns
│   ├── HashAggregationOperator.java      # MODIFIED: uses AccumulatorFactory
│   └── ... (rest unchanged)
│
├── planner/
│   ├── LogicalPlanner.java               # MODIFIED: resolves functions
│   └── plan/
│       ├── AggregationNode.java          # MODIFIED: ResolvedFunction list
│       ├── FilterNode.java               # MODIFIED: serialized Expression
│       ├── EvalNode.java                 # NEW: computed columns
│       └── ...
│
└── shard/executor/
    ├── LocalExecutionPlanner.java        # MODIFIED: uses ExpressionCompiler
    └── ExpressionEvaluator.java          # DEPRECATED
```

## Testing Strategy

### Unit Tests

- **FunctionRegistry**: exact match, coercion, ambiguity, unknown function
- **BlockExpression** (per type): normal values, NULL propagation, empty/large pages
- **Scalar functions** (per function): normal I/O, NULL input, edge cases, type variants
- **Aggregates** (per accumulator): normal, NULL handling, empty input, partial/final round-trip
- **ExpressionCompiler**: each AST node type, nested expressions, function resolution

### Integration Tests

Extend `dqe/integ-test/` with queries:

- String: `SELECT UPPER(name), LENGTH(name) FROM index`
- Math: `SELECT ABS(balance), ROUND(price, 2) FROM index`
- Date: `SELECT EXTRACT(YEAR FROM timestamp) FROM index`
- CAST: `SELECT CAST(age AS DOUBLE) FROM index`
- CASE: `SELECT CASE WHEN status > 200 THEN 'error' ELSE 'ok' END FROM index`
- Mixed: `SELECT UPPER(name), COUNT(*) FROM index WHERE LENGTH(name) > 5 GROUP BY UPPER(name)`
- Advanced agg: `SELECT category, STDDEV(price) FROM index GROUP BY category`

### Performance Benchmarking

Manual benchmarking of vectorized vs row-at-a-time on 1M+ row datasets during development.

## Migration Path

1. **Phase 1**: Add new framework (`function/` package) alongside existing code. Both `ExpressionEvaluator` and `BlockExpression` coexist.
2. **Phase 2**: Migrate operators to use `BlockExpression`. `FilterOperator` and `HashAggregationOperator` switch to new interfaces.
3. **Phase 3**: Remove deprecated `ExpressionEvaluator` and regex-based aggregate parsing.
