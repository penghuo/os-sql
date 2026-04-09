/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.function.expression;

import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import io.trino.sql.tree.ArithmeticBinaryExpression;
import io.trino.sql.tree.ArithmeticUnaryExpression;
import io.trino.sql.tree.BetweenPredicate;
import io.trino.sql.tree.BooleanLiteral;
import io.trino.sql.tree.Cast;
import io.trino.sql.tree.CoalesceExpression;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.DecimalLiteral;
import io.trino.sql.tree.DoubleLiteral;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.Extract;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.GenericLiteral;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.InListExpression;
import io.trino.sql.tree.InPredicate;
import io.trino.sql.tree.IsNotNullPredicate;
import io.trino.sql.tree.IsNullPredicate;
import io.trino.sql.tree.LikePredicate;
import io.trino.sql.tree.LogicalExpression;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.NotExpression;
import io.trino.sql.tree.NullIfExpression;
import io.trino.sql.tree.NullLiteral;
import io.trino.sql.tree.SearchedCaseExpression;
import io.trino.sql.tree.SimpleCaseExpression;
import io.trino.sql.tree.StringLiteral;
import io.trino.sql.tree.WhenClause;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.opensearch.sql.dqe.function.FunctionMetadata;
import org.opensearch.sql.dqe.function.FunctionRegistry;
import org.opensearch.sql.dqe.function.ResolvedFunction;

/**
 * Compiles Trino AST {@link Expression} nodes into vectorized {@link BlockExpression} trees. This
 * is the bridge between the parsed SQL and the vectorized evaluation engine.
 */
public class ExpressionCompiler {

  private final FunctionRegistry registry;
  private final Map<String, Integer> columnIndexMap;
  private final Map<String, Type> columnTypeMap;

  public ExpressionCompiler(
      FunctionRegistry registry,
      Map<String, Integer> columnIndexMap,
      Map<String, Type> columnTypeMap) {
    this.registry = registry;
    this.columnIndexMap = columnIndexMap;
    this.columnTypeMap = columnTypeMap;
  }

  /** Compile an AST Expression into a vectorized BlockExpression. */
  public BlockExpression compile(Expression expr) {
    if (expr instanceof Identifier) {
      return compileIdentifier((Identifier) expr);
    } else if (expr instanceof LongLiteral) {
      return new ConstantExpression(((LongLiteral) expr).getParsedValue(), BigintType.BIGINT);
    } else if (expr instanceof DoubleLiteral) {
      return new ConstantExpression(((DoubleLiteral) expr).getValue(), DoubleType.DOUBLE);
    } else if (expr instanceof DecimalLiteral) {
      return new ConstantExpression(
          Double.parseDouble(((DecimalLiteral) expr).getValue()), DoubleType.DOUBLE);
    } else if (expr instanceof StringLiteral) {
      return new ConstantExpression(((StringLiteral) expr).getValue(), VarcharType.VARCHAR);
    } else if (expr instanceof BooleanLiteral) {
      return new ConstantExpression(((BooleanLiteral) expr).getValue(), BooleanType.BOOLEAN);
    } else if (expr instanceof NullLiteral) {
      return new ConstantExpression(null, BigintType.BIGINT);
    } else if (expr instanceof GenericLiteral generic) {
      return compileGenericLiteral(generic);
    } else if (expr instanceof ComparisonExpression cmp) {
      return new ComparisonBlockExpression(
          cmp.getOperator(), compile(cmp.getLeft()), compile(cmp.getRight()));
    } else if (expr instanceof ArithmeticBinaryExpression arith) {
      return new ArithmeticBlockExpression(
          arith.getOperator(), compile(arith.getLeft()), compile(arith.getRight()));
    } else if (expr instanceof LogicalExpression logical) {
      return compileLogical(logical);
    } else if (expr instanceof NotExpression not) {
      return new NotBlockExpression(compile(not.getValue()));
    } else if (expr instanceof IsNullPredicate isNull) {
      return new IsNullExpression(compile(isNull.getValue()), false);
    } else if (expr instanceof IsNotNullPredicate isNotNull) {
      return new IsNullExpression(compile(isNotNull.getValue()), true);
    } else if (expr instanceof Cast cast) {
      return compileCast(cast);
    } else if (expr instanceof SearchedCaseExpression caseExpr) {
      return compileSearchedCase(caseExpr);
    } else if (expr instanceof CoalesceExpression coalesce) {
      return compileCoalesce(coalesce);
    } else if (expr instanceof NullIfExpression nullIf) {
      return new NullIfBlockExpression(compile(nullIf.getFirst()), compile(nullIf.getSecond()));
    } else if (expr instanceof BetweenPredicate between) {
      // BETWEEN a AND b → (value >= a AND value <= b)
      BlockExpression value = compile(between.getValue());
      BlockExpression min = compile(between.getMin());
      BlockExpression max = compile(between.getMax());
      BlockExpression geMin =
          new ComparisonBlockExpression(
              ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL, value, min);
      BlockExpression leMax =
          new ComparisonBlockExpression(
              ComparisonExpression.Operator.LESS_THAN_OR_EQUAL, value, max);
      return new LogicalAndExpression(geMin, leMax);
    } else if (expr instanceof ArithmeticUnaryExpression unary) {
      BlockExpression child = compile(unary.getValue());
      if (unary.getSign() == ArithmeticUnaryExpression.Sign.MINUS) {
        // -x → 0 - x (match zero constant type to child type for correct promotion)
        BlockExpression zero =
            (child.getType() instanceof DoubleType)
                ? new ConstantExpression(0.0d, DoubleType.DOUBLE)
                : new ConstantExpression(0L, BigintType.BIGINT);
        return new ArithmeticBlockExpression(
            ArithmeticBinaryExpression.Operator.SUBTRACT, zero, child);
      }
      return child; // +x → x
    } else if (expr instanceof SimpleCaseExpression simpleCase) {
      return compileSimpleCase(simpleCase);
    } else if (expr instanceof LikePredicate like) {
      return compileLike(like);
    } else if (expr instanceof InPredicate in) {
      return compileIn(in);
    } else if (expr instanceof FunctionCall func) {
      // Check if this function call matches a column name (e.g., COUNT(*) in HAVING
      // references the aggregation output column "count(*)")
      String funcColName = expressionToColumnName(func);
      if (columnIndexMap.containsKey(funcColName)) {
        Type colType = columnTypeMap.getOrDefault(funcColName, BigintType.BIGINT);
        return new ColumnReference(columnIndexMap.get(funcColName), colType);
      }
      return compileFunctionCall(func);
    } else if (expr instanceof Extract extract) {
      // EXTRACT(field FROM expr) → convert to the equivalent function call
      // e.g., EXTRACT(MINUTE FROM EventTime) → minute(EventTime)
      String funcName = extract.getField().name().toLowerCase(java.util.Locale.ROOT);
      BlockExpression arg = compile(extract.getExpression());
      return compileNamedFunction(funcName, List.of(arg));
    }
    throw new UnsupportedOperationException(
        "Unsupported expression type: " + expr.getClass().getSimpleName());
  }

  /**
   * Compile a GenericLiteral (e.g., DATE '2013-07-01', TIMESTAMP '2013-07-01 12:00:00'). Converts
   * the literal value to the appropriate Trino internal representation.
   */
  private BlockExpression compileGenericLiteral(GenericLiteral literal) {
    String typeName = literal.getType().toUpperCase(java.util.Locale.ROOT);
    String value = literal.getValue();

    switch (typeName) {
      case "DATE":
        {
          // Parse date string and convert to epoch days for DateType
          java.time.LocalDate date = java.time.LocalDate.parse(value);
          long epochDays = date.toEpochDay();
          return new ConstantExpression(epochDays, io.trino.spi.type.DateType.DATE);
        }
      case "TIMESTAMP":
        {
          // Parse timestamp string and convert to Trino timestamp micros
          java.time.LocalDateTime dt =
              java.time.LocalDateTime.parse(
                  value, java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
          long epochMicros =
              dt.toEpochSecond(java.time.ZoneOffset.UTC) * 1_000_000L + dt.getNano() / 1_000L;
          return new ConstantExpression(epochMicros, TimestampType.TIMESTAMP_MILLIS);
        }
      case "INTEGER":
      case "INT":
        return new ConstantExpression(Long.parseLong(value), BigintType.BIGINT);
      case "BIGINT":
        return new ConstantExpression(Long.parseLong(value), BigintType.BIGINT);
      case "DOUBLE":
        return new ConstantExpression(Double.parseDouble(value), DoubleType.DOUBLE);
      default:
        // Treat as VARCHAR for unknown types
        return new ConstantExpression(value, VarcharType.VARCHAR);
    }
  }

  private BlockExpression compileIdentifier(Identifier identifier) {
    String name = identifier.getValue();
    Integer colIdx = columnIndexMap.get(name);
    if (colIdx == null) {
      throw new IllegalArgumentException(
          "Column '" + name + "' not found in column index map: " + columnIndexMap.keySet());
    }
    Type type = columnTypeMap.getOrDefault(name, BigintType.BIGINT);
    return new ColumnReference(colIdx, type);
  }

  private BlockExpression compileLogical(LogicalExpression logical) {
    List<Expression> terms = logical.getTerms();
    BlockExpression result = compile(terms.get(0));
    for (int i = 1; i < terms.size(); i++) {
      BlockExpression next = compile(terms.get(i));
      if (logical.getOperator() == LogicalExpression.Operator.AND) {
        result = new LogicalAndExpression(result, next);
      } else {
        result = new LogicalOrExpression(result, next);
      }
    }
    return result;
  }

  private BlockExpression compileCast(Cast cast) {
    BlockExpression child = compile(cast.getExpression());
    String targetTypeName = cast.getType().toString().toLowerCase();
    Type targetType;
    switch (targetTypeName) {
      case "double":
        targetType = DoubleType.DOUBLE;
        break;
      case "bigint":
        targetType = BigintType.BIGINT;
        break;
      case "integer":
        targetType = BigintType.BIGINT; // Promote to BIGINT for simplicity
        break;
      case "varchar":
        targetType = VarcharType.VARCHAR;
        break;
      case "boolean":
        targetType = BooleanType.BOOLEAN;
        break;
      default:
        // For unknown types, fall back to VARCHAR to avoid crashes
        targetType = VarcharType.VARCHAR;
        break;
    }
    // TRY_CAST is represented as a Cast with safe=true; returns null on failure
    if (cast.isSafe()) {
      return new TryCastBlockExpression(child, targetType);
    }
    return new CastBlockExpression(child, targetType);
  }

  private BlockExpression compileSearchedCase(SearchedCaseExpression caseExpr) {
    List<BlockExpression> conditions = new ArrayList<>();
    List<BlockExpression> results = new ArrayList<>();
    for (WhenClause when : caseExpr.getWhenClauses()) {
      conditions.add(compile(when.getOperand()));
      results.add(compile(when.getResult()));
    }
    BlockExpression elseExpr = caseExpr.getDefaultValue().map(this::compile).orElse(null);
    Type outputType = results.isEmpty() ? VarcharType.VARCHAR : results.get(0).getType();
    return new CaseBlockExpression(conditions, results, elseExpr, outputType);
  }

  private BlockExpression compileCoalesce(CoalesceExpression coalesce) {
    List<BlockExpression> operands =
        coalesce.getOperands().stream().map(this::compile).collect(Collectors.toList());
    Type outputType = operands.isEmpty() ? VarcharType.VARCHAR : operands.get(0).getType();

    // Determine common output type: promote mixed integer types to BIGINT, and if any operand
    // is DOUBLE while others are integer-family, promote to DOUBLE.
    for (BlockExpression op : operands) {
      outputType = promoteTypes(outputType, op.getType());
    }

    // Insert implicit casts for operands whose types differ from the common output type
    List<BlockExpression> castOperands = new ArrayList<>();
    for (BlockExpression op : operands) {
      if (!op.getType().equals(outputType)) {
        castOperands.add(new CastBlockExpression(op, outputType));
      } else {
        castOperands.add(op);
      }
    }
    return new CoalesceBlockExpression(castOperands, outputType);
  }

  /**
   * Determines the wider of two types for implicit promotion. Rules: - IntegerType + BigintType ->
   * BigintType - Any integer + DoubleType -> DoubleType - Otherwise, the first type wins.
   */
  private Type promoteTypes(Type a, Type b) {
    if (a.equals(b)) {
      return a;
    }
    boolean aIsInt = a instanceof BigintType || a instanceof IntegerType;
    boolean bIsInt = b instanceof BigintType || b instanceof IntegerType;
    boolean aIsDouble = a instanceof DoubleType;
    boolean bIsDouble = b instanceof DoubleType;

    // If both are integer-family, promote to BIGINT
    if (aIsInt && bIsInt) {
      return BigintType.BIGINT;
    }
    // If one is double and the other is numeric, promote to DOUBLE
    if ((aIsDouble && bIsInt) || (aIsInt && bIsDouble)) {
      return DoubleType.DOUBLE;
    }
    if (aIsDouble && bIsDouble) {
      return DoubleType.DOUBLE;
    }
    // Default: keep the first type
    return a;
  }

  private BlockExpression compileSimpleCase(SimpleCaseExpression simpleCase) {
    // CASE operand WHEN v1 THEN r1 WHEN v2 THEN r2 ... ELSE default END
    // → CASE WHEN operand = v1 THEN r1 WHEN operand = v2 THEN r2 ... ELSE default END
    BlockExpression operand = compile(simpleCase.getOperand());
    List<BlockExpression> conditions = new ArrayList<>();
    List<BlockExpression> results = new ArrayList<>();
    for (WhenClause when : simpleCase.getWhenClauses()) {
      BlockExpression whenValue = compile(when.getOperand());
      conditions.add(
          new ComparisonBlockExpression(ComparisonExpression.Operator.EQUAL, operand, whenValue));
      results.add(compile(when.getResult()));
    }
    BlockExpression elseExpr = simpleCase.getDefaultValue().map(this::compile).orElse(null);
    Type outputType = results.isEmpty() ? VarcharType.VARCHAR : results.get(0).getType();
    return new CaseBlockExpression(conditions, results, elseExpr, outputType);
  }

  private BlockExpression compileLike(LikePredicate like) {
    BlockExpression child = compile(like.getValue());
    if (!(like.getPattern() instanceof StringLiteral patternLiteral)) {
      throw new UnsupportedOperationException("Only string literal LIKE patterns are supported");
    }
    return new LikeBlockExpression(child, patternLiteral.getValue());
  }

  private BlockExpression compileIn(InPredicate in) {
    BlockExpression value = compile(in.getValue());
    if (!(in.getValueList() instanceof InListExpression inList)) {
      throw new UnsupportedOperationException("Only IN list expressions are supported");
    }
    Type targetType = value.getType();
    List<BlockExpression> list =
        inList.getValues().stream()
            .map(this::compile)
            .map(e -> e.getType().equals(targetType) ? e : new CastBlockExpression(e, targetType))
            .collect(Collectors.toList());
    return new InBlockExpression(value, list);
  }

  private BlockExpression compileFunctionCall(FunctionCall func) {
    String funcName = func.getName().toString();
    List<BlockExpression> compiledArgs =
        func.getArguments().stream().map(this::compile).collect(Collectors.toList());
    List<Type> argTypes =
        compiledArgs.stream().map(BlockExpression::getType).collect(Collectors.toList());

    ResolvedFunction resolved = registry.resolve(funcName, argTypes);
    FunctionMetadata metadata = registry.getMetadata(resolved);

    if (metadata.getScalarImplementation() == null) {
      throw new UnsupportedOperationException(
          "Function '" + funcName + "' has no scalar implementation");
    }

    // Insert implicit casts where the resolved parameter type differs from the actual arg type
    List<Type> paramTypes = resolved.getArgumentTypes();
    List<BlockExpression> castArgs = new ArrayList<>();
    for (int i = 0; i < compiledArgs.size(); i++) {
      BlockExpression arg = compiledArgs.get(i);
      if (!arg.getType().equals(paramTypes.get(i))) {
        castArgs.add(new CastBlockExpression(arg, paramTypes.get(i)));
      } else {
        castArgs.add(arg);
      }
    }

    return new ScalarFunctionExpression(
        metadata.getScalarImplementation(), castArgs, metadata.getReturnType());
  }

  /**
   * Convert a FunctionCall expression to its column name form (matching LogicalPlanner's naming).
   * E.g., COUNT(*) -> "count(*)", MIN(URL) -> "min(URL)".
   */
  private static String expressionToColumnName(FunctionCall func) {
    String args;
    if (func.getArguments().isEmpty()) {
      args = "*";
    } else {
      args = func.getArguments().stream().map(Object::toString).collect(Collectors.joining(", "));
    }
    String distinct = func.isDistinct() ? "DISTINCT " : "";
    return func.getName().toString() + "(" + distinct + args + ")";
  }

  /**
   * Compile a named function call with pre-compiled arguments. Used by EXTRACT and other expression
   * transformations that don't start as FunctionCall AST nodes.
   */
  private BlockExpression compileNamedFunction(
      String funcName, List<BlockExpression> compiledArgs) {
    List<Type> argTypes =
        compiledArgs.stream().map(BlockExpression::getType).collect(Collectors.toList());
    ResolvedFunction resolved = registry.resolve(funcName, argTypes);
    FunctionMetadata metadata = registry.getMetadata(resolved);
    if (metadata.getScalarImplementation() == null) {
      throw new UnsupportedOperationException(
          "Function '" + funcName + "' has no scalar implementation");
    }
    List<Type> paramTypes = resolved.getArgumentTypes();
    List<BlockExpression> castArgs = new ArrayList<>();
    for (int i = 0; i < compiledArgs.size(); i++) {
      BlockExpression arg = compiledArgs.get(i);
      if (!arg.getType().equals(paramTypes.get(i))) {
        castArgs.add(new CastBlockExpression(arg, paramTypes.get(i)));
      } else {
        castArgs.add(arg);
      }
    }
    return new ScalarFunctionExpression(
        metadata.getScalarImplementation(), castArgs, metadata.getReturnType());
  }
}
