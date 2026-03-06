/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.function.expression;

import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DoubleType;
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
import io.trino.sql.tree.FunctionCall;
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
        // -x → 0 - x
        BlockExpression zero = new ConstantExpression(0L, BigintType.BIGINT);
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
      return compileFunctionCall(func);
    }
    throw new UnsupportedOperationException(
        "Unsupported expression type: " + expr.getClass().getSimpleName());
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
      case "varchar":
        targetType = VarcharType.VARCHAR;
        break;
      case "boolean":
        targetType = BooleanType.BOOLEAN;
        break;
      default:
        throw new UnsupportedOperationException("Unsupported CAST target type: " + targetTypeName);
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
    return new CoalesceBlockExpression(operands, outputType);
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
    List<BlockExpression> list =
        inList.getValues().stream().map(this::compile).collect(Collectors.toList());
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
}
