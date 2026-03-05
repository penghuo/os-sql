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
import io.trino.sql.tree.BooleanLiteral;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.DecimalLiteral;
import io.trino.sql.tree.DoubleLiteral;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.IsNotNullPredicate;
import io.trino.sql.tree.IsNullPredicate;
import io.trino.sql.tree.LogicalExpression;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.NotExpression;
import io.trino.sql.tree.NullLiteral;
import io.trino.sql.tree.StringLiteral;
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

    return new ScalarFunctionExpression(
        metadata.getScalarImplementation(), compiledArgs, metadata.getReturnType());
  }
}
