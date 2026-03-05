/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.shard.executor;

import io.trino.spi.Page;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import io.trino.sql.tree.ArithmeticBinaryExpression;
import io.trino.sql.tree.BooleanLiteral;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.DecimalLiteral;
import io.trino.sql.tree.DoubleLiteral;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.LogicalExpression;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.NotExpression;
import io.trino.sql.tree.NullLiteral;
import io.trino.sql.tree.StringLiteral;
import java.util.Map;

/**
 * Evaluates Trino Expression AST nodes against a columnar {@link Page} row by row. Supports
 * comparisons, logical operators, arithmetic, and literal values with SQL three-valued NULL
 * semantics.
 */
public class ExpressionEvaluator {

  private final Map<String, Integer> columnIndexMap;
  private final Map<String, Type> columnTypeMap;

  public ExpressionEvaluator(Map<String, Integer> columnIndexMap, Map<String, Type> columnTypeMap) {
    this.columnIndexMap = columnIndexMap;
    this.columnTypeMap = columnTypeMap;
  }

  /**
   * Evaluate an expression for a given row, returning a typed value. Returns {@code null} for SQL
   * NULL, {@code Long} for BIGINT, {@code Double} for DOUBLE, {@code String} for VARCHAR, {@code
   * Boolean} for logical results.
   */
  public Object evaluate(Expression expr, Page page, int position) {
    if (expr instanceof Identifier) {
      return evaluateIdentifier((Identifier) expr, page, position);
    } else if (expr instanceof LongLiteral) {
      return ((LongLiteral) expr).getParsedValue();
    } else if (expr instanceof DoubleLiteral) {
      return ((DoubleLiteral) expr).getValue();
    } else if (expr instanceof DecimalLiteral) {
      return Double.parseDouble(((DecimalLiteral) expr).getValue());
    } else if (expr instanceof StringLiteral) {
      return ((StringLiteral) expr).getValue();
    } else if (expr instanceof BooleanLiteral) {
      return ((BooleanLiteral) expr).getValue();
    } else if (expr instanceof NullLiteral) {
      return null;
    } else if (expr instanceof ComparisonExpression) {
      return evaluateComparison((ComparisonExpression) expr, page, position);
    } else if (expr instanceof LogicalExpression) {
      return evaluateLogical((LogicalExpression) expr, page, position);
    } else if (expr instanceof NotExpression) {
      return evaluateNot((NotExpression) expr, page, position);
    } else if (expr instanceof ArithmeticBinaryExpression) {
      return evaluateArithmetic((ArithmeticBinaryExpression) expr, page, position);
    }
    throw new UnsupportedOperationException(
        "Unsupported expression type: " + expr.getClass().getSimpleName());
  }

  /**
   * Evaluate an expression as a boolean predicate. Returns {@code false} for SQL NULL (three-valued
   * logic collapse).
   */
  public boolean evaluateAsBoolean(Expression expr, Page page, int position) {
    Object result = evaluate(expr, page, position);
    if (result == null) {
      return false;
    }
    if (result instanceof Boolean) {
      return (Boolean) result;
    }
    throw new IllegalStateException(
        "Expected Boolean result but got " + result.getClass().getSimpleName());
  }

  private Object evaluateIdentifier(Identifier identifier, Page page, int position) {
    String name = identifier.getValue();
    Integer colIdx = columnIndexMap.get(name);
    if (colIdx == null) {
      throw new IllegalArgumentException(
          "Column '" + name + "' not found in column index map: " + columnIndexMap.keySet());
    }
    if (page.getBlock(colIdx).isNull(position)) {
      return null;
    }
    Type type = columnTypeMap.get(name);
    if (type instanceof BigintType) {
      return BigintType.BIGINT.getLong(page.getBlock(colIdx), position);
    } else if (type instanceof DoubleType) {
      return DoubleType.DOUBLE.getDouble(page.getBlock(colIdx), position);
    } else if (type instanceof VarcharType) {
      return VarcharType.VARCHAR.getSlice(page.getBlock(colIdx), position).toStringUtf8();
    }
    throw new UnsupportedOperationException("Unsupported column type: " + type);
  }

  private Object evaluateComparison(ComparisonExpression expr, Page page, int position) {
    Object left = evaluate(expr.getLeft(), page, position);
    Object right = evaluate(expr.getRight(), page, position);
    if (left == null || right == null) {
      return null;
    }
    int cmp = compareValues(left, right);
    switch (expr.getOperator()) {
      case EQUAL:
        return cmp == 0;
      case NOT_EQUAL:
        return cmp != 0;
      case LESS_THAN:
        return cmp < 0;
      case LESS_THAN_OR_EQUAL:
        return cmp <= 0;
      case GREATER_THAN:
        return cmp > 0;
      case GREATER_THAN_OR_EQUAL:
        return cmp >= 0;
      default:
        throw new UnsupportedOperationException("Unsupported operator: " + expr.getOperator());
    }
  }

  private Object evaluateLogical(LogicalExpression expr, Page page, int position) {
    switch (expr.getOperator()) {
      case AND:
        return evaluateAnd(expr, page, position);
      case OR:
        return evaluateOr(expr, page, position);
      default:
        throw new UnsupportedOperationException(
            "Unsupported logical operator: " + expr.getOperator());
    }
  }

  private Object evaluateAnd(LogicalExpression expr, Page page, int position) {
    boolean hasNull = false;
    for (Expression term : expr.getTerms()) {
      Object val = evaluate(term, page, position);
      if (val == null) {
        hasNull = true;
      } else if (!(Boolean) val) {
        return false;
      }
    }
    return hasNull ? null : true;
  }

  private Object evaluateOr(LogicalExpression expr, Page page, int position) {
    boolean hasNull = false;
    for (Expression term : expr.getTerms()) {
      Object val = evaluate(term, page, position);
      if (val == null) {
        hasNull = true;
      } else if ((Boolean) val) {
        return true;
      }
    }
    return hasNull ? null : false;
  }

  private Object evaluateNot(NotExpression expr, Page page, int position) {
    Object val = evaluate(expr.getValue(), page, position);
    if (val == null) {
      return null;
    }
    return !(Boolean) val;
  }

  private Object evaluateArithmetic(ArithmeticBinaryExpression expr, Page page, int position) {
    Object left = evaluate(expr.getLeft(), page, position);
    Object right = evaluate(expr.getRight(), page, position);
    if (left == null || right == null) {
      return null;
    }
    // Promote to double if either side is double
    if (left instanceof Double || right instanceof Double) {
      double l = toDouble(left);
      double r = toDouble(right);
      switch (expr.getOperator()) {
        case ADD:
          return l + r;
        case SUBTRACT:
          return l - r;
        case MULTIPLY:
          return l * r;
        case DIVIDE:
          return l / r;
        case MODULUS:
          return l % r;
        default:
          throw new UnsupportedOperationException(
              "Unsupported arithmetic operator: " + expr.getOperator());
      }
    }
    // Both are Long
    long l = (Long) left;
    long r = (Long) right;
    switch (expr.getOperator()) {
      case ADD:
        return l + r;
      case SUBTRACT:
        return l - r;
      case MULTIPLY:
        return l * r;
      case DIVIDE:
        return l / r;
      case MODULUS:
        return l % r;
      default:
        throw new UnsupportedOperationException(
            "Unsupported arithmetic operator: " + expr.getOperator());
    }
  }

  /**
   * Compare two values with type promotion. Numeric types (Long, Double) are compared numerically
   * with Long-to-Double promotion. Strings are compared lexicographically.
   */
  @SuppressWarnings("unchecked")
  private int compareValues(Object left, Object right) {
    if (left instanceof String && right instanceof String) {
      return ((String) left).compareTo((String) right);
    }
    if (left instanceof Number && right instanceof Number) {
      if (left instanceof Double || right instanceof Double) {
        return Double.compare(toDouble(left), toDouble(right));
      }
      return Long.compare((Long) left, (Long) right);
    }
    throw new UnsupportedOperationException(
        "Cannot compare "
            + left.getClass().getSimpleName()
            + " with "
            + right.getClass().getSimpleName());
  }

  private double toDouble(Object value) {
    return ((Number) value).doubleValue();
  }
}
