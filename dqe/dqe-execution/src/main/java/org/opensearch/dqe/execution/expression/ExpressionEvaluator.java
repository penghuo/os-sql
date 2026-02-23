/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.execution.expression;

import io.airlift.slice.Slice;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TinyintType;
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
import io.trino.sql.tree.DereferenceExpression;
import io.trino.sql.tree.DoubleLiteral;
import io.trino.sql.tree.Expression;
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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;
import org.opensearch.dqe.analyzer.type.TypedExpression;
import org.opensearch.dqe.parser.DqeErrorCode;
import org.opensearch.dqe.parser.DqeException;
import org.opensearch.dqe.types.DqeType;

/**
 * Evaluates expressions against rows in a Page. Used by FilterOperator and ProjectOperator.
 * Supports Phase 1 expression constructs: column references, literals, arithmetic, comparison,
 * boolean, IS NULL, BETWEEN, IN, LIKE, CAST, CASE, COALESCE, NULLIF.
 */
public class ExpressionEvaluator {

  private final TypedExpression typedExpression;
  private final Map<String, Integer> inputColumns;
  private final DqeType outputType;

  public ExpressionEvaluator(TypedExpression typedExpression, Map<String, Integer> inputColumns) {
    this.typedExpression =
        Objects.requireNonNull(typedExpression, "typedExpression must not be null");
    this.inputColumns = Map.copyOf(Objects.requireNonNull(inputColumns));
    this.outputType = typedExpression.getType();
  }

  /** Evaluates the expression for a single row. Returns null, Long, Double, Boolean, or String. */
  public Object evaluate(Page page, int position) {
    return eval(typedExpression.getExpression(), page, position);
  }

  /** Evaluates the expression for all rows and returns a Block. */
  public Block evaluateAll(Page page) {
    Type trinoType = outputType.getTrinoType();
    BlockBuilder builder = trinoType.createBlockBuilder(null, page.getPositionCount());
    for (int i = 0; i < page.getPositionCount(); i++) {
      Object value = evaluate(page, i);
      writeValue(builder, trinoType, value);
    }
    return builder.build();
  }

  public DqeType getOutputType() {
    return outputType;
  }

  // ---------- Core evaluation ----------

  private Object eval(Expression expr, Page page, int pos) {
    if (expr instanceof Identifier id) {
      return readColumn(id.getValue(), page, pos);
    }
    if (expr instanceof DereferenceExpression deref) {
      return readColumn(derefToString(deref), page, pos);
    }
    if (expr instanceof LongLiteral lit) {
      return lit.getParsedValue();
    }
    if (expr instanceof DoubleLiteral lit) {
      return lit.getValue();
    }
    if (expr instanceof DecimalLiteral lit) {
      return Double.parseDouble(lit.getValue());
    }
    if (expr instanceof StringLiteral lit) {
      return lit.getValue();
    }
    if (expr instanceof BooleanLiteral lit) {
      return lit.getValue();
    }
    if (expr instanceof NullLiteral) {
      return null;
    }
    if (expr instanceof ComparisonExpression cmp) {
      return evalComparison(cmp, page, pos);
    }
    if (expr instanceof ArithmeticBinaryExpression arith) {
      return evalArithmeticBinary(arith, page, pos);
    }
    if (expr instanceof ArithmeticUnaryExpression unary) {
      return evalArithmeticUnary(unary, page, pos);
    }
    if (expr instanceof LogicalExpression logical) {
      return evalLogical(logical, page, pos);
    }
    if (expr instanceof NotExpression not) {
      Object val = eval(not.getValue(), page, pos);
      if (val == null) {
        return null;
      }
      return !toBoolean(val);
    }
    if (expr instanceof IsNullPredicate isNull) {
      return eval(isNull.getValue(), page, pos) == null;
    }
    if (expr instanceof IsNotNullPredicate isNotNull) {
      return eval(isNotNull.getValue(), page, pos) != null;
    }
    if (expr instanceof BetweenPredicate between) {
      return evalBetween(between, page, pos);
    }
    if (expr instanceof InPredicate in) {
      return evalIn(in, page, pos);
    }
    if (expr instanceof LikePredicate like) {
      return evalLike(like, page, pos);
    }
    if (expr instanceof Cast cast) {
      return evalCast(cast, page, pos);
    }
    if (expr instanceof SearchedCaseExpression sce) {
      return evalSearchedCase(sce, page, pos);
    }
    if (expr instanceof SimpleCaseExpression simple) {
      return evalSimpleCase(simple, page, pos);
    }
    if (expr instanceof CoalesceExpression coalesce) {
      for (Expression operand : coalesce.getOperands()) {
        Object val = eval(operand, page, pos);
        if (val != null) {
          return val;
        }
      }
      return null;
    }
    if (expr instanceof NullIfExpression nullIf) {
      Object first = eval(nullIf.getFirst(), page, pos);
      Object second = eval(nullIf.getSecond(), page, pos);
      if (first != null && first.equals(second)) {
        return null;
      }
      return first;
    }
    throw new DqeException(
        "Unsupported expression type: " + expr.getClass().getSimpleName(),
        DqeErrorCode.EXECUTION_ERROR);
  }

  private Object readColumn(String columnName, Page page, int pos) {
    Integer channelIndex = inputColumns.get(columnName);
    if (channelIndex == null) {
      throw new DqeException(
          "Column not found in input: " + columnName, DqeErrorCode.EXECUTION_ERROR);
    }
    Block block = page.getBlock(channelIndex);
    if (block.isNull(pos)) {
      return null;
    }
    Type type =
        block.getClass().getSimpleName().contains("Varchar")
            ? VarcharType.VARCHAR
            : guessType(block);
    return readTypedValue(block, pos);
  }

  private Object readTypedValue(Block block, int pos) {
    if (block.isNull(pos)) {
      return null;
    }
    // Try slice first (VARCHAR) since VariableWidthBlock.getLong doesn't throw
    // UnsupportedOperationException but instead throws IndexOutOfBoundsException for short strings
    try {
      Slice slice = block.getSlice(pos, 0, block.getSliceLength(pos));
      return slice.toStringUtf8();
    } catch (UnsupportedOperationException | IndexOutOfBoundsException e) {
      // not a slice block
    }
    try {
      // Try long (covers BIGINT, INTEGER, SMALLINT, TINYINT, TIMESTAMP, REAL)
      long longVal = block.getLong(pos, 0);
      return longVal;
    } catch (UnsupportedOperationException | IndexOutOfBoundsException e) {
      // not a long block
    }
    try {
      // Boolean
      byte byteVal = block.getByte(pos, 0);
      return byteVal != 0;
    } catch (UnsupportedOperationException | IndexOutOfBoundsException e) {
      // not a byte block
    }
    return null;
  }

  private Type guessType(Block block) {
    return VarcharType.VARCHAR; // fallback
  }

  private Object evalComparison(ComparisonExpression cmp, Page page, int pos) {
    Object left = eval(cmp.getLeft(), page, pos);
    Object right = eval(cmp.getRight(), page, pos);
    if (left == null || right == null) {
      return null; // SQL three-valued logic
    }
    int result = compareValues(left, right);
    return switch (cmp.getOperator()) {
      case EQUAL -> result == 0;
      case NOT_EQUAL -> result != 0;
      case LESS_THAN -> result < 0;
      case LESS_THAN_OR_EQUAL -> result <= 0;
      case GREATER_THAN -> result > 0;
      case GREATER_THAN_OR_EQUAL -> result >= 0;
      default ->
          throw new DqeException(
              "Unknown comparison operator: " + cmp.getOperator(), DqeErrorCode.EXECUTION_ERROR);
    };
  }

  private Object evalArithmeticBinary(ArithmeticBinaryExpression arith, Page page, int pos) {
    Object left = eval(arith.getLeft(), page, pos);
    Object right = eval(arith.getRight(), page, pos);
    if (left == null || right == null) {
      return null;
    }
    double l = toDouble(left);
    double r = toDouble(right);
    return switch (arith.getOperator()) {
      case ADD -> promoteResult(left, right, l + r);
      case SUBTRACT -> promoteResult(left, right, l - r);
      case MULTIPLY -> promoteResult(left, right, l * r);
      case DIVIDE -> {
        if (r == 0) {
          throw new DqeException("Division by zero", DqeErrorCode.EXECUTION_ERROR);
        }
        yield promoteResult(left, right, l / r);
      }
      case MODULUS -> {
        if (r == 0) {
          throw new DqeException("Division by zero", DqeErrorCode.EXECUTION_ERROR);
        }
        yield promoteResult(left, right, l % r);
      }
    };
  }

  private Object evalArithmeticUnary(ArithmeticUnaryExpression unary, Page page, int pos) {
    Object val = eval(unary.getValue(), page, pos);
    if (val == null) {
      return null;
    }
    if (unary.getSign() == ArithmeticUnaryExpression.Sign.MINUS) {
      if (val instanceof Long l) {
        return -l;
      }
      return -toDouble(val);
    }
    return val;
  }

  private Object evalLogical(LogicalExpression logical, Page page, int pos) {
    List<Expression> terms = logical.getTerms();
    if (logical.getOperator() == LogicalExpression.Operator.AND) {
      Boolean result = true;
      for (Expression term : terms) {
        Object val = eval(term, page, pos);
        if (val == null) {
          result = null;
        } else if (!toBoolean(val)) {
          return false;
        }
      }
      return result;
    } else {
      // OR
      Boolean result = false;
      for (Expression term : terms) {
        Object val = eval(term, page, pos);
        if (val == null) {
          result = null;
        } else if (toBoolean(val)) {
          return true;
        }
      }
      return result;
    }
  }

  private Object evalBetween(BetweenPredicate between, Page page, int pos) {
    Object val = eval(between.getValue(), page, pos);
    Object min = eval(between.getMin(), page, pos);
    Object max = eval(between.getMax(), page, pos);
    if (val == null || min == null || max == null) {
      return null;
    }
    return compareValues(val, min) >= 0 && compareValues(val, max) <= 0;
  }

  private Object evalIn(InPredicate in, Page page, int pos) {
    Object val = eval(in.getValue(), page, pos);
    if (val == null) {
      return null;
    }
    if (in.getValueList() instanceof InListExpression inList) {
      boolean hasNull = false;
      for (Expression elem : inList.getValues()) {
        Object elemVal = eval(elem, page, pos);
        if (elemVal == null) {
          hasNull = true;
        } else if (compareValues(val, elemVal) == 0) {
          return true;
        }
      }
      return hasNull ? null : false;
    }
    return false;
  }

  private Object evalLike(LikePredicate like, Page page, int pos) {
    Object val = eval(like.getValue(), page, pos);
    if (val == null) {
      return null;
    }
    String str = val.toString();
    String pattern = ((StringLiteral) like.getPattern()).getValue();
    String regex = likeToRegex(pattern);
    return Pattern.matches(regex, str);
  }

  private Object evalCast(Cast cast, Page page, int pos) {
    Object val = eval(cast.getExpression(), page, pos);
    if (val == null) {
      return null;
    }
    String targetType = cast.getType().toString().toUpperCase();
    try {
      return castValue(val, targetType);
    } catch (Exception e) {
      if (cast.isSafe()) {
        return null; // TRY_CAST returns null on failure
      }
      throw new DqeException(
          "Cast failed: cannot cast " + val + " to " + targetType, DqeErrorCode.EXECUTION_ERROR, e);
    }
  }

  private Object evalSearchedCase(SearchedCaseExpression sce, Page page, int pos) {
    for (WhenClause when : sce.getWhenClauses()) {
      Object cond = eval(when.getOperand(), page, pos);
      if (cond != null && toBoolean(cond)) {
        return eval(when.getResult(), page, pos);
      }
    }
    return sce.getDefaultValue().isPresent() ? eval(sce.getDefaultValue().get(), page, pos) : null;
  }

  private Object evalSimpleCase(SimpleCaseExpression simple, Page page, int pos) {
    Object operand = eval(simple.getOperand(), page, pos);
    if (operand == null) {
      return simple.getDefaultValue().isPresent()
          ? eval(simple.getDefaultValue().get(), page, pos)
          : null;
    }
    for (WhenClause when : simple.getWhenClauses()) {
      Object whenVal = eval(when.getOperand(), page, pos);
      if (whenVal != null && compareValues(operand, whenVal) == 0) {
        return eval(when.getResult(), page, pos);
      }
    }
    return simple.getDefaultValue().isPresent()
        ? eval(simple.getDefaultValue().get(), page, pos)
        : null;
  }

  // ---------- Helpers ----------

  @SuppressWarnings("unchecked")
  private static int compareValues(Object a, Object b) {
    if (a instanceof Number && b instanceof Number) {
      return Double.compare(((Number) a).doubleValue(), ((Number) b).doubleValue());
    }
    if (a instanceof String && b instanceof String) {
      return ((String) a).compareTo((String) b);
    }
    if (a instanceof Boolean && b instanceof Boolean) {
      return Boolean.compare((Boolean) a, (Boolean) b);
    }
    // Cross-type: coerce to string
    return a.toString().compareTo(b.toString());
  }

  private static double toDouble(Object val) {
    if (val instanceof Number n) {
      return n.doubleValue();
    }
    if (val instanceof String s) {
      return Double.parseDouble(s);
    }
    throw new DqeException("Cannot convert to number: " + val, DqeErrorCode.EXECUTION_ERROR);
  }

  private static boolean toBoolean(Object val) {
    if (val instanceof Boolean b) {
      return b;
    }
    if (val instanceof Number n) {
      return n.longValue() != 0;
    }
    if (val instanceof String s) {
      return Boolean.parseBoolean(s);
    }
    return false;
  }

  private static Object promoteResult(Object left, Object right, double result) {
    // If both inputs were integral and result is integral, return Long
    if (left instanceof Long && right instanceof Long) {
      long longResult = (long) result;
      if ((double) longResult == result) {
        return longResult;
      }
    }
    return result;
  }

  private static Object castValue(Object val, String targetType) {
    return switch (targetType) {
      case "VARCHAR", "CHAR" -> val.toString();
      case "BIGINT" -> {
        if (val instanceof Number n) yield n.longValue();
        if (val instanceof Boolean b) yield b ? 1L : 0L;
        yield Long.parseLong(val.toString().trim());
      }
      case "INTEGER", "INT" -> {
        if (val instanceof Number n) yield (long) n.intValue();
        yield (long) Integer.parseInt(val.toString().trim());
      }
      case "DOUBLE" -> {
        if (val instanceof Number n) yield n.doubleValue();
        yield Double.parseDouble(val.toString().trim());
      }
      case "BOOLEAN" -> {
        if (val instanceof Boolean b) yield b;
        if (val instanceof Number n) yield n.longValue() != 0;
        yield Boolean.parseBoolean(val.toString().trim());
      }
      case "REAL" -> {
        if (val instanceof Number n) yield n.doubleValue();
        yield (double) Float.parseFloat(val.toString().trim());
      }
      default -> val.toString();
    };
  }

  private static String likeToRegex(String pattern) {
    StringBuilder sb = new StringBuilder("^");
    for (int i = 0; i < pattern.length(); i++) {
      char c = pattern.charAt(i);
      if (c == '%') {
        sb.append(".*");
      } else if (c == '_') {
        sb.append(".");
      } else if ("\\[]{}().*+?$^|".indexOf(c) >= 0) {
        sb.append('\\').append(c);
      } else {
        sb.append(c);
      }
    }
    sb.append("$");
    return sb.toString();
  }

  private static String derefToString(DereferenceExpression deref) {
    Expression base = deref.getBase();
    String field = deref.getField().orElseThrow().getValue();
    if (base instanceof Identifier id) {
      return id.getValue() + "." + field;
    }
    if (base instanceof DereferenceExpression innerDeref) {
      return derefToString(innerDeref) + "." + field;
    }
    return base + "." + field;
  }

  public static void writeValue(BlockBuilder builder, Type type, Object value) {
    if (value == null) {
      builder.appendNull();
      return;
    }
    if (type instanceof VarcharType) {
      io.airlift.slice.Slice slice = io.airlift.slice.Slices.utf8Slice(value.toString());
      type.writeSlice(builder, slice);
    } else if (type instanceof BooleanType) {
      type.writeBoolean(builder, toBoolean(value));
    } else if (type instanceof DoubleType) {
      type.writeDouble(builder, toDouble(value));
    } else if (type instanceof BigintType
        || type instanceof IntegerType
        || type instanceof SmallintType
        || type instanceof TinyintType
        || type instanceof TimestampType) {
      long longVal;
      if (value instanceof Number n) {
        longVal = n.longValue();
      } else {
        longVal = Long.parseLong(value.toString());
      }
      type.writeLong(builder, longVal);
    } else if (type instanceof RealType) {
      float fVal;
      if (value instanceof Number n) {
        fVal = n.floatValue();
      } else {
        fVal = Float.parseFloat(value.toString());
      }
      type.writeLong(builder, Float.floatToIntBits(fVal));
    } else {
      // Fallback: write as varchar
      io.airlift.slice.Slice slice = io.airlift.slice.Slices.utf8Slice(value.toString());
      VarcharType.VARCHAR.writeSlice(builder, slice);
    }
  }
}
