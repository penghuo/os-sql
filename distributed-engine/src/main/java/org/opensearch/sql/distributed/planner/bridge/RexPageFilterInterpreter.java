/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.planner.bridge;

import java.math.BigDecimal;
import java.util.List;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.distributed.data.Block;
import org.opensearch.sql.distributed.data.BooleanArrayBlock;
import org.opensearch.sql.distributed.data.DictionaryBlock;
import org.opensearch.sql.distributed.data.DoubleArrayBlock;
import org.opensearch.sql.distributed.data.IntArrayBlock;
import org.opensearch.sql.distributed.data.LongArrayBlock;
import org.opensearch.sql.distributed.data.Page;
import org.opensearch.sql.distributed.data.RunLengthEncodedBlock;
import org.opensearch.sql.distributed.data.VariableWidthBlock;
import org.opensearch.sql.distributed.operator.PageFilter;

/**
 * Interprets Calcite {@link RexNode} expressions against {@link Page} data to produce boolean[]
 * filter masks for {@link org.opensearch.sql.distributed.operator.FilterAndProjectOperator}.
 *
 * <p>Phase 1 supports: column references (RexInputRef), literals (RexLiteral), comparison operators
 * (=, !=, <, >, <=, >=), boolean operators (AND, OR, NOT), IS NULL, IS NOT NULL.
 */
public final class RexPageFilterInterpreter {

  private RexPageFilterInterpreter() {}

  /**
   * Converts a RexNode predicate to a PageFilter that evaluates it against Pages.
   *
   * @param predicate the Calcite predicate expression
   * @return a PageFilter implementation
   */
  public static PageFilter toPageFilter(RexNode predicate) {
    return page -> evaluate(predicate, page);
  }

  /**
   * Evaluates a RexNode predicate against all positions of a Page.
   *
   * @param node the expression to evaluate
   * @param page the input page
   * @return boolean array where true means the row passes the filter
   */
  static boolean[] evaluate(RexNode node, Page page) {
    int positionCount = page.getPositionCount();

    if (node instanceof RexCall call) {
      return evaluateCall(call, page, positionCount);
    }
    if (node instanceof RexInputRef ref) {
      // Boolean column as a predicate
      return evaluateBooleanColumn(ref.getIndex(), page, positionCount);
    }
    if (node instanceof RexLiteral literal) {
      boolean value = Boolean.TRUE.equals(RexLiteral.booleanValue(literal));
      boolean[] result = new boolean[positionCount];
      if (value) {
        java.util.Arrays.fill(result, true);
      }
      return result;
    }
    throw new UnsupportedOperationException(
        "Unsupported filter expression type: " + node.getClass().getSimpleName());
  }

  private static boolean[] evaluateCall(RexCall call, Page page, int positionCount) {
    SqlKind kind = call.getKind();

    switch (kind) {
      case AND:
        return evaluateAnd(call.getOperands(), page, positionCount);
      case OR:
        return evaluateOr(call.getOperands(), page, positionCount);
      case NOT:
        return evaluateNot(call.getOperands().get(0), page, positionCount);

      case IS_NULL:
        return evaluateIsNull(call.getOperands().get(0), page, positionCount);
      case IS_NOT_NULL:
        return evaluateIsNotNull(call.getOperands().get(0), page, positionCount);

      case EQUALS:
      case NOT_EQUALS:
      case LESS_THAN:
      case LESS_THAN_OR_EQUAL:
      case GREATER_THAN:
      case GREATER_THAN_OR_EQUAL:
        return evaluateComparison(call, page, positionCount);

      case IS_TRUE:
        return evaluate(call.getOperands().get(0), page);
      case IS_FALSE:
        return evaluateNot(call.getOperands().get(0), page, positionCount);
      case IS_NOT_TRUE:
        return evaluateNot(call.getOperands().get(0), page, positionCount);
      case IS_NOT_FALSE:
        return evaluate(call.getOperands().get(0), page);

      default:
        throw new UnsupportedOperationException(
            "Unsupported filter operator: "
                + call.getOperator().getName()
                + " (kind: "
                + kind
                + ")");
    }
  }

  private static boolean[] evaluateAnd(List<RexNode> operands, Page page, int positionCount) {
    boolean[] result = evaluate(operands.get(0), page);
    for (int i = 1; i < operands.size(); i++) {
      boolean[] right = evaluate(operands.get(i), page);
      for (int j = 0; j < positionCount; j++) {
        result[j] = result[j] && right[j];
      }
    }
    return result;
  }

  private static boolean[] evaluateOr(List<RexNode> operands, Page page, int positionCount) {
    boolean[] result = evaluate(operands.get(0), page);
    for (int i = 1; i < operands.size(); i++) {
      boolean[] right = evaluate(operands.get(i), page);
      for (int j = 0; j < positionCount; j++) {
        result[j] = result[j] || right[j];
      }
    }
    return result;
  }

  private static boolean[] evaluateNot(RexNode operand, Page page, int positionCount) {
    boolean[] inner = evaluate(operand, page);
    for (int i = 0; i < positionCount; i++) {
      inner[i] = !inner[i];
    }
    return inner;
  }

  private static boolean[] evaluateIsNull(RexNode operand, Page page, int positionCount) {
    boolean[] result = new boolean[positionCount];
    if (operand instanceof RexInputRef ref) {
      Block block = page.getBlock(ref.getIndex());
      for (int i = 0; i < positionCount; i++) {
        result[i] = block.isNull(i);
      }
    }
    return result;
  }

  private static boolean[] evaluateIsNotNull(RexNode operand, Page page, int positionCount) {
    boolean[] result = new boolean[positionCount];
    if (operand instanceof RexInputRef ref) {
      Block block = page.getBlock(ref.getIndex());
      for (int i = 0; i < positionCount; i++) {
        result[i] = !block.isNull(i);
      }
    }
    return result;
  }

  /**
   * Evaluates comparison operations (=, !=, <, >, <=, >=). Supports column-vs-literal and
   * column-vs-column comparisons.
   */
  private static boolean[] evaluateComparison(RexCall call, Page page, int positionCount) {
    RexNode left = call.getOperands().get(0);
    RexNode right = call.getOperands().get(1);
    SqlKind kind = call.getKind();
    boolean[] result = new boolean[positionCount];

    if (left instanceof RexInputRef leftRef && right instanceof RexLiteral rightLit) {
      evaluateColumnVsLiteral(leftRef.getIndex(), rightLit, kind, page, result, positionCount);
    } else if (left instanceof RexLiteral leftLit && right instanceof RexInputRef rightRef) {
      // Flip: literal op column → column flipped-op literal
      evaluateColumnVsLiteral(
          rightRef.getIndex(), leftLit, flipKind(kind), page, result, positionCount);
    } else if (left instanceof RexInputRef leftRef && right instanceof RexInputRef rightRef) {
      evaluateColumnVsColumn(
          leftRef.getIndex(), rightRef.getIndex(), kind, page, result, positionCount);
    } else {
      throw new UnsupportedOperationException(
          "Unsupported comparison operand types: "
              + left.getClass().getSimpleName()
              + " vs "
              + right.getClass().getSimpleName());
    }
    return result;
  }

  private static void evaluateColumnVsLiteral(
      int channel, RexLiteral literal, SqlKind kind, Page page, boolean[] result, int count) {
    Block block = page.getBlock(channel);
    Block resolved = resolveValueBlock(block);

    if (resolved instanceof LongArrayBlock longBlock) {
      long litVal = toLong(literal);
      for (int i = 0; i < count; i++) {
        if (block.isNull(i)) {
          result[i] = false;
        } else {
          int pos = resolvePosition(block, i);
          result[i] = compareLong(longBlock.getLong(pos), litVal, kind);
        }
      }
    } else if (resolved instanceof DoubleArrayBlock doubleBlock) {
      double litVal = toDouble(literal);
      for (int i = 0; i < count; i++) {
        if (block.isNull(i)) {
          result[i] = false;
        } else {
          int pos = resolvePosition(block, i);
          result[i] = compareDouble(doubleBlock.getDouble(pos), litVal, kind);
        }
      }
    } else if (resolved instanceof IntArrayBlock intBlock) {
      int litVal = toInt(literal);
      for (int i = 0; i < count; i++) {
        if (block.isNull(i)) {
          result[i] = false;
        } else {
          int pos = resolvePosition(block, i);
          result[i] = compareInt(intBlock.getInt(pos), litVal, kind);
        }
      }
    } else if (resolved instanceof VariableWidthBlock varBlock) {
      byte[] litVal = toBytes(literal);
      for (int i = 0; i < count; i++) {
        if (block.isNull(i)) {
          result[i] = false;
        } else {
          int pos = resolvePosition(block, i);
          result[i] = compareBytes(varBlock.getSlice(pos), litVal, kind);
        }
      }
    } else if (resolved instanceof BooleanArrayBlock boolBlock) {
      boolean litVal = Boolean.TRUE.equals(RexLiteral.booleanValue(literal));
      for (int i = 0; i < count; i++) {
        if (block.isNull(i)) {
          result[i] = false;
        } else {
          int pos = resolvePosition(block, i);
          result[i] = compareBoolean(boolBlock.getBoolean(pos), litVal, kind);
        }
      }
    } else {
      throw new UnsupportedOperationException(
          "Unsupported block type for comparison: " + resolved.getClass().getSimpleName());
    }
  }

  private static void evaluateColumnVsColumn(
      int leftChannel, int rightChannel, SqlKind kind, Page page, boolean[] result, int count) {
    Block leftBlock = page.getBlock(leftChannel);
    Block rightBlock = page.getBlock(rightChannel);
    Block leftResolved = resolveValueBlock(leftBlock);

    if (leftResolved instanceof LongArrayBlock leftLong) {
      Block rightResolved = resolveValueBlock(rightBlock);
      LongArrayBlock rightLong = (LongArrayBlock) rightResolved;
      for (int i = 0; i < count; i++) {
        if (leftBlock.isNull(i) || rightBlock.isNull(i)) {
          result[i] = false;
        } else {
          long lv = leftLong.getLong(resolvePosition(leftBlock, i));
          long rv = rightLong.getLong(resolvePosition(rightBlock, i));
          result[i] = compareLong(lv, rv, kind);
        }
      }
    } else if (leftResolved instanceof DoubleArrayBlock leftDouble) {
      Block rightResolved = resolveValueBlock(rightBlock);
      DoubleArrayBlock rightDouble = (DoubleArrayBlock) rightResolved;
      for (int i = 0; i < count; i++) {
        if (leftBlock.isNull(i) || rightBlock.isNull(i)) {
          result[i] = false;
        } else {
          double lv = leftDouble.getDouble(resolvePosition(leftBlock, i));
          double rv = rightDouble.getDouble(resolvePosition(rightBlock, i));
          result[i] = compareDouble(lv, rv, kind);
        }
      }
    } else {
      throw new UnsupportedOperationException(
          "Column-vs-column comparison not supported for: "
              + leftResolved.getClass().getSimpleName());
    }
  }

  private static boolean[] evaluateBooleanColumn(int channel, Page page, int positionCount) {
    boolean[] result = new boolean[positionCount];
    Block block = page.getBlock(channel);
    Block resolved = resolveValueBlock(block);
    if (resolved instanceof BooleanArrayBlock boolBlock) {
      for (int i = 0; i < positionCount; i++) {
        result[i] = !block.isNull(i) && boolBlock.getBoolean(resolvePosition(block, i));
      }
    }
    return result;
  }

  // --- Comparison helpers ---

  private static boolean compareLong(long left, long right, SqlKind kind) {
    return applyComparison(Long.compare(left, right), kind);
  }

  private static boolean compareDouble(double left, double right, SqlKind kind) {
    return applyComparison(Double.compare(left, right), kind);
  }

  private static boolean compareInt(int left, int right, SqlKind kind) {
    return applyComparison(Integer.compare(left, right), kind);
  }

  private static boolean compareBytes(byte[] left, byte[] right, SqlKind kind) {
    return applyComparison(java.util.Arrays.compare(left, right), kind);
  }

  private static boolean compareBoolean(boolean left, boolean right, SqlKind kind) {
    return applyComparison(Boolean.compare(left, right), kind);
  }

  private static boolean applyComparison(int cmp, SqlKind kind) {
    switch (kind) {
      case EQUALS:
        return cmp == 0;
      case NOT_EQUALS:
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
        throw new IllegalArgumentException("Not a comparison kind: " + kind);
    }
  }

  private static SqlKind flipKind(SqlKind kind) {
    switch (kind) {
      case LESS_THAN:
        return SqlKind.GREATER_THAN;
      case LESS_THAN_OR_EQUAL:
        return SqlKind.GREATER_THAN_OR_EQUAL;
      case GREATER_THAN:
        return SqlKind.LESS_THAN;
      case GREATER_THAN_OR_EQUAL:
        return SqlKind.LESS_THAN_OR_EQUAL;
      default:
        return kind; // EQUALS and NOT_EQUALS are symmetric
    }
  }

  // --- Literal extraction ---

  private static long toLong(RexLiteral literal) {
    SqlTypeName typeName = literal.getTypeName();
    if (typeName == SqlTypeName.BIGINT
        || typeName == SqlTypeName.INTEGER
        || typeName == SqlTypeName.SMALLINT
        || typeName == SqlTypeName.TINYINT) {
      return literal.getValueAs(Long.class);
    }
    if (typeName == SqlTypeName.DECIMAL) {
      return literal.getValueAs(BigDecimal.class).longValueExact();
    }
    return literal.getValueAs(Number.class).longValue();
  }

  private static double toDouble(RexLiteral literal) {
    return literal.getValueAs(Number.class).doubleValue();
  }

  private static int toInt(RexLiteral literal) {
    return literal.getValueAs(Number.class).intValue();
  }

  private static byte[] toBytes(RexLiteral literal) {
    String str = literal.getValueAs(String.class);
    return str != null ? str.getBytes(java.nio.charset.StandardCharsets.UTF_8) : new byte[0];
  }

  // --- Block resolution helpers ---

  private static Block resolveValueBlock(Block block) {
    if (block instanceof DictionaryBlock dict) {
      return dict.getDictionary();
    }
    if (block instanceof RunLengthEncodedBlock rle) {
      return rle.getValue();
    }
    return block;
  }

  private static int resolvePosition(Block block, int position) {
    if (block instanceof DictionaryBlock dict) {
      return dict.getId(position);
    }
    if (block instanceof RunLengthEncodedBlock) {
      return 0;
    }
    return position;
  }
}
