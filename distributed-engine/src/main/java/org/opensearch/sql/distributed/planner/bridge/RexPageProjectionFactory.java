/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.planner.bridge;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
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
import org.opensearch.sql.distributed.operator.ColumnProjection;
import org.opensearch.sql.distributed.operator.PageProjection;

/**
 * Converts a list of Calcite {@link RexNode} projections to a list of {@link PageProjection}
 * instances for use by {@link org.opensearch.sql.distributed.operator.FilterAndProjectOperator}.
 *
 * <p>Phase 1 supports: column pass-through (RexInputRef), literal constants (RexLiteral), basic
 * arithmetic (+, -, *, /), and CAST operations.
 */
public final class RexPageProjectionFactory {

  private RexPageProjectionFactory() {}

  /**
   * Converts a list of RexNode projections to PageProjection instances.
   *
   * @param projections the Calcite projection expressions
   * @return list of PageProjection instances
   */
  public static List<PageProjection> createProjections(List<RexNode> projections) {
    List<PageProjection> result = new ArrayList<>(projections.size());
    for (RexNode rex : projections) {
      result.add(createProjection(rex));
    }
    return result;
  }

  /**
   * Creates a single PageProjection from a RexNode.
   *
   * @param rex the Calcite expression
   * @return a PageProjection implementation
   */
  public static PageProjection createProjection(RexNode rex) {
    if (rex instanceof RexInputRef ref) {
      return new ColumnProjection(ref.getIndex());
    }
    if (rex instanceof RexLiteral literal) {
      return new LiteralProjection(literal);
    }
    if (rex instanceof RexCall call) {
      return createCallProjection(call);
    }
    throw new UnsupportedOperationException(
        "Unsupported projection expression type: " + rex.getClass().getSimpleName());
  }

  private static PageProjection createCallProjection(RexCall call) {
    SqlKind kind = call.getKind();
    switch (kind) {
      case PLUS:
      case MINUS:
      case TIMES:
      case DIVIDE:
        return new ArithmeticProjection(call);
      case CAST:
        // For simple casts, just project the inner expression
        return createProjection(call.getOperands().get(0));
      default:
        throw new UnsupportedOperationException(
            "Unsupported projection operator: "
                + call.getOperator().getName()
                + " (kind: "
                + kind
                + ")");
    }
  }

  // --- Projection implementations ---

  /** Projects a constant literal value for every selected position. */
  static class LiteralProjection implements PageProjection {
    private final RexLiteral literal;

    LiteralProjection(RexLiteral literal) {
      this.literal = literal;
    }

    @Override
    public Block project(Page page, boolean[] selectedPositions) {
      int selectedCount = countSelected(selectedPositions);
      SqlTypeName typeName = literal.getTypeName();

      if (RexLiteral.isNullLiteral(literal)) {
        return buildNullBlock(selectedCount, typeName);
      }

      switch (typeName) {
        case BIGINT:
        case INTEGER:
        case SMALLINT:
        case TINYINT:
          {
            long value = literal.getValueAs(Long.class);
            long[] values = new long[selectedCount];
            java.util.Arrays.fill(values, value);
            return new LongArrayBlock(selectedCount, Optional.empty(), values);
          }
        case DOUBLE:
        case FLOAT:
        case REAL:
        case DECIMAL:
          {
            double value = literal.getValueAs(Number.class).doubleValue();
            double[] values = new double[selectedCount];
            java.util.Arrays.fill(values, value);
            return new DoubleArrayBlock(selectedCount, Optional.empty(), values);
          }
        case BOOLEAN:
          {
            boolean value = Boolean.TRUE.equals(literal.getValueAs(Boolean.class));
            boolean[] values = new boolean[selectedCount];
            java.util.Arrays.fill(values, value);
            return new BooleanArrayBlock(selectedCount, Optional.empty(), values);
          }
        case CHAR:
        case VARCHAR:
          {
            String str = literal.getValueAs(String.class);
            byte[] bytes =
                str != null ? str.getBytes(java.nio.charset.StandardCharsets.UTF_8) : new byte[0];
            return buildRepeatedStringBlock(bytes, selectedCount);
          }
        default:
          throw new UnsupportedOperationException(
              "Unsupported literal type for projection: " + typeName);
      }
    }

    private static Block buildNullBlock(int count, SqlTypeName typeName) {
      boolean[] nulls = new boolean[count];
      java.util.Arrays.fill(nulls, true);
      // Default to LongArrayBlock for null values
      return new LongArrayBlock(count, Optional.of(nulls), new long[count]);
    }

    private static VariableWidthBlock buildRepeatedStringBlock(byte[] value, int count) {
      byte[] slice = new byte[value.length * count];
      int[] offsets = new int[count + 1];
      for (int i = 0; i < count; i++) {
        System.arraycopy(value, 0, slice, i * value.length, value.length);
        offsets[i + 1] = (i + 1) * value.length;
      }
      return new VariableWidthBlock(count, slice, offsets, Optional.empty());
    }
  }

  /** Projects the result of an arithmetic expression (+, -, *, /). */
  static class ArithmeticProjection implements PageProjection {
    private final RexCall call;

    ArithmeticProjection(RexCall call) {
      this.call = call;
    }

    @Override
    public Block project(Page page, boolean[] selectedPositions) {
      int selectedCount = countSelected(selectedPositions);
      int[] positions = selectedIndices(selectedPositions);

      RexNode leftRex = call.getOperands().get(0);
      RexNode rightRex = call.getOperands().get(1);
      SqlKind op = call.getKind();

      // Compute result as double[] for simplicity
      double[] results = new double[selectedCount];
      boolean[] nulls = null;
      boolean hasNull = false;

      for (int i = 0; i < selectedCount; i++) {
        int pos = positions[i];
        Double leftVal = extractNumericValue(leftRex, page, pos);
        Double rightVal = extractNumericValue(rightRex, page, pos);

        if (leftVal == null || rightVal == null) {
          if (nulls == null) {
            nulls = new boolean[selectedCount];
          }
          nulls[i] = true;
          hasNull = true;
        } else {
          results[i] = applyArithmetic(leftVal, rightVal, op);
        }
      }

      return new DoubleArrayBlock(
          selectedCount, hasNull ? Optional.of(nulls) : Optional.empty(), results);
    }

    private static Double extractNumericValue(RexNode rex, Page page, int position) {
      if (rex instanceof RexInputRef ref) {
        Block block = page.getBlock(ref.getIndex());
        if (block.isNull(position)) {
          return null;
        }
        Block resolved = resolveValueBlock(block);
        int resolvedPos = resolvePosition(block, position);
        if (resolved instanceof LongArrayBlock longBlock) {
          return (double) longBlock.getLong(resolvedPos);
        }
        if (resolved instanceof DoubleArrayBlock doubleBlock) {
          return doubleBlock.getDouble(resolvedPos);
        }
        if (resolved instanceof IntArrayBlock intBlock) {
          return (double) intBlock.getInt(resolvedPos);
        }
        throw new UnsupportedOperationException(
            "Cannot extract numeric from: " + resolved.getClass().getSimpleName());
      }
      if (rex instanceof RexLiteral literal) {
        if (RexLiteral.isNullLiteral(literal)) {
          return null;
        }
        return literal.getValueAs(Number.class).doubleValue();
      }
      if (rex instanceof RexCall nestedCall) {
        // Recursively evaluate nested arithmetic
        ArithmeticProjection nested = new ArithmeticProjection(nestedCall);
        boolean[] singleSelected = new boolean[page.getPositionCount()];
        singleSelected[position] = true;
        Block result = nested.project(page, singleSelected);
        if (result.isNull(0)) {
          return null;
        }
        return ((DoubleArrayBlock) result).getDouble(0);
      }
      throw new UnsupportedOperationException(
          "Cannot extract numeric from expression: " + rex.getClass().getSimpleName());
    }

    private static double applyArithmetic(double left, double right, SqlKind op) {
      switch (op) {
        case PLUS:
          return left + right;
        case MINUS:
          return left - right;
        case TIMES:
          return left * right;
        case DIVIDE:
          if (right == 0.0) {
            return Double.NaN;
          }
          return left / right;
        default:
          throw new IllegalArgumentException("Not an arithmetic operator: " + op);
      }
    }
  }

  // --- Utility methods ---

  static int countSelected(boolean[] selected) {
    int count = 0;
    for (boolean s : selected) {
      if (s) count++;
    }
    return count;
  }

  static int[] selectedIndices(boolean[] selected) {
    int count = countSelected(selected);
    int[] indices = new int[count];
    int idx = 0;
    for (int i = 0; i < selected.length; i++) {
      if (selected[i]) {
        indices[idx++] = i;
      }
    }
    return indices;
  }

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
