/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.function.expression;

import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.ByteArrayBlock;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import io.trino.sql.tree.ComparisonExpression;
import java.util.Optional;

/**
 * Vectorized comparison expression. Evaluates two child expressions and compares them position by
 * position, producing a BOOLEAN output Block. Supports NULL propagation.
 *
 * <p>Uses direct {@link ByteArrayBlock} construction instead of {@code BlockBuilder} for reduced
 * overhead. Includes specialized fast paths for BIGINT-only comparisons (the most common case in
 * ClickBench filter predicates).
 */
public class ComparisonBlockExpression implements BlockExpression {

  private final ComparisonExpression.Operator operator;
  private final BlockExpression left;
  private final BlockExpression right;

  /** Pre-computed flags for fast-path selection (avoid repeated instanceof checks). */
  private final boolean bothLong;

  private final boolean bothDouble;
  private final boolean eitherDouble;

  public ComparisonBlockExpression(
      ComparisonExpression.Operator operator, BlockExpression left, BlockExpression right) {
    this.operator = operator;
    this.left = left;
    this.right = right;

    Type leftType = left.getType();
    Type rightType = right.getType();
    boolean leftIsLong = isLongType(leftType);
    boolean rightIsLong = isLongType(rightType);
    boolean leftIsDouble = leftType instanceof DoubleType;
    boolean rightIsDouble = rightType instanceof DoubleType;

    this.bothLong = leftIsLong && rightIsLong;
    this.bothDouble = leftIsDouble && rightIsDouble;
    this.eitherDouble = leftIsDouble || rightIsDouble;
  }

  @Override
  public Block evaluate(Page page) {
    Block leftBlock = left.evaluate(page);
    Block rightBlock = right.evaluate(page);
    int positionCount = page.getPositionCount();

    // Fast path: both sides are long-backed types (BIGINT, INTEGER, TIMESTAMP, etc.)
    if (bothLong) {
      return evaluateLongLong(leftBlock, rightBlock, positionCount);
    }

    // Fast path: both sides are DOUBLE
    if (bothDouble) {
      return evaluateDoubleDouble(leftBlock, rightBlock, positionCount);
    }

    // Fast path: numeric with at least one DOUBLE (promote to double)
    if (eitherDouble) {
      return evaluateNumericDouble(leftBlock, rightBlock, positionCount);
    }

    // General path for VARCHAR, BOOLEAN, and mixed types
    return evaluateGeneral(leftBlock, rightBlock, positionCount);
  }

  /** Fast path for BIGINT vs BIGINT (or any long-backed type pair). No type dispatch per row. */
  private Block evaluateLongLong(Block leftBlock, Block rightBlock, int positionCount) {
    byte[] values = new byte[positionCount];
    boolean[] nulls = null;
    boolean hasNulls = leftBlock.mayHaveNull() || rightBlock.mayHaveNull();

    if (hasNulls) {
      nulls = new boolean[positionCount];
      switch (operator) {
        case EQUAL:
          for (int pos = 0; pos < positionCount; pos++) {
            if (leftBlock.isNull(pos) || rightBlock.isNull(pos)) {
              nulls[pos] = true;
            } else {
              long l = left.getType().getLong(leftBlock, pos);
              long r = right.getType().getLong(rightBlock, pos);
              values[pos] = (byte) (l == r ? 1 : 0);
            }
          }
          break;
        case NOT_EQUAL:
          for (int pos = 0; pos < positionCount; pos++) {
            if (leftBlock.isNull(pos) || rightBlock.isNull(pos)) {
              nulls[pos] = true;
            } else {
              long l = left.getType().getLong(leftBlock, pos);
              long r = right.getType().getLong(rightBlock, pos);
              values[pos] = (byte) (l != r ? 1 : 0);
            }
          }
          break;
        case LESS_THAN:
          for (int pos = 0; pos < positionCount; pos++) {
            if (leftBlock.isNull(pos) || rightBlock.isNull(pos)) {
              nulls[pos] = true;
            } else {
              long l = left.getType().getLong(leftBlock, pos);
              long r = right.getType().getLong(rightBlock, pos);
              values[pos] = (byte) (l < r ? 1 : 0);
            }
          }
          break;
        case LESS_THAN_OR_EQUAL:
          for (int pos = 0; pos < positionCount; pos++) {
            if (leftBlock.isNull(pos) || rightBlock.isNull(pos)) {
              nulls[pos] = true;
            } else {
              long l = left.getType().getLong(leftBlock, pos);
              long r = right.getType().getLong(rightBlock, pos);
              values[pos] = (byte) (l <= r ? 1 : 0);
            }
          }
          break;
        case GREATER_THAN:
          for (int pos = 0; pos < positionCount; pos++) {
            if (leftBlock.isNull(pos) || rightBlock.isNull(pos)) {
              nulls[pos] = true;
            } else {
              long l = left.getType().getLong(leftBlock, pos);
              long r = right.getType().getLong(rightBlock, pos);
              values[pos] = (byte) (l > r ? 1 : 0);
            }
          }
          break;
        case GREATER_THAN_OR_EQUAL:
          for (int pos = 0; pos < positionCount; pos++) {
            if (leftBlock.isNull(pos) || rightBlock.isNull(pos)) {
              nulls[pos] = true;
            } else {
              long l = left.getType().getLong(leftBlock, pos);
              long r = right.getType().getLong(rightBlock, pos);
              values[pos] = (byte) (l >= r ? 1 : 0);
            }
          }
          break;
        default:
          throw new UnsupportedOperationException("Unsupported comparison operator: " + operator);
      }
      return new ByteArrayBlock(positionCount, Optional.of(nulls), values);
    }

    // No nulls possible: tight loop without null checks
    switch (operator) {
      case EQUAL:
        for (int pos = 0; pos < positionCount; pos++) {
          long l = left.getType().getLong(leftBlock, pos);
          long r = right.getType().getLong(rightBlock, pos);
          values[pos] = (byte) (l == r ? 1 : 0);
        }
        break;
      case NOT_EQUAL:
        for (int pos = 0; pos < positionCount; pos++) {
          long l = left.getType().getLong(leftBlock, pos);
          long r = right.getType().getLong(rightBlock, pos);
          values[pos] = (byte) (l != r ? 1 : 0);
        }
        break;
      case LESS_THAN:
        for (int pos = 0; pos < positionCount; pos++) {
          long l = left.getType().getLong(leftBlock, pos);
          long r = right.getType().getLong(rightBlock, pos);
          values[pos] = (byte) (l < r ? 1 : 0);
        }
        break;
      case LESS_THAN_OR_EQUAL:
        for (int pos = 0; pos < positionCount; pos++) {
          long l = left.getType().getLong(leftBlock, pos);
          long r = right.getType().getLong(rightBlock, pos);
          values[pos] = (byte) (l <= r ? 1 : 0);
        }
        break;
      case GREATER_THAN:
        for (int pos = 0; pos < positionCount; pos++) {
          long l = left.getType().getLong(leftBlock, pos);
          long r = right.getType().getLong(rightBlock, pos);
          values[pos] = (byte) (l > r ? 1 : 0);
        }
        break;
      case GREATER_THAN_OR_EQUAL:
        for (int pos = 0; pos < positionCount; pos++) {
          long l = left.getType().getLong(leftBlock, pos);
          long r = right.getType().getLong(rightBlock, pos);
          values[pos] = (byte) (l >= r ? 1 : 0);
        }
        break;
      default:
        throw new UnsupportedOperationException("Unsupported comparison operator: " + operator);
    }
    return new ByteArrayBlock(positionCount, Optional.empty(), values);
  }

  /** Fast path for DOUBLE vs DOUBLE. */
  private Block evaluateDoubleDouble(Block leftBlock, Block rightBlock, int positionCount) {
    byte[] values = new byte[positionCount];
    boolean[] nulls = null;
    boolean hasNulls = leftBlock.mayHaveNull() || rightBlock.mayHaveNull();

    if (hasNulls) {
      nulls = new boolean[positionCount];
      for (int pos = 0; pos < positionCount; pos++) {
        if (leftBlock.isNull(pos) || rightBlock.isNull(pos)) {
          nulls[pos] = true;
        } else {
          double l = DoubleType.DOUBLE.getDouble(leftBlock, pos);
          double r = DoubleType.DOUBLE.getDouble(rightBlock, pos);
          values[pos] = (byte) (applyOperator(Double.compare(l, r)) ? 1 : 0);
        }
      }
      return new ByteArrayBlock(positionCount, Optional.of(nulls), values);
    }

    for (int pos = 0; pos < positionCount; pos++) {
      double l = DoubleType.DOUBLE.getDouble(leftBlock, pos);
      double r = DoubleType.DOUBLE.getDouble(rightBlock, pos);
      values[pos] = (byte) (applyOperator(Double.compare(l, r)) ? 1 : 0);
    }
    return new ByteArrayBlock(positionCount, Optional.empty(), values);
  }

  /** Numeric path with DOUBLE promotion (one side is DOUBLE). */
  private Block evaluateNumericDouble(Block leftBlock, Block rightBlock, int positionCount) {
    byte[] values = new byte[positionCount];
    boolean[] nulls = null;
    boolean hasNulls = leftBlock.mayHaveNull() || rightBlock.mayHaveNull();
    Type leftType = left.getType();
    Type rightType = right.getType();

    if (hasNulls) {
      nulls = new boolean[positionCount];
      for (int pos = 0; pos < positionCount; pos++) {
        if (leftBlock.isNull(pos) || rightBlock.isNull(pos)) {
          nulls[pos] = true;
        } else {
          double l = readNumeric(leftBlock, pos, leftType);
          double r = readNumeric(rightBlock, pos, rightType);
          values[pos] = (byte) (applyOperator(Double.compare(l, r)) ? 1 : 0);
        }
      }
      return new ByteArrayBlock(positionCount, Optional.of(nulls), values);
    }

    for (int pos = 0; pos < positionCount; pos++) {
      double l = readNumeric(leftBlock, pos, leftType);
      double r = readNumeric(rightBlock, pos, rightType);
      values[pos] = (byte) (applyOperator(Double.compare(l, r)) ? 1 : 0);
    }
    return new ByteArrayBlock(positionCount, Optional.empty(), values);
  }

  /** General path for VARCHAR, BOOLEAN, and mixed types. */
  private Block evaluateGeneral(Block leftBlock, Block rightBlock, int positionCount) {
    byte[] values = new byte[positionCount];
    boolean[] nulls = null;
    boolean hasNulls = leftBlock.mayHaveNull() || rightBlock.mayHaveNull();

    if (hasNulls) {
      nulls = new boolean[positionCount];
      for (int pos = 0; pos < positionCount; pos++) {
        if (leftBlock.isNull(pos) || rightBlock.isNull(pos)) {
          nulls[pos] = true;
        } else {
          int cmp = compareAt(leftBlock, rightBlock, pos);
          values[pos] = (byte) (applyOperator(cmp) ? 1 : 0);
        }
      }
      return new ByteArrayBlock(positionCount, Optional.of(nulls), values);
    }

    for (int pos = 0; pos < positionCount; pos++) {
      int cmp = compareAt(leftBlock, rightBlock, pos);
      values[pos] = (byte) (applyOperator(cmp) ? 1 : 0);
    }
    return new ByteArrayBlock(positionCount, Optional.empty(), values);
  }

  @Override
  public Type getType() {
    return BooleanType.BOOLEAN;
  }

  private int compareAt(Block leftBlock, Block rightBlock, int pos) {
    Type leftType = left.getType();
    Type rightType = right.getType();

    // Both VARCHAR
    if (leftType instanceof VarcharType && rightType instanceof VarcharType) {
      String l = VarcharType.VARCHAR.getSlice(leftBlock, pos).toStringUtf8();
      String r = VarcharType.VARCHAR.getSlice(rightBlock, pos).toStringUtf8();
      return l.compareTo(r);
    }

    // Both BOOLEAN
    if (leftType instanceof BooleanType && rightType instanceof BooleanType) {
      boolean l = BooleanType.BOOLEAN.getBoolean(leftBlock, pos);
      boolean r = BooleanType.BOOLEAN.getBoolean(rightBlock, pos);
      return Boolean.compare(l, r);
    }

    // Boolean vs literal (e.g., is_active = true)
    if (leftType instanceof BooleanType || rightType instanceof BooleanType) {
      boolean l = readBoolean(leftBlock, pos, leftType);
      boolean r = readBoolean(rightBlock, pos, rightType);
      return Boolean.compare(l, r);
    }

    // Both numeric -- promote to double if either is DOUBLE
    double l = readNumeric(leftBlock, pos, leftType);
    double r = readNumeric(rightBlock, pos, rightType);
    return Double.compare(l, r);
  }

  private double readNumeric(Block block, int pos, Type type) {
    if (type instanceof DoubleType) {
      return DoubleType.DOUBLE.getDouble(block, pos);
    }
    return type.getLong(block, pos);
  }

  private boolean readBoolean(Block block, int pos, Type type) {
    if (type instanceof BooleanType) {
      return BooleanType.BOOLEAN.getBoolean(block, pos);
    }
    // For numeric types, treat non-zero as true (for "is_active = true" where true is a literal)
    return type.getLong(block, pos) != 0;
  }

  private boolean applyOperator(int cmp) {
    switch (operator) {
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
        throw new UnsupportedOperationException("Unsupported comparison operator: " + operator);
    }
  }

  /** Check if a type stores values as long (BIGINT, INTEGER, TIMESTAMP, etc.). */
  private static boolean isLongType(Type type) {
    return type instanceof BigintType
        || type instanceof io.trino.spi.type.IntegerType
        || type instanceof io.trino.spi.type.SmallintType
        || type instanceof io.trino.spi.type.TinyintType
        || type instanceof io.trino.spi.type.TimestampType
        || type instanceof io.trino.spi.type.DateType;
  }
}
