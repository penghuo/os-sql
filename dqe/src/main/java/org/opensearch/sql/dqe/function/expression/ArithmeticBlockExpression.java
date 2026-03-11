/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.function.expression;

import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.Type;
import io.trino.sql.tree.ArithmeticBinaryExpression;
import java.util.Optional;

/**
 * Vectorized arithmetic expression. Evaluates two child expressions and applies an arithmetic
 * operator position by position. Automatically promotes to DOUBLE if either operand is DOUBLE.
 *
 * <p>Uses direct {@link LongArrayBlock} construction for BIGINT results instead of {@code
 * BlockBuilder} for reduced overhead.
 */
public class ArithmeticBlockExpression implements BlockExpression {

  private final ArithmeticBinaryExpression.Operator operator;
  private final BlockExpression left;
  private final BlockExpression right;
  private final Type outputType;

  /** Pre-computed flags for fast-path selection. */
  private final boolean bothLong;

  public ArithmeticBlockExpression(
      ArithmeticBinaryExpression.Operator operator, BlockExpression left, BlockExpression right) {
    this.operator = operator;
    this.left = left;
    this.right = right;
    this.outputType =
        (left.getType() instanceof DoubleType || right.getType() instanceof DoubleType)
            ? DoubleType.DOUBLE
            : BigintType.BIGINT;
    this.bothLong =
        !(left.getType() instanceof DoubleType)
            && !(right.getType() instanceof DoubleType)
            && outputType instanceof BigintType;
  }

  @Override
  public Block evaluate(Page page) {
    Block leftBlock = left.evaluate(page);
    Block rightBlock = right.evaluate(page);
    int positionCount = page.getPositionCount();

    if (bothLong) {
      return evaluateLongDirect(leftBlock, rightBlock, positionCount);
    }

    if (outputType instanceof DoubleType) {
      return evaluateDoubleDirect(leftBlock, rightBlock, positionCount);
    }

    // Fallback (should not reach here given the constructor logic)
    return evaluateLongDirect(leftBlock, rightBlock, positionCount);
  }

  /** Direct BIGINT path using LongArrayBlock without BlockBuilder. */
  private Block evaluateLongDirect(Block leftBlock, Block rightBlock, int positionCount) {
    long[] values = new long[positionCount];
    boolean[] nulls = null;
    boolean hasNulls = false;
    boolean mayHaveNull = leftBlock.mayHaveNull() || rightBlock.mayHaveNull();

    if (mayHaveNull) {
      nulls = new boolean[positionCount];
      switch (operator) {
        case ADD:
          for (int pos = 0; pos < positionCount; pos++) {
            if (leftBlock.isNull(pos) || rightBlock.isNull(pos)) {
              nulls[pos] = true;
              hasNulls = true;
            } else {
              values[pos] =
                  left.getType().getLong(leftBlock, pos) + right.getType().getLong(rightBlock, pos);
            }
          }
          break;
        case SUBTRACT:
          for (int pos = 0; pos < positionCount; pos++) {
            if (leftBlock.isNull(pos) || rightBlock.isNull(pos)) {
              nulls[pos] = true;
              hasNulls = true;
            } else {
              values[pos] =
                  left.getType().getLong(leftBlock, pos) - right.getType().getLong(rightBlock, pos);
            }
          }
          break;
        case MULTIPLY:
          for (int pos = 0; pos < positionCount; pos++) {
            if (leftBlock.isNull(pos) || rightBlock.isNull(pos)) {
              nulls[pos] = true;
              hasNulls = true;
            } else {
              values[pos] =
                  left.getType().getLong(leftBlock, pos) * right.getType().getLong(rightBlock, pos);
            }
          }
          break;
        case DIVIDE:
          for (int pos = 0; pos < positionCount; pos++) {
            if (leftBlock.isNull(pos) || rightBlock.isNull(pos)) {
              nulls[pos] = true;
              hasNulls = true;
            } else {
              values[pos] =
                  left.getType().getLong(leftBlock, pos) / right.getType().getLong(rightBlock, pos);
            }
          }
          break;
        case MODULUS:
          for (int pos = 0; pos < positionCount; pos++) {
            if (leftBlock.isNull(pos) || rightBlock.isNull(pos)) {
              nulls[pos] = true;
              hasNulls = true;
            } else {
              values[pos] =
                  left.getType().getLong(leftBlock, pos) % right.getType().getLong(rightBlock, pos);
            }
          }
          break;
        default:
          throw new UnsupportedOperationException("Unsupported operator: " + operator);
      }
      return new LongArrayBlock(
          positionCount, hasNulls ? Optional.of(nulls) : Optional.empty(), values);
    }

    // No nulls: tight loop
    switch (operator) {
      case ADD:
        for (int pos = 0; pos < positionCount; pos++) {
          values[pos] =
              left.getType().getLong(leftBlock, pos) + right.getType().getLong(rightBlock, pos);
        }
        break;
      case SUBTRACT:
        for (int pos = 0; pos < positionCount; pos++) {
          values[pos] =
              left.getType().getLong(leftBlock, pos) - right.getType().getLong(rightBlock, pos);
        }
        break;
      case MULTIPLY:
        for (int pos = 0; pos < positionCount; pos++) {
          values[pos] =
              left.getType().getLong(leftBlock, pos) * right.getType().getLong(rightBlock, pos);
        }
        break;
      case DIVIDE:
        for (int pos = 0; pos < positionCount; pos++) {
          values[pos] =
              left.getType().getLong(leftBlock, pos) / right.getType().getLong(rightBlock, pos);
        }
        break;
      case MODULUS:
        for (int pos = 0; pos < positionCount; pos++) {
          values[pos] =
              left.getType().getLong(leftBlock, pos) % right.getType().getLong(rightBlock, pos);
        }
        break;
      default:
        throw new UnsupportedOperationException("Unsupported operator: " + operator);
    }
    return new LongArrayBlock(positionCount, Optional.empty(), values);
  }

  /** Direct DOUBLE path - still uses BlockBuilder since there's no public DoubleArrayBlock. */
  private Block evaluateDoubleDirect(Block leftBlock, Block rightBlock, int positionCount) {
    // Use LongArrayBlock with Double.doubleToRawLongBits for double values
    // This avoids the BlockBuilder overhead while maintaining correctness
    // because DoubleType stores doubles as raw long bits in LongArrayBlock
    long[] values = new long[positionCount];
    boolean[] nulls = null;
    boolean hasNulls = false;
    boolean mayHaveNull = leftBlock.mayHaveNull() || rightBlock.mayHaveNull();
    Type leftType = left.getType();
    Type rightType = right.getType();

    if (mayHaveNull) {
      nulls = new boolean[positionCount];
      for (int pos = 0; pos < positionCount; pos++) {
        if (leftBlock.isNull(pos) || rightBlock.isNull(pos)) {
          nulls[pos] = true;
          hasNulls = true;
        } else {
          double l = readAsDouble(leftBlock, pos, leftType);
          double r = readAsDouble(rightBlock, pos, rightType);
          values[pos] = Double.doubleToLongBits(applyDouble(l, r));
        }
      }
      return new LongArrayBlock(
          positionCount, hasNulls ? Optional.of(nulls) : Optional.empty(), values);
    }

    for (int pos = 0; pos < positionCount; pos++) {
      double l = readAsDouble(leftBlock, pos, leftType);
      double r = readAsDouble(rightBlock, pos, rightType);
      values[pos] = Double.doubleToLongBits(applyDouble(l, r));
    }
    return new LongArrayBlock(positionCount, Optional.empty(), values);
  }

  @Override
  public Type getType() {
    return outputType;
  }

  private double readAsDouble(Block block, int pos, Type type) {
    if (type instanceof DoubleType) {
      return DoubleType.DOUBLE.getDouble(block, pos);
    }
    return type.getLong(block, pos);
  }

  private double applyDouble(double l, double r) {
    switch (operator) {
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
        throw new UnsupportedOperationException("Unsupported operator: " + operator);
    }
  }
}
