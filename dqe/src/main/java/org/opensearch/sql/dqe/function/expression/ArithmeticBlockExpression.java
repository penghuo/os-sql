/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.function.expression;

import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.Type;
import io.trino.sql.tree.ArithmeticBinaryExpression;

/**
 * Vectorized arithmetic expression. Evaluates two child expressions and applies an arithmetic
 * operator position by position. Automatically promotes to DOUBLE if either operand is DOUBLE.
 */
public class ArithmeticBlockExpression implements BlockExpression {

  private final ArithmeticBinaryExpression.Operator operator;
  private final BlockExpression left;
  private final BlockExpression right;
  private final Type outputType;

  public ArithmeticBlockExpression(
      ArithmeticBinaryExpression.Operator operator, BlockExpression left, BlockExpression right) {
    this.operator = operator;
    this.left = left;
    this.right = right;
    this.outputType =
        (left.getType() instanceof DoubleType || right.getType() instanceof DoubleType)
            ? DoubleType.DOUBLE
            : BigintType.BIGINT;
  }

  @Override
  public Block evaluate(Page page) {
    Block leftBlock = left.evaluate(page);
    Block rightBlock = right.evaluate(page);
    int positionCount = page.getPositionCount();
    BlockBuilder builder = outputType.createBlockBuilder(null, positionCount);

    if (outputType instanceof DoubleType) {
      evaluateDouble(leftBlock, rightBlock, positionCount, builder);
    } else {
      evaluateLong(leftBlock, rightBlock, positionCount, builder);
    }
    return builder.build();
  }

  @Override
  public Type getType() {
    return outputType;
  }

  private void evaluateDouble(
      Block leftBlock, Block rightBlock, int positionCount, BlockBuilder builder) {
    for (int pos = 0; pos < positionCount; pos++) {
      if (leftBlock.isNull(pos) || rightBlock.isNull(pos)) {
        builder.appendNull();
        continue;
      }
      double l = readAsDouble(leftBlock, pos, left.getType());
      double r = readAsDouble(rightBlock, pos, right.getType());
      DoubleType.DOUBLE.writeDouble(builder, applyDouble(l, r));
    }
  }

  private void evaluateLong(
      Block leftBlock, Block rightBlock, int positionCount, BlockBuilder builder) {
    for (int pos = 0; pos < positionCount; pos++) {
      if (leftBlock.isNull(pos) || rightBlock.isNull(pos)) {
        builder.appendNull();
        continue;
      }
      long l = left.getType().getLong(leftBlock, pos);
      long r = right.getType().getLong(rightBlock, pos);
      BigintType.BIGINT.writeLong(builder, applyLong(l, r));
    }
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

  private long applyLong(long l, long r) {
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
