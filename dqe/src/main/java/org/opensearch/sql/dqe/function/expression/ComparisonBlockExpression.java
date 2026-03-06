/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.function.expression;

import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import io.trino.sql.tree.ComparisonExpression;

/**
 * Vectorized comparison expression. Evaluates two child expressions and compares them position by
 * position, producing a BOOLEAN output Block. Supports NULL propagation.
 */
public class ComparisonBlockExpression implements BlockExpression {

  private final ComparisonExpression.Operator operator;
  private final BlockExpression left;
  private final BlockExpression right;

  public ComparisonBlockExpression(
      ComparisonExpression.Operator operator, BlockExpression left, BlockExpression right) {
    this.operator = operator;
    this.left = left;
    this.right = right;
  }

  @Override
  public Block evaluate(Page page) {
    Block leftBlock = left.evaluate(page);
    Block rightBlock = right.evaluate(page);
    int positionCount = page.getPositionCount();
    BlockBuilder builder = BooleanType.BOOLEAN.createBlockBuilder(null, positionCount);

    for (int pos = 0; pos < positionCount; pos++) {
      if (leftBlock.isNull(pos) || rightBlock.isNull(pos)) {
        builder.appendNull();
      } else {
        int cmp = compareAt(leftBlock, rightBlock, pos);
        boolean result = applyOperator(cmp);
        BooleanType.BOOLEAN.writeBoolean(builder, result);
      }
    }
    return builder.build();
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

    // Both numeric — promote to double if either is DOUBLE
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
}
