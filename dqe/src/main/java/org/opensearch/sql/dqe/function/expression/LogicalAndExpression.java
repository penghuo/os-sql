/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.function.expression;

import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.Type;

/**
 * Vectorized AND expression with SQL three-valued NULL logic: true AND null = null, false AND null
 * = false.
 */
public class LogicalAndExpression implements BlockExpression {

  private final BlockExpression left;
  private final BlockExpression right;

  public LogicalAndExpression(BlockExpression left, BlockExpression right) {
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
      boolean leftNull = leftBlock.isNull(pos);
      boolean rightNull = rightBlock.isNull(pos);
      boolean leftVal = !leftNull && BooleanType.BOOLEAN.getBoolean(leftBlock, pos);
      boolean rightVal = !rightNull && BooleanType.BOOLEAN.getBoolean(rightBlock, pos);

      if (!leftNull && !leftVal) {
        // false AND anything = false
        BooleanType.BOOLEAN.writeBoolean(builder, false);
      } else if (!rightNull && !rightVal) {
        // anything AND false = false
        BooleanType.BOOLEAN.writeBoolean(builder, false);
      } else if (leftNull || rightNull) {
        // at least one null and neither is false -> null
        builder.appendNull();
      } else {
        // both true
        BooleanType.BOOLEAN.writeBoolean(builder, true);
      }
    }
    return builder.build();
  }

  @Override
  public Type getType() {
    return BooleanType.BOOLEAN;
  }
}
