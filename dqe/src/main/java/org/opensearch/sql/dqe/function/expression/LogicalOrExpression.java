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
 * Vectorized OR expression with SQL three-valued NULL logic: false OR null = null, true OR null =
 * true.
 */
public class LogicalOrExpression implements BlockExpression {

  private final BlockExpression left;
  private final BlockExpression right;

  public LogicalOrExpression(BlockExpression left, BlockExpression right) {
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

      if (!leftNull && leftVal) {
        // true OR anything = true
        BooleanType.BOOLEAN.writeBoolean(builder, true);
      } else if (!rightNull && rightVal) {
        // anything OR true = true
        BooleanType.BOOLEAN.writeBoolean(builder, true);
      } else if (leftNull || rightNull) {
        // at least one null and neither is true -> null
        builder.appendNull();
      } else {
        // both false
        BooleanType.BOOLEAN.writeBoolean(builder, false);
      }
    }
    return builder.build();
  }

  @Override
  public Type getType() {
    return BooleanType.BOOLEAN;
  }
}
