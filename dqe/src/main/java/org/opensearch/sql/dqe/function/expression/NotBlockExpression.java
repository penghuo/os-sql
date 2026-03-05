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

/** Vectorized NOT expression. null -> null, true -> false, false -> true. */
public class NotBlockExpression implements BlockExpression {

  private final BlockExpression child;

  public NotBlockExpression(BlockExpression child) {
    this.child = child;
  }

  @Override
  public Block evaluate(Page page) {
    Block childBlock = child.evaluate(page);
    int positionCount = page.getPositionCount();
    BlockBuilder builder = BooleanType.BOOLEAN.createBlockBuilder(null, positionCount);

    for (int pos = 0; pos < positionCount; pos++) {
      if (childBlock.isNull(pos)) {
        builder.appendNull();
      } else {
        BooleanType.BOOLEAN.writeBoolean(builder, !BooleanType.BOOLEAN.getBoolean(childBlock, pos));
      }
    }
    return builder.build();
  }

  @Override
  public Type getType() {
    return BooleanType.BOOLEAN;
  }
}
