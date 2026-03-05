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
 * Vectorized IS NULL / IS NOT NULL expression. Unlike other expressions, this never produces NULL
 * output — it always returns true or false.
 */
public class IsNullExpression implements BlockExpression {

  private final BlockExpression child;
  private final boolean negated;

  /**
   * @param negated if true, this acts as IS NOT NULL
   */
  public IsNullExpression(BlockExpression child, boolean negated) {
    this.child = child;
    this.negated = negated;
  }

  @Override
  public Block evaluate(Page page) {
    Block childBlock = child.evaluate(page);
    int positionCount = page.getPositionCount();
    BlockBuilder builder = BooleanType.BOOLEAN.createBlockBuilder(null, positionCount);

    for (int pos = 0; pos < positionCount; pos++) {
      boolean isNull = childBlock.isNull(pos);
      BooleanType.BOOLEAN.writeBoolean(builder, negated ? !isNull : isNull);
    }
    return builder.build();
  }

  @Override
  public Type getType() {
    return BooleanType.BOOLEAN;
  }
}
