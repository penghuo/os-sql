/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.function.expression;

import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.ByteArrayBlock;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.Type;
import java.util.Optional;

/**
 * Vectorized NOT expression. null -> null, true -> false, false -> true.
 *
 * <p>Uses direct {@link ByteArrayBlock} construction instead of {@code BlockBuilder} for reduced
 * overhead.
 */
public class NotBlockExpression implements BlockExpression {

  private final BlockExpression child;

  public NotBlockExpression(BlockExpression child) {
    this.child = child;
  }

  @Override
  public Block evaluate(Page page) {
    Block childBlock = child.evaluate(page);
    int positionCount = page.getPositionCount();
    byte[] values = new byte[positionCount];

    if (!childBlock.mayHaveNull()) {
      // Fast path: no nulls
      for (int pos = 0; pos < positionCount; pos++) {
        values[pos] = (byte) (BooleanType.BOOLEAN.getBoolean(childBlock, pos) ? 0 : 1);
      }
      return new ByteArrayBlock(positionCount, Optional.empty(), values);
    }

    // General path with null handling
    boolean[] nulls = new boolean[positionCount];
    boolean hasNulls = false;
    for (int pos = 0; pos < positionCount; pos++) {
      if (childBlock.isNull(pos)) {
        nulls[pos] = true;
        hasNulls = true;
      } else {
        values[pos] = (byte) (BooleanType.BOOLEAN.getBoolean(childBlock, pos) ? 0 : 1);
      }
    }
    return new ByteArrayBlock(
        positionCount, hasNulls ? Optional.of(nulls) : Optional.empty(), values);
  }

  @Override
  public Type getType() {
    return BooleanType.BOOLEAN;
  }
}
