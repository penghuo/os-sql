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
 * Vectorized AND expression with SQL three-valued NULL logic: true AND null = null, false AND null
 * = false.
 *
 * <p>Uses direct {@link ByteArrayBlock} construction instead of {@code BlockBuilder} for reduced
 * overhead.
 */
@lombok.Getter
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
    byte[] values = new byte[positionCount];
    boolean[] nulls = null;
    boolean hasNullOutput = false;

    boolean leftMayHaveNull = leftBlock.mayHaveNull();
    boolean rightMayHaveNull = rightBlock.mayHaveNull();

    if (!leftMayHaveNull && !rightMayHaveNull) {
      // Fast path: no nulls possible, pure boolean AND
      for (int pos = 0; pos < positionCount; pos++) {
        boolean leftVal = BooleanType.BOOLEAN.getBoolean(leftBlock, pos);
        boolean rightVal = BooleanType.BOOLEAN.getBoolean(rightBlock, pos);
        values[pos] = (byte) (leftVal && rightVal ? 1 : 0);
      }
      return new ByteArrayBlock(positionCount, Optional.empty(), values);
    }

    // General path with null handling
    nulls = new boolean[positionCount];
    for (int pos = 0; pos < positionCount; pos++) {
      boolean leftNull = leftMayHaveNull && leftBlock.isNull(pos);
      boolean rightNull = rightMayHaveNull && rightBlock.isNull(pos);
      boolean leftVal = !leftNull && BooleanType.BOOLEAN.getBoolean(leftBlock, pos);
      boolean rightVal = !rightNull && BooleanType.BOOLEAN.getBoolean(rightBlock, pos);

      if (!leftNull && !leftVal) {
        // false AND anything = false
        values[pos] = 0;
      } else if (!rightNull && !rightVal) {
        // anything AND false = false
        values[pos] = 0;
      } else if (leftNull || rightNull) {
        // at least one null and neither is false -> null
        nulls[pos] = true;
        hasNullOutput = true;
      } else {
        // both true
        values[pos] = 1;
      }
    }
    return new ByteArrayBlock(
        positionCount, hasNullOutput ? Optional.of(nulls) : Optional.empty(), values);
  }

  @Override
  public Type getType() {
    return BooleanType.BOOLEAN;
  }
}
