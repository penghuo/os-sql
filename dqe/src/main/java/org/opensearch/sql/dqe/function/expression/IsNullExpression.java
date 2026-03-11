/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.function.expression;

import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.ByteArrayBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.Type;
import java.util.Optional;

/**
 * Vectorized IS NULL / IS NOT NULL expression. Unlike other expressions, this never produces NULL
 * output -- it always returns true or false.
 *
 * <p>Uses direct {@link ByteArrayBlock} construction and includes a fast path when the input block
 * has no nulls.
 */
public class IsNullExpression implements BlockExpression {

  private final BlockExpression child;
  private final boolean negated;

  /** Single-value block for constant true/false results. */
  private static final Block TRUE_BLOCK;

  private static final Block FALSE_BLOCK;

  static {
    byte[] trueVal = {1};
    byte[] falseVal = {0};
    TRUE_BLOCK = new ByteArrayBlock(1, Optional.empty(), trueVal);
    FALSE_BLOCK = new ByteArrayBlock(1, Optional.empty(), falseVal);
  }

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

    // Fast path: if child has no nulls, IS NULL is all false, IS NOT NULL is all true
    if (!childBlock.mayHaveNull()) {
      Block constantBlock = negated ? TRUE_BLOCK : FALSE_BLOCK;
      return RunLengthEncodedBlock.create(constantBlock, positionCount);
    }

    // General path: check each position
    byte[] values = new byte[positionCount];
    for (int pos = 0; pos < positionCount; pos++) {
      boolean isNull = childBlock.isNull(pos);
      values[pos] = (byte) ((negated ? !isNull : isNull) ? 1 : 0);
    }
    return new ByteArrayBlock(positionCount, Optional.empty(), values);
  }

  @Override
  public Type getType() {
    return BooleanType.BOOLEAN;
  }
}
