/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.function.expression;

import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import java.util.List;

/** Vectorized COALESCE expression. Returns the first non-null argument for each position. */
public class CoalesceBlockExpression implements BlockExpression {

  private final List<BlockExpression> operands;
  private final Type outputType;

  public CoalesceBlockExpression(List<BlockExpression> operands, Type outputType) {
    this.operands = operands;
    this.outputType = outputType;
  }

  @Override
  public Block evaluate(Page page) {
    int positionCount = page.getPositionCount();
    Block[] blocks = operands.stream().map(o -> o.evaluate(page)).toArray(Block[]::new);
    BlockBuilder builder = outputType.createBlockBuilder(null, positionCount);

    for (int pos = 0; pos < positionCount; pos++) {
      boolean found = false;
      for (Block block : blocks) {
        if (!block.isNull(pos)) {
          copyValue(block, pos, builder);
          found = true;
          break;
        }
      }
      if (!found) {
        builder.appendNull();
      }
    }
    return builder.build();
  }

  @Override
  public Type getType() {
    return outputType;
  }

  private void copyValue(Block source, int pos, BlockBuilder builder) {
    if (outputType instanceof VarcharType) {
      VarcharType.VARCHAR.writeSlice(builder, VarcharType.VARCHAR.getSlice(source, pos));
    } else if (outputType instanceof BigintType) {
      BigintType.BIGINT.writeLong(builder, BigintType.BIGINT.getLong(source, pos));
    } else if (outputType instanceof DoubleType) {
      DoubleType.DOUBLE.writeDouble(builder, DoubleType.DOUBLE.getDouble(source, pos));
    } else if (outputType instanceof BooleanType) {
      BooleanType.BOOLEAN.writeBoolean(builder, BooleanType.BOOLEAN.getBoolean(source, pos));
    } else {
      // For IntegerType, SmallintType, TinyintType, etc. — use getLong/writeLong
      outputType.writeLong(builder, outputType.getLong(source, pos));
    }
  }
}
