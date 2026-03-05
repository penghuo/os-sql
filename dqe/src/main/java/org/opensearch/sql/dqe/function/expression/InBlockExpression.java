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

/** Vectorized IN expression. Checks if a value is a member of a set of values. */
public class InBlockExpression implements BlockExpression {

  private final BlockExpression value;
  private final List<BlockExpression> inList;

  public InBlockExpression(BlockExpression value, List<BlockExpression> inList) {
    this.value = value;
    this.inList = inList;
  }

  @Override
  public Block evaluate(Page page) {
    Block valueBlock = value.evaluate(page);
    Block[] listBlocks = inList.stream().map(e -> e.evaluate(page)).toArray(Block[]::new);
    int positionCount = page.getPositionCount();
    Type type = value.getType();
    BlockBuilder builder = BooleanType.BOOLEAN.createBlockBuilder(null, positionCount);

    for (int pos = 0; pos < positionCount; pos++) {
      if (valueBlock.isNull(pos)) {
        builder.appendNull();
        continue;
      }
      boolean found = false;
      boolean hasNull = false;
      for (Block listBlock : listBlocks) {
        if (listBlock.isNull(pos)) {
          hasNull = true;
          continue;
        }
        if (valuesEqual(valueBlock, listBlock, pos, type)) {
          found = true;
          break;
        }
      }
      if (found) {
        BooleanType.BOOLEAN.writeBoolean(builder, true);
      } else if (hasNull) {
        builder.appendNull();
      } else {
        BooleanType.BOOLEAN.writeBoolean(builder, false);
      }
    }
    return builder.build();
  }

  @Override
  public Type getType() {
    return BooleanType.BOOLEAN;
  }

  private boolean valuesEqual(Block a, Block b, int pos, Type type) {
    if (type instanceof BigintType) {
      return BigintType.BIGINT.getLong(a, pos) == BigintType.BIGINT.getLong(b, pos);
    } else if (type instanceof DoubleType) {
      return DoubleType.DOUBLE.getDouble(a, pos) == DoubleType.DOUBLE.getDouble(b, pos);
    } else if (type instanceof VarcharType) {
      return VarcharType.VARCHAR.getSlice(a, pos).equals(VarcharType.VARCHAR.getSlice(b, pos));
    }
    return false;
  }
}
