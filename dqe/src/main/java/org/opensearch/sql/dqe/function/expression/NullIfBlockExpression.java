/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.function.expression;

import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

/** Vectorized NULLIF(a, b) expression. Returns null if a equals b, otherwise returns a. */
public class NullIfBlockExpression implements BlockExpression {

  private final BlockExpression first;
  private final BlockExpression second;

  public NullIfBlockExpression(BlockExpression first, BlockExpression second) {
    this.first = first;
    this.second = second;
  }

  @Override
  public Block evaluate(Page page) {
    Block firstBlock = first.evaluate(page);
    Block secondBlock = second.evaluate(page);
    int positionCount = page.getPositionCount();
    Type type = first.getType();
    BlockBuilder builder = type.createBlockBuilder(null, positionCount);

    for (int pos = 0; pos < positionCount; pos++) {
      if (firstBlock.isNull(pos)) {
        builder.appendNull();
      } else if (!secondBlock.isNull(pos) && valuesEqual(firstBlock, secondBlock, pos)) {
        builder.appendNull();
      } else {
        copyValue(firstBlock, pos, type, builder);
      }
    }
    return builder.build();
  }

  @Override
  public Type getType() {
    return first.getType();
  }

  private boolean valuesEqual(Block a, Block b, int pos) {
    Type firstType = first.getType();
    Type secondType = second.getType();

    if (firstType instanceof VarcharType && secondType instanceof VarcharType) {
      return VarcharType.VARCHAR.getSlice(a, pos).equals(VarcharType.VARCHAR.getSlice(b, pos));
    }

    // Both numeric — promote to double if either side is DOUBLE
    if (firstType instanceof DoubleType || secondType instanceof DoubleType) {
      double lv = readNumeric(a, pos, firstType);
      double rv = readNumeric(b, pos, secondType);
      return Double.compare(lv, rv) == 0;
    }

    // Integer family (BigintType, IntegerType, SmallintType, TinyintType) — compare as long
    return firstType.getLong(a, pos) == secondType.getLong(b, pos);
  }

  private double readNumeric(Block block, int pos, Type type) {
    if (type instanceof DoubleType) {
      return DoubleType.DOUBLE.getDouble(block, pos);
    }
    return type.getLong(block, pos);
  }

  private void copyValue(Block source, int pos, Type type, BlockBuilder builder) {
    if (type instanceof VarcharType) {
      VarcharType.VARCHAR.writeSlice(builder, VarcharType.VARCHAR.getSlice(source, pos));
    } else if (type instanceof DoubleType) {
      DoubleType.DOUBLE.writeDouble(builder, DoubleType.DOUBLE.getDouble(source, pos));
    } else {
      // Integer family (BigintType, IntegerType, SmallintType, TinyintType)
      type.writeLong(builder, type.getLong(source, pos));
    }
  }
}
