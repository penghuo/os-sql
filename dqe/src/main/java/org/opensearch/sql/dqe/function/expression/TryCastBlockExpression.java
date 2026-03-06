/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.function.expression;

import io.airlift.slice.Slices;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

/**
 * Vectorized TRY_CAST expression. Converts values from one type to another, returning null instead
 * of throwing an error when the conversion fails.
 */
public class TryCastBlockExpression implements BlockExpression {

  private final BlockExpression child;
  private final Type targetType;

  public TryCastBlockExpression(BlockExpression child, Type targetType) {
    this.child = child;
    this.targetType = targetType;
  }

  @Override
  public Block evaluate(Page page) {
    Block input = child.evaluate(page);
    int positionCount = page.getPositionCount();
    BlockBuilder builder = targetType.createBlockBuilder(null, positionCount);
    Type sourceType = child.getType();

    for (int pos = 0; pos < positionCount; pos++) {
      if (input.isNull(pos)) {
        builder.appendNull();
        continue;
      }
      try {
        castValue(input, pos, sourceType, builder);
      } catch (Exception e) {
        // TRY_CAST returns null on conversion failure
        builder.appendNull();
      }
    }
    return builder.build();
  }

  @Override
  public Type getType() {
    return targetType;
  }

  private void castValue(Block input, int pos, Type sourceType, BlockBuilder builder) {
    if (targetType instanceof DoubleType) {
      castToDouble(input, pos, sourceType, builder);
    } else if (targetType instanceof BigintType) {
      castToBigint(input, pos, sourceType, builder);
    } else if (targetType instanceof VarcharType) {
      castToVarchar(input, pos, sourceType, builder);
    } else if (targetType instanceof BooleanType) {
      castToBoolean(input, pos, sourceType, builder);
    } else {
      throw new UnsupportedOperationException(
          "Unsupported TRY_CAST from " + sourceType + " to " + targetType);
    }
  }

  private void castToDouble(Block input, int pos, Type sourceType, BlockBuilder builder) {
    if (sourceType instanceof BigintType || sourceType instanceof io.trino.spi.type.IntegerType) {
      DoubleType.DOUBLE.writeDouble(builder, sourceType.getLong(input, pos));
    } else if (sourceType instanceof VarcharType) {
      String val = VarcharType.VARCHAR.getSlice(input, pos).toStringUtf8();
      DoubleType.DOUBLE.writeDouble(builder, Double.parseDouble(val));
    } else if (sourceType instanceof DoubleType) {
      DoubleType.DOUBLE.writeDouble(builder, DoubleType.DOUBLE.getDouble(input, pos));
    } else {
      throw new UnsupportedOperationException("Cannot TRY_CAST " + sourceType + " to DOUBLE");
    }
  }

  private void castToBigint(Block input, int pos, Type sourceType, BlockBuilder builder) {
    if (sourceType instanceof DoubleType) {
      BigintType.BIGINT.writeLong(builder, (long) DoubleType.DOUBLE.getDouble(input, pos));
    } else if (sourceType instanceof VarcharType) {
      String val = VarcharType.VARCHAR.getSlice(input, pos).toStringUtf8();
      BigintType.BIGINT.writeLong(builder, Long.parseLong(val));
    } else if (sourceType instanceof BigintType
        || sourceType instanceof io.trino.spi.type.IntegerType) {
      BigintType.BIGINT.writeLong(builder, sourceType.getLong(input, pos));
    } else {
      throw new UnsupportedOperationException("Cannot TRY_CAST " + sourceType + " to BIGINT");
    }
  }

  private void castToVarchar(Block input, int pos, Type sourceType, BlockBuilder builder) {
    String val;
    if (sourceType instanceof BigintType || sourceType instanceof io.trino.spi.type.IntegerType) {
      val = String.valueOf(sourceType.getLong(input, pos));
    } else if (sourceType instanceof DoubleType) {
      val = String.valueOf(DoubleType.DOUBLE.getDouble(input, pos));
    } else if (sourceType instanceof BooleanType) {
      val = String.valueOf(BooleanType.BOOLEAN.getBoolean(input, pos));
    } else if (sourceType instanceof VarcharType) {
      val = VarcharType.VARCHAR.getSlice(input, pos).toStringUtf8();
    } else {
      throw new UnsupportedOperationException("Cannot TRY_CAST " + sourceType + " to VARCHAR");
    }
    VarcharType.VARCHAR.writeSlice(builder, Slices.utf8Slice(val));
  }

  private void castToBoolean(Block input, int pos, Type sourceType, BlockBuilder builder) {
    if (sourceType instanceof VarcharType) {
      String val = VarcharType.VARCHAR.getSlice(input, pos).toStringUtf8();
      BooleanType.BOOLEAN.writeBoolean(builder, Boolean.parseBoolean(val));
    } else if (sourceType instanceof BigintType) {
      BooleanType.BOOLEAN.writeBoolean(builder, BigintType.BIGINT.getLong(input, pos) != 0);
    } else {
      throw new UnsupportedOperationException("Cannot TRY_CAST " + sourceType + " to BOOLEAN");
    }
  }
}
