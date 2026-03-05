/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.function.expression;

import io.airlift.slice.Slices;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import lombok.Getter;

/**
 * Produces a constant-value Block for every row in the Page. Uses {@link RunLengthEncodedBlock} for
 * efficient representation of repeated values.
 */
@Getter
public class ConstantExpression implements BlockExpression {

  private final Object value;
  private final Type type;
  private final Block singleValueBlock;

  public ConstantExpression(Object value, Type type) {
    this.value = value;
    this.type = type;
    this.singleValueBlock = buildSingleValueBlock(value, type);
  }

  @Override
  public Block evaluate(Page page) {
    return RunLengthEncodedBlock.create(singleValueBlock, page.getPositionCount());
  }

  private static Block buildSingleValueBlock(Object value, Type type) {
    BlockBuilder builder = type.createBlockBuilder(null, 1);
    if (value == null) {
      builder.appendNull();
    } else if (type instanceof BigintType) {
      BigintType.BIGINT.writeLong(builder, ((Number) value).longValue());
    } else if (type instanceof DoubleType) {
      DoubleType.DOUBLE.writeDouble(builder, ((Number) value).doubleValue());
    } else if (type instanceof VarcharType) {
      VarcharType.VARCHAR.writeSlice(builder, Slices.utf8Slice(value.toString()));
    } else if (type instanceof BooleanType) {
      BooleanType.BOOLEAN.writeBoolean(builder, (Boolean) value);
    } else {
      throw new UnsupportedOperationException("Unsupported constant type: " + type);
    }
    return builder.build();
  }
}
