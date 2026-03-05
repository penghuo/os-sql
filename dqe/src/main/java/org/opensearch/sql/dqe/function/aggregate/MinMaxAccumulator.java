/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.function.aggregate;

import io.airlift.slice.Slices;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

/**
 * Accumulator for MIN and MAX aggregates. Supports BIGINT, INTEGER, DOUBLE, and VARCHAR types.
 * Returns null when no non-null values have been seen.
 */
public class MinMaxAccumulator implements Accumulator {

  private final Type inputType;
  private final boolean isMin;
  private Object currentValue = null;

  /**
   * Create a MinMaxAccumulator.
   *
   * @param inputType the type of the input column
   * @param isMin true for MIN, false for MAX
   */
  public MinMaxAccumulator(Type inputType, boolean isMin) {
    this.inputType = inputType;
    this.isMin = isMin;
  }

  @Override
  public void addBlock(Block block, int positionCount) {
    for (int i = 0; i < positionCount; i++) {
      if (block.isNull(i)) {
        continue;
      }
      Object value = readValue(block, i);
      if (currentValue == null || shouldReplace(value)) {
        currentValue = value;
      }
    }
  }

  @SuppressWarnings("unchecked")
  private boolean shouldReplace(Object value) {
    int cmp = ((Comparable<Object>) value).compareTo(currentValue);
    return isMin ? cmp < 0 : cmp > 0;
  }

  private Object readValue(Block block, int position) {
    if (inputType instanceof BigintType) {
      return BigintType.BIGINT.getLong(block, position);
    } else if (inputType instanceof IntegerType) {
      return IntegerType.INTEGER.getLong(block, position);
    } else if (inputType instanceof DoubleType) {
      return DoubleType.DOUBLE.getDouble(block, position);
    } else if (inputType instanceof VarcharType) {
      return VarcharType.VARCHAR.getSlice(block, position).toStringUtf8();
    }
    throw new UnsupportedOperationException("Unsupported type for MIN/MAX: " + inputType);
  }

  @Override
  public void writeFinalTo(BlockBuilder builder) {
    if (currentValue == null) {
      builder.appendNull();
    } else if (inputType instanceof BigintType) {
      BigintType.BIGINT.writeLong(builder, (Long) currentValue);
    } else if (inputType instanceof IntegerType) {
      IntegerType.INTEGER.writeLong(builder, (Long) currentValue);
    } else if (inputType instanceof DoubleType) {
      DoubleType.DOUBLE.writeDouble(builder, (Double) currentValue);
    } else if (inputType instanceof VarcharType) {
      VarcharType.VARCHAR.writeSlice(builder, Slices.utf8Slice((String) currentValue));
    } else {
      throw new UnsupportedOperationException("Unsupported type for MIN/MAX: " + inputType);
    }
  }

  /** Factory for MIN(column) aggregate. */
  public static AggregateAccumulatorFactory minFactory(Type inputType) {
    return new AggregateAccumulatorFactory() {
      @Override
      public Accumulator createAccumulator() {
        return new MinMaxAccumulator(inputType, true);
      }

      @Override
      public Type getIntermediateType() {
        return inputType;
      }

      @Override
      public Type getOutputType() {
        return inputType;
      }
    };
  }

  /** Factory for MAX(column) aggregate. */
  public static AggregateAccumulatorFactory maxFactory(Type inputType) {
    return new AggregateAccumulatorFactory() {
      @Override
      public Accumulator createAccumulator() {
        return new MinMaxAccumulator(inputType, false);
      }

      @Override
      public Type getIntermediateType() {
        return inputType;
      }

      @Override
      public Type getOutputType() {
        return inputType;
      }
    };
  }
}
