/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.function.aggregate;

import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.Type;

/**
 * Accumulator for SUM aggregate. Handles both integer types (producing BIGINT) and double types
 * (producing DOUBLE). Returns null when no non-null values have been seen.
 */
public class SumAccumulator implements Accumulator {

  private final Type inputType;
  private double sum = 0;
  private boolean hasValue = false;

  /**
   * Create a SumAccumulator.
   *
   * @param inputType the type of the input column (BigintType or DoubleType)
   */
  public SumAccumulator(Type inputType) {
    this.inputType = inputType;
  }

  @Override
  public void addBlock(Block block, int positionCount) {
    for (int i = 0; i < positionCount; i++) {
      if (!block.isNull(i)) {
        hasValue = true;
        if (inputType instanceof DoubleType) {
          sum += DoubleType.DOUBLE.getDouble(block, i);
        } else {
          sum += inputType.getLong(block, i);
        }
      }
    }
  }

  @Override
  public void writeFinalTo(BlockBuilder builder) {
    if (!hasValue) {
      builder.appendNull();
    } else if (inputType instanceof DoubleType) {
      DoubleType.DOUBLE.writeDouble(builder, sum);
    } else {
      BigintType.BIGINT.writeLong(builder, (long) sum);
    }
  }

  /** Factory for SUM(column) aggregate. */
  public static AggregateAccumulatorFactory factory(Type inputType) {
    return new AggregateAccumulatorFactory() {
      @Override
      public Accumulator createAccumulator() {
        return new SumAccumulator(inputType);
      }

      @Override
      public Type getIntermediateType() {
        return inputType instanceof DoubleType ? DoubleType.DOUBLE : BigintType.BIGINT;
      }

      @Override
      public Type getOutputType() {
        return inputType instanceof DoubleType ? DoubleType.DOUBLE : BigintType.BIGINT;
      }
    };
  }
}
