/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.function.aggregate;

import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.Type;

/**
 * Accumulator for AVG aggregate. Tracks a running sum and count, and produces a DOUBLE result equal
 * to sum/count. Returns null when no non-null values have been seen.
 */
public class AvgAccumulator implements Accumulator {

  private final Type inputType;
  private double sum = 0;
  private long count = 0;

  /**
   * Create an AvgAccumulator.
   *
   * @param inputType the type of the input column (BigintType or DoubleType)
   */
  public AvgAccumulator(Type inputType) {
    this.inputType = inputType;
  }

  @Override
  public void addBlock(Block block, int positionCount) {
    for (int i = 0; i < positionCount; i++) {
      if (!block.isNull(i)) {
        count++;
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
    if (count == 0) {
      builder.appendNull();
    } else {
      DoubleType.DOUBLE.writeDouble(builder, sum / count);
    }
  }

  /** Factory for AVG(column) aggregate. */
  public static AggregateAccumulatorFactory factory(Type inputType) {
    return new AggregateAccumulatorFactory() {
      @Override
      public Accumulator createAccumulator() {
        return new AvgAccumulator(inputType);
      }

      @Override
      public Type getIntermediateType() {
        return DoubleType.DOUBLE;
      }

      @Override
      public Type getOutputType() {
        return DoubleType.DOUBLE;
      }
    };
  }
}
