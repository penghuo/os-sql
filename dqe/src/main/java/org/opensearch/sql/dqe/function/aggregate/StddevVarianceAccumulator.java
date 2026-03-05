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
 * Accumulator for STDDEV and VARIANCE using Welford's online algorithm for numerical stability.
 * Computes sample standard deviation / variance (N-1 denominator).
 */
public class StddevVarianceAccumulator implements Accumulator {

  private final Type inputType;
  private final boolean isStddev;
  private long count;
  private double mean;
  private double m2;

  /**
   * @param inputType the numeric input type
   * @param isStddev if true, produces stddev; if false, produces variance
   */
  public StddevVarianceAccumulator(Type inputType, boolean isStddev) {
    this.inputType = inputType;
    this.isStddev = isStddev;
    this.count = 0;
    this.mean = 0;
    this.m2 = 0;
  }

  @Override
  public void addBlock(Block block, int positionCount) {
    for (int pos = 0; pos < positionCount; pos++) {
      if (block.isNull(pos)) {
        continue;
      }
      double value = readDouble(block, pos);
      count++;
      double delta = value - mean;
      mean += delta / count;
      double delta2 = value - mean;
      m2 += delta * delta2;
    }
  }

  @Override
  public void writeFinalTo(BlockBuilder builder) {
    if (count < 2) {
      builder.appendNull();
      return;
    }
    double variance = m2 / (count - 1);
    DoubleType.DOUBLE.writeDouble(builder, isStddev ? Math.sqrt(variance) : variance);
  }

  private double readDouble(Block block, int pos) {
    if (inputType instanceof DoubleType) {
      return DoubleType.DOUBLE.getDouble(block, pos);
    }
    return inputType.getLong(block, pos);
  }

  /** Factory for STDDEV_SAMP. */
  public static AggregateAccumulatorFactory stddevFactory(Type inputType) {
    return new AggregateAccumulatorFactory() {
      @Override
      public Accumulator createAccumulator() {
        return new StddevVarianceAccumulator(inputType, true);
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

  /** Factory for VAR_SAMP. */
  public static AggregateAccumulatorFactory varianceFactory(Type inputType) {
    return new AggregateAccumulatorFactory() {
      @Override
      public Accumulator createAccumulator() {
        return new StddevVarianceAccumulator(inputType, false);
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
