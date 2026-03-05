/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.function.aggregate;

import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.Type;

/**
 * Accumulator for BOOL_AND and BOOL_OR aggregate functions. BOOL_AND returns true if all non-null
 * values are true. BOOL_OR returns true if any non-null value is true.
 */
public class BoolAggAccumulator implements Accumulator {

  private final boolean isAnd;
  private boolean result;
  private boolean hasValue;

  /**
   * @param isAnd if true, computes BOOL_AND; if false, computes BOOL_OR
   */
  public BoolAggAccumulator(boolean isAnd) {
    this.isAnd = isAnd;
    this.result = isAnd; // AND starts true, OR starts false
    this.hasValue = false;
  }

  @Override
  public void addBlock(Block block, int positionCount) {
    for (int pos = 0; pos < positionCount; pos++) {
      if (block.isNull(pos)) {
        continue;
      }
      hasValue = true;
      boolean val = BooleanType.BOOLEAN.getBoolean(block, pos);
      if (isAnd) {
        result = result && val;
      } else {
        result = result || val;
      }
    }
  }

  @Override
  public void writeFinalTo(BlockBuilder builder) {
    if (!hasValue) {
      builder.appendNull();
    } else {
      BooleanType.BOOLEAN.writeBoolean(builder, result);
    }
  }

  /** Factory for BOOL_AND. */
  public static AggregateAccumulatorFactory boolAndFactory() {
    return new AggregateAccumulatorFactory() {
      @Override
      public Accumulator createAccumulator() {
        return new BoolAggAccumulator(true);
      }

      @Override
      public Type getIntermediateType() {
        return BooleanType.BOOLEAN;
      }

      @Override
      public Type getOutputType() {
        return BooleanType.BOOLEAN;
      }
    };
  }

  /** Factory for BOOL_OR. */
  public static AggregateAccumulatorFactory boolOrFactory() {
    return new AggregateAccumulatorFactory() {
      @Override
      public Accumulator createAccumulator() {
        return new BoolAggAccumulator(false);
      }

      @Override
      public Type getIntermediateType() {
        return BooleanType.BOOLEAN;
      }

      @Override
      public Type getOutputType() {
        return BooleanType.BOOLEAN;
      }
    };
  }
}
