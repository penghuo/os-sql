/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.function.aggregate;

import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.Type;

/**
 * Accumulator for COUNT aggregate. Supports both COUNT(*) (count all rows) and COUNT(column) (count
 * non-null values).
 */
public class CountAccumulator implements Accumulator {

  private final boolean countStar;
  private long count = 0;

  /**
   * Create a CountAccumulator.
   *
   * @param countStar if true, counts all rows regardless of nulls; if false, counts only non-null
   *     positions
   */
  public CountAccumulator(boolean countStar) {
    this.countStar = countStar;
  }

  @Override
  public void addBlock(Block block, int positionCount) {
    if (countStar) {
      count += positionCount;
    } else {
      for (int i = 0; i < positionCount; i++) {
        if (!block.isNull(i)) {
          count++;
        }
      }
    }
  }

  @Override
  public void writeFinalTo(BlockBuilder builder) {
    BigintType.BIGINT.writeLong(builder, count);
  }

  /** Factory for COUNT(*) — counts all rows. */
  public static AggregateAccumulatorFactory factory() {
    return new AggregateAccumulatorFactory() {
      @Override
      public Accumulator createAccumulator() {
        return new CountAccumulator(true);
      }

      @Override
      public Type getIntermediateType() {
        return BigintType.BIGINT;
      }

      @Override
      public Type getOutputType() {
        return BigintType.BIGINT;
      }
    };
  }

  /** Factory for COUNT(column) — counts non-null values only. */
  public static AggregateAccumulatorFactory factoryForColumn() {
    return new AggregateAccumulatorFactory() {
      @Override
      public Accumulator createAccumulator() {
        return new CountAccumulator(false);
      }

      @Override
      public Type getIntermediateType() {
        return BigintType.BIGINT;
      }

      @Override
      public Type getOutputType() {
        return BigintType.BIGINT;
      }
    };
  }
}
