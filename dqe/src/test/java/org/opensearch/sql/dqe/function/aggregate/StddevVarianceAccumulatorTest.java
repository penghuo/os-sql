/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.function.aggregate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.DoubleType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("StddevVarianceAccumulator")
class StddevVarianceAccumulatorTest {

  @Test
  @DisplayName("stddev([2,4,4,4,5,5,7,9]) is approximately 2.138")
  void stddevSample() {
    Block block = buildDoubleBlock(2, 4, 4, 4, 5, 5, 7, 9);
    Accumulator acc = new StddevVarianceAccumulator(DoubleType.DOUBLE, true);
    acc.addBlock(block, 8);

    BlockBuilder builder = DoubleType.DOUBLE.createBlockBuilder(null, 1);
    acc.writeFinalTo(builder);
    Block result = builder.build();

    assertEquals(2.138, DoubleType.DOUBLE.getDouble(result, 0), 0.001);
  }

  @Test
  @DisplayName("variance([2,4,4,4,5,5,7,9]) is approximately 4.571")
  void varianceSample() {
    Block block = buildDoubleBlock(2, 4, 4, 4, 5, 5, 7, 9);
    Accumulator acc = new StddevVarianceAccumulator(DoubleType.DOUBLE, false);
    acc.addBlock(block, 8);

    BlockBuilder builder = DoubleType.DOUBLE.createBlockBuilder(null, 1);
    acc.writeFinalTo(builder);
    Block result = builder.build();

    assertEquals(4.571, DoubleType.DOUBLE.getDouble(result, 0), 0.001);
  }

  @Test
  @DisplayName("Single value returns null (N-1 denominator)")
  void singleValueReturnsNull() {
    Block block = buildDoubleBlock(5.0);
    Accumulator acc = new StddevVarianceAccumulator(DoubleType.DOUBLE, true);
    acc.addBlock(block, 1);

    BlockBuilder builder = DoubleType.DOUBLE.createBlockBuilder(null, 1);
    acc.writeFinalTo(builder);
    Block result = builder.build();

    assertTrue(result.isNull(0));
  }

  @Test
  @DisplayName("Null values are skipped")
  void nullsAreSkipped() {
    BlockBuilder bb = DoubleType.DOUBLE.createBlockBuilder(null, 4);
    DoubleType.DOUBLE.writeDouble(bb, 2.0);
    bb.appendNull();
    DoubleType.DOUBLE.writeDouble(bb, 4.0);
    DoubleType.DOUBLE.writeDouble(bb, 6.0);
    Block block = bb.build();

    Accumulator acc = new StddevVarianceAccumulator(DoubleType.DOUBLE, false);
    acc.addBlock(block, 4);

    BlockBuilder builder = DoubleType.DOUBLE.createBlockBuilder(null, 1);
    acc.writeFinalTo(builder);
    Block result = builder.build();

    // variance of [2, 4, 6] = 4.0
    assertEquals(4.0, DoubleType.DOUBLE.getDouble(result, 0), 0.001);
  }

  private static Block buildDoubleBlock(double... values) {
    BlockBuilder builder = DoubleType.DOUBLE.createBlockBuilder(null, values.length);
    for (double v : values) {
      DoubleType.DOUBLE.writeDouble(builder, v);
    }
    return builder.build();
  }
}
