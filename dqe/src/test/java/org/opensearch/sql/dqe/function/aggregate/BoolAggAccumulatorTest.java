/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.function.aggregate;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.BooleanType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("BoolAggAccumulator")
class BoolAggAccumulatorTest {

  @Test
  @DisplayName("bool_and([true, true, true]) = true")
  void boolAndAllTrue() {
    Block block = buildBoolBlock(true, true, true);
    Accumulator acc = new BoolAggAccumulator(true);
    acc.addBlock(block, 3);

    BlockBuilder builder = BooleanType.BOOLEAN.createBlockBuilder(null, 1);
    acc.writeFinalTo(builder);
    assertTrue(BooleanType.BOOLEAN.getBoolean(builder.build(), 0));
  }

  @Test
  @DisplayName("bool_and([true, false]) = false")
  void boolAndWithFalse() {
    Block block = buildBoolBlock(true, false);
    Accumulator acc = new BoolAggAccumulator(true);
    acc.addBlock(block, 2);

    BlockBuilder builder = BooleanType.BOOLEAN.createBlockBuilder(null, 1);
    acc.writeFinalTo(builder);
    assertFalse(BooleanType.BOOLEAN.getBoolean(builder.build(), 0));
  }

  @Test
  @DisplayName("bool_or([false, false, true]) = true")
  void boolOrWithTrue() {
    Block block = buildBoolBlock(false, false, true);
    Accumulator acc = new BoolAggAccumulator(false);
    acc.addBlock(block, 3);

    BlockBuilder builder = BooleanType.BOOLEAN.createBlockBuilder(null, 1);
    acc.writeFinalTo(builder);
    assertTrue(BooleanType.BOOLEAN.getBoolean(builder.build(), 0));
  }

  @Test
  @DisplayName("bool_or([false, false]) = false")
  void boolOrAllFalse() {
    Block block = buildBoolBlock(false, false);
    Accumulator acc = new BoolAggAccumulator(false);
    acc.addBlock(block, 2);

    BlockBuilder builder = BooleanType.BOOLEAN.createBlockBuilder(null, 1);
    acc.writeFinalTo(builder);
    assertFalse(BooleanType.BOOLEAN.getBoolean(builder.build(), 0));
  }

  @Test
  @DisplayName("Null values are skipped in bool_and")
  void nullsSkippedBoolAnd() {
    BlockBuilder bb = BooleanType.BOOLEAN.createBlockBuilder(null, 3);
    BooleanType.BOOLEAN.writeBoolean(bb, true);
    bb.appendNull();
    BooleanType.BOOLEAN.writeBoolean(bb, true);
    Block block = bb.build();

    Accumulator acc = new BoolAggAccumulator(true);
    acc.addBlock(block, 3);

    BlockBuilder builder = BooleanType.BOOLEAN.createBlockBuilder(null, 1);
    acc.writeFinalTo(builder);
    assertTrue(BooleanType.BOOLEAN.getBoolean(builder.build(), 0));
  }

  @Test
  @DisplayName("Empty input returns null")
  void emptyReturnsNull() {
    Accumulator acc = new BoolAggAccumulator(true);
    BlockBuilder builder = BooleanType.BOOLEAN.createBlockBuilder(null, 1);
    acc.writeFinalTo(builder);
    assertTrue(builder.build().isNull(0));
  }

  private static Block buildBoolBlock(boolean... values) {
    BlockBuilder builder = BooleanType.BOOLEAN.createBlockBuilder(null, values.length);
    for (boolean v : values) {
      BooleanType.BOOLEAN.writeBoolean(builder, v);
    }
    return builder.build();
  }
}
