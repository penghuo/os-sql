/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.function.aggregate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.airlift.slice.Slices;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.VarcharType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@DisplayName("Accumulator implementations")
class AccumulatorTest {

  // --------------- CountAccumulator ---------------

  @Nested
  @DisplayName("CountAccumulator")
  class CountTests {

    @Test
    @DisplayName("COUNT(*) counts all positions")
    void countStarCountsAllPositions() {
      Accumulator acc = new CountAccumulator(true);
      Block block = buildBigintBlock(10L, 20L, 30L);
      acc.addBlock(block, 3);

      long result = readBigint(acc);
      assertEquals(3L, result);
    }

    @Test
    @DisplayName("COUNT(column) skips null positions")
    void countColumnSkipsNulls() {
      Accumulator acc = new CountAccumulator(false);
      // Build block: [10, null, 30]
      Block block = buildBigintBlockWithNulls(10L, null, 30L);
      acc.addBlock(block, 3);

      long result = readBigint(acc);
      assertEquals(2L, result);
    }

    @Test
    @DisplayName("COUNT(*) factory produces correct types")
    void countStarFactory() {
      AggregateAccumulatorFactory factory = CountAccumulator.factory();
      assertEquals(BigintType.BIGINT, factory.getIntermediateType());
      assertEquals(BigintType.BIGINT, factory.getOutputType());

      Accumulator acc = factory.createAccumulator();
      acc.addBlock(buildBigintBlock(1L, 2L), 2);
      assertEquals(2L, readBigint(acc));
    }

    @Test
    @DisplayName("COUNT(column) factory produces correct types")
    void countColumnFactory() {
      AggregateAccumulatorFactory factory = CountAccumulator.factoryForColumn();
      assertEquals(BigintType.BIGINT, factory.getIntermediateType());
      assertEquals(BigintType.BIGINT, factory.getOutputType());
    }
  }

  // --------------- SumAccumulator ---------------

  @Nested
  @DisplayName("SumAccumulator")
  class SumTests {

    @Test
    @DisplayName("SUM of BIGINT values")
    void sumBigint() {
      Accumulator acc = new SumAccumulator(BigintType.BIGINT);
      Block block = buildBigintBlock(10L, 20L, 30L);
      acc.addBlock(block, 3);

      long result = readBigint(acc);
      assertEquals(60L, result);
    }

    @Test
    @DisplayName("SUM of DOUBLE values")
    void sumDouble() {
      Accumulator acc = new SumAccumulator(DoubleType.DOUBLE);
      Block block = buildDoubleBlock(1.5, 2.5);
      acc.addBlock(block, 2);

      double result = readDouble(acc);
      assertEquals(4.0, result, 0.001);
    }

    @Test
    @DisplayName("SUM skips null values")
    void sumWithNulls() {
      Accumulator acc = new SumAccumulator(BigintType.BIGINT);
      Block block = buildBigintBlockWithNulls(10L, null, 30L);
      acc.addBlock(block, 3);

      long result = readBigint(acc);
      assertEquals(40L, result);
    }

    @Test
    @DisplayName("SUM of empty block returns null")
    void sumEmpty() {
      Accumulator acc = new SumAccumulator(BigintType.BIGINT);
      Block block = buildBigintBlock();
      acc.addBlock(block, 0);

      BlockBuilder builder = BigintType.BIGINT.createBlockBuilder(null, 1);
      acc.writeFinalTo(builder);
      Block result = builder.build();
      assertTrue(result.isNull(0));
    }

    @Test
    @DisplayName("SUM factory produces correct types for BIGINT")
    void sumBigintFactory() {
      AggregateAccumulatorFactory factory = SumAccumulator.factory(BigintType.BIGINT);
      assertEquals(BigintType.BIGINT, factory.getIntermediateType());
      assertEquals(BigintType.BIGINT, factory.getOutputType());
    }

    @Test
    @DisplayName("SUM factory produces correct types for DOUBLE")
    void sumDoubleFactory() {
      AggregateAccumulatorFactory factory = SumAccumulator.factory(DoubleType.DOUBLE);
      assertEquals(DoubleType.DOUBLE, factory.getIntermediateType());
      assertEquals(DoubleType.DOUBLE, factory.getOutputType());
    }
  }

  // --------------- AvgAccumulator ---------------

  @Nested
  @DisplayName("AvgAccumulator")
  class AvgTests {

    @Test
    @DisplayName("AVG of BIGINT values")
    void avgBigint() {
      Accumulator acc = new AvgAccumulator(BigintType.BIGINT);
      Block block = buildBigintBlock(2L, 4L, 6L);
      acc.addBlock(block, 3);

      double result = readDouble(acc);
      assertEquals(4.0, result, 0.001);
    }

    @Test
    @DisplayName("AVG of empty block returns null")
    void avgEmpty() {
      Accumulator acc = new AvgAccumulator(BigintType.BIGINT);
      Block block = buildBigintBlock();
      acc.addBlock(block, 0);

      BlockBuilder builder = DoubleType.DOUBLE.createBlockBuilder(null, 1);
      acc.writeFinalTo(builder);
      Block result = builder.build();
      assertTrue(result.isNull(0));
    }

    @Test
    @DisplayName("AVG factory produces DOUBLE output type")
    void avgFactory() {
      AggregateAccumulatorFactory factory = AvgAccumulator.factory(BigintType.BIGINT);
      assertEquals(DoubleType.DOUBLE, factory.getIntermediateType());
      assertEquals(DoubleType.DOUBLE, factory.getOutputType());
    }
  }

  // --------------- MinMaxAccumulator ---------------

  @Nested
  @DisplayName("MinMaxAccumulator")
  class MinMaxTests {

    @Test
    @DisplayName("MIN of BIGINT values")
    void minBigint() {
      Accumulator acc = new MinMaxAccumulator(BigintType.BIGINT, true);
      Block block = buildBigintBlock(30L, 10L, 20L);
      acc.addBlock(block, 3);

      long result = readBigint(acc);
      assertEquals(10L, result);
    }

    @Test
    @DisplayName("MAX of BIGINT values")
    void maxBigint() {
      Accumulator acc = new MinMaxAccumulator(BigintType.BIGINT, false);
      Block block = buildBigintBlock(30L, 10L, 20L);
      acc.addBlock(block, 3);

      long result = readBigint(acc);
      assertEquals(30L, result);
    }

    @Test
    @DisplayName("MIN and MAX skip null values")
    void minMaxWithNulls() {
      // MIN with nulls: [null, 10, null] -> 10
      Accumulator minAcc = new MinMaxAccumulator(BigintType.BIGINT, true);
      Block block = buildBigintBlockWithNulls(null, 10L, null);
      minAcc.addBlock(block, 3);
      assertEquals(10L, readBigint(minAcc));

      // MAX with nulls: [null, 10, null] -> 10
      Accumulator maxAcc = new MinMaxAccumulator(BigintType.BIGINT, false);
      maxAcc.addBlock(block, 3);
      assertEquals(10L, readBigint(maxAcc));
    }

    @Test
    @DisplayName("MIN of VARCHAR values")
    void minVarchar() {
      Accumulator acc = new MinMaxAccumulator(VarcharType.VARCHAR, true);
      Block block = buildVarcharBlock("cherry", "apple", "banana");
      acc.addBlock(block, 3);

      String result = readVarchar(acc);
      assertEquals("apple", result);
    }

    @Test
    @DisplayName("MAX of VARCHAR values")
    void maxVarchar() {
      Accumulator acc = new MinMaxAccumulator(VarcharType.VARCHAR, false);
      Block block = buildVarcharBlock("cherry", "apple", "banana");
      acc.addBlock(block, 3);

      String result = readVarchar(acc);
      assertEquals("cherry", result);
    }

    @Test
    @DisplayName("MIN of empty block returns null")
    void minEmpty() {
      Accumulator acc = new MinMaxAccumulator(BigintType.BIGINT, true);
      Block block = buildBigintBlock();
      acc.addBlock(block, 0);

      BlockBuilder builder = BigintType.BIGINT.createBlockBuilder(null, 1);
      acc.writeFinalTo(builder);
      Block result = builder.build();
      assertTrue(result.isNull(0));
    }

    @Test
    @DisplayName("minFactory produces correct types")
    void minFactory() {
      AggregateAccumulatorFactory factory = MinMaxAccumulator.minFactory(BigintType.BIGINT);
      assertEquals(BigintType.BIGINT, factory.getIntermediateType());
      assertEquals(BigintType.BIGINT, factory.getOutputType());
    }

    @Test
    @DisplayName("maxFactory produces correct types")
    void maxFactory() {
      AggregateAccumulatorFactory factory = MinMaxAccumulator.maxFactory(VarcharType.VARCHAR);
      assertEquals(VarcharType.VARCHAR, factory.getIntermediateType());
      assertEquals(VarcharType.VARCHAR, factory.getOutputType());
    }
  }

  // --------------- Helper methods ---------------

  /** Build a BIGINT Block from the given values. */
  private static Block buildBigintBlock(Long... values) {
    BlockBuilder builder = BigintType.BIGINT.createBlockBuilder(null, values.length);
    for (Long value : values) {
      BigintType.BIGINT.writeLong(builder, value);
    }
    return builder.build();
  }

  /** Build a BIGINT Block that may contain null values. */
  private static Block buildBigintBlockWithNulls(Long... values) {
    BlockBuilder builder = BigintType.BIGINT.createBlockBuilder(null, values.length);
    for (Long value : values) {
      if (value == null) {
        builder.appendNull();
      } else {
        BigintType.BIGINT.writeLong(builder, value);
      }
    }
    return builder.build();
  }

  /** Build a DOUBLE Block from the given values. */
  private static Block buildDoubleBlock(Double... values) {
    BlockBuilder builder = DoubleType.DOUBLE.createBlockBuilder(null, values.length);
    for (Double value : values) {
      DoubleType.DOUBLE.writeDouble(builder, value);
    }
    return builder.build();
  }

  /** Build a VARCHAR Block from the given values. */
  private static Block buildVarcharBlock(String... values) {
    BlockBuilder builder = VarcharType.VARCHAR.createBlockBuilder(null, values.length);
    for (String value : values) {
      VarcharType.VARCHAR.writeSlice(builder, Slices.utf8Slice(value));
    }
    return builder.build();
  }

  /** Write the accumulator's final result to a BIGINT BlockBuilder and read it back. */
  private static long readBigint(Accumulator acc) {
    BlockBuilder builder = BigintType.BIGINT.createBlockBuilder(null, 1);
    acc.writeFinalTo(builder);
    Block result = builder.build();
    return BigintType.BIGINT.getLong(result, 0);
  }

  /** Write the accumulator's final result to a DOUBLE BlockBuilder and read it back. */
  private static double readDouble(Accumulator acc) {
    BlockBuilder builder = DoubleType.DOUBLE.createBlockBuilder(null, 1);
    acc.writeFinalTo(builder);
    Block result = builder.build();
    return DoubleType.DOUBLE.getDouble(result, 0);
  }

  /** Write the accumulator's final result to a VARCHAR BlockBuilder and read it back. */
  private static String readVarchar(Accumulator acc) {
    BlockBuilder builder = VarcharType.VARCHAR.createBlockBuilder(null, 1);
    acc.writeFinalTo(builder);
    Block result = builder.build();
    return VarcharType.VARCHAR.getSlice(result, 0).toStringUtf8();
  }
}
