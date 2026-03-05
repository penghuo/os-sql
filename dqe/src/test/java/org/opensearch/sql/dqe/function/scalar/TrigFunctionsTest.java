/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.function.scalar;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.DoubleType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** Tests for {@link TrigFunctions}. */
@DisplayName("TrigFunctions")
class TrigFunctionsTest {

  @Test
  @DisplayName("sin(0) returns 0.0")
  void sinZero() {
    Block result = TrigFunctions.sin().evaluate(new Block[] {buildDoubleBlock(0.0)}, 1);
    assertEquals(0.0, DoubleType.DOUBLE.getDouble(result, 0), 0.001);
  }

  @Test
  @DisplayName("cos(0) returns 1.0")
  void cosZero() {
    Block result = TrigFunctions.cos().evaluate(new Block[] {buildDoubleBlock(0.0)}, 1);
    assertEquals(1.0, DoubleType.DOUBLE.getDouble(result, 0), 0.001);
  }

  @Test
  @DisplayName("tan(0) returns 0.0")
  void tanZero() {
    Block result = TrigFunctions.tan().evaluate(new Block[] {buildDoubleBlock(0.0)}, 1);
    assertEquals(0.0, DoubleType.DOUBLE.getDouble(result, 0), 0.001);
  }

  @Test
  @DisplayName("asin(0) returns 0.0")
  void asinZero() {
    Block result = TrigFunctions.asin().evaluate(new Block[] {buildDoubleBlock(0.0)}, 1);
    assertEquals(0.0, DoubleType.DOUBLE.getDouble(result, 0), 0.001);
  }

  @Test
  @DisplayName("acos(1) returns 0.0")
  void acosOne() {
    Block result = TrigFunctions.acos().evaluate(new Block[] {buildDoubleBlock(1.0)}, 1);
    assertEquals(0.0, DoubleType.DOUBLE.getDouble(result, 0), 0.001);
  }

  @Test
  @DisplayName("atan(0) returns 0.0")
  void atanZero() {
    Block result = TrigFunctions.atan().evaluate(new Block[] {buildDoubleBlock(0.0)}, 1);
    assertEquals(0.0, DoubleType.DOUBLE.getDouble(result, 0), 0.001);
  }

  @Test
  @DisplayName("atan2(1, 1) returns PI/4")
  void atan2OneOne() {
    Block yBlock = buildDoubleBlock(1.0);
    Block xBlock = buildDoubleBlock(1.0);
    Block result = TrigFunctions.atan2().evaluate(new Block[] {yBlock, xBlock}, 1);
    assertEquals(Math.PI / 4, DoubleType.DOUBLE.getDouble(result, 0), 0.001);
  }

  @Test
  @DisplayName("NULL input produces null output for unary trig functions")
  void nullPropagationUnary() {
    BlockBuilder builder = DoubleType.DOUBLE.createBlockBuilder(null, 1);
    builder.appendNull();
    Block nullBlock = builder.build();

    assertTrue(TrigFunctions.sin().evaluate(new Block[] {nullBlock}, 1).isNull(0));
    assertTrue(TrigFunctions.cos().evaluate(new Block[] {nullBlock}, 1).isNull(0));
    assertTrue(TrigFunctions.tan().evaluate(new Block[] {nullBlock}, 1).isNull(0));
    assertTrue(TrigFunctions.asin().evaluate(new Block[] {nullBlock}, 1).isNull(0));
    assertTrue(TrigFunctions.acos().evaluate(new Block[] {nullBlock}, 1).isNull(0));
    assertTrue(TrigFunctions.atan().evaluate(new Block[] {nullBlock}, 1).isNull(0));
  }

  @Test
  @DisplayName("NULL input produces null output for atan2")
  void nullPropagationAtan2() {
    BlockBuilder nullBuilder = DoubleType.DOUBLE.createBlockBuilder(null, 1);
    nullBuilder.appendNull();
    Block nullBlock = nullBuilder.build();
    Block validBlock = buildDoubleBlock(1.0);

    // Both null
    assertTrue(TrigFunctions.atan2().evaluate(new Block[] {nullBlock, nullBlock}, 1).isNull(0));
    // First null
    assertTrue(TrigFunctions.atan2().evaluate(new Block[] {nullBlock, validBlock}, 1).isNull(0));
    // Second null
    assertTrue(TrigFunctions.atan2().evaluate(new Block[] {validBlock, nullBlock}, 1).isNull(0));
  }

  @Test
  @DisplayName("Multiple positions are processed correctly")
  void multiplePositions() {
    BlockBuilder builder = DoubleType.DOUBLE.createBlockBuilder(null, 2);
    DoubleType.DOUBLE.writeDouble(builder, 0.0);
    DoubleType.DOUBLE.writeDouble(builder, Math.PI / 2);
    Block input = builder.build();

    Block result = TrigFunctions.sin().evaluate(new Block[] {input}, 2);
    assertEquals(0.0, DoubleType.DOUBLE.getDouble(result, 0), 0.001);
    assertEquals(1.0, DoubleType.DOUBLE.getDouble(result, 1), 0.001);
  }

  private static Block buildDoubleBlock(double value) {
    BlockBuilder builder = DoubleType.DOUBLE.createBlockBuilder(null, 1);
    DoubleType.DOUBLE.writeDouble(builder, value);
    return builder.build();
  }
}
