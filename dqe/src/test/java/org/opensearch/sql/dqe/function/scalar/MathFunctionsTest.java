/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.function.scalar;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.DoubleType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("MathFunctions")
class MathFunctionsTest {

  // ---------- abs ----------

  @Test
  @DisplayName("abs(-5.0) returns 5.0")
  void absNegative() {
    Block result = MathFunctions.abs().evaluate(doubleArgs(-5.0), 1);
    assertEquals(5.0, DoubleType.DOUBLE.getDouble(result, 0), 0.001);
  }

  @Test
  @DisplayName("abs(null) returns null")
  void absNull() {
    Block input = buildDoubleBlockWithNull();
    Block result = MathFunctions.abs().evaluate(new Block[] {input}, 1);
    assertTrue(result.isNull(0));
  }

  // ---------- ceil / floor ----------

  @Test
  @DisplayName("ceil(3.2) returns 4.0")
  void ceilValue() {
    Block result = MathFunctions.ceil().evaluate(doubleArgs(3.2), 1);
    assertEquals(4.0, DoubleType.DOUBLE.getDouble(result, 0), 0.001);
  }

  @Test
  @DisplayName("floor(3.8) returns 3.0")
  void floorValue() {
    Block result = MathFunctions.floor().evaluate(doubleArgs(3.8), 1);
    assertEquals(3.0, DoubleType.DOUBLE.getDouble(result, 0), 0.001);
  }

  // ---------- round ----------

  @Test
  @DisplayName("round(3.14159, 2) returns 3.14")
  void roundTwoDecimals() {
    Block[] args = doubleBigintArgs(3.14159, 2);
    Block result = MathFunctions.round().evaluate(args, 1);
    assertEquals(3.14, DoubleType.DOUBLE.getDouble(result, 0), 0.001);
  }

  // ---------- truncate ----------

  @Test
  @DisplayName("truncate(3.14159, 2) returns 3.14")
  void truncateTwoDecimals() {
    Block[] args = doubleBigintArgs(3.14159, 2);
    Block result = MathFunctions.truncate().evaluate(args, 1);
    assertEquals(3.14, DoubleType.DOUBLE.getDouble(result, 0), 0.001);
  }

  @Test
  @DisplayName("truncate(-3.14159, 2) returns -3.14")
  void truncateNegative() {
    Block[] args = doubleBigintArgs(-3.14159, 2);
    Block result = MathFunctions.truncate().evaluate(args, 1);
    assertEquals(-3.14, DoubleType.DOUBLE.getDouble(result, 0), 0.001);
  }

  // ---------- power ----------

  @Test
  @DisplayName("power(2.0, 10.0) returns 1024.0")
  void powerOf2() {
    Block result = MathFunctions.power().evaluate(twoDoubleArgs(2.0, 10.0), 1);
    assertEquals(1024.0, DoubleType.DOUBLE.getDouble(result, 0), 0.001);
  }

  // ---------- sqrt ----------

  @Test
  @DisplayName("sqrt(16.0) returns 4.0")
  void sqrtValue() {
    Block result = MathFunctions.sqrt().evaluate(doubleArgs(16.0), 1);
    assertEquals(4.0, DoubleType.DOUBLE.getDouble(result, 0), 0.001);
  }

  // ---------- cbrt ----------

  @Test
  @DisplayName("cbrt(27.0) returns 3.0")
  void cbrtValue() {
    Block result = MathFunctions.cbrt().evaluate(doubleArgs(27.0), 1);
    assertEquals(3.0, DoubleType.DOUBLE.getDouble(result, 0), 0.001);
  }

  // ---------- exp ----------

  @Test
  @DisplayName("exp(0.0) returns 1.0")
  void expZero() {
    Block result = MathFunctions.exp().evaluate(doubleArgs(0.0), 1);
    assertEquals(1.0, DoubleType.DOUBLE.getDouble(result, 0), 0.001);
  }

  // ---------- ln ----------

  @Test
  @DisplayName("ln(1.0) returns 0.0")
  void lnOne() {
    Block result = MathFunctions.ln().evaluate(doubleArgs(1.0), 1);
    assertEquals(0.0, DoubleType.DOUBLE.getDouble(result, 0), 0.001);
  }

  // ---------- log2 ----------

  @Test
  @DisplayName("log2(8.0) returns 3.0")
  void log2Of8() {
    Block result = MathFunctions.log2().evaluate(doubleArgs(8.0), 1);
    assertEquals(3.0, DoubleType.DOUBLE.getDouble(result, 0), 0.001);
  }

  // ---------- log10 ----------

  @Test
  @DisplayName("log10(1000.0) returns 3.0")
  void log10Of1000() {
    Block result = MathFunctions.log10().evaluate(doubleArgs(1000.0), 1);
    assertEquals(3.0, DoubleType.DOUBLE.getDouble(result, 0), 0.001);
  }

  // ---------- mod ----------

  @Test
  @DisplayName("mod(10.0, 3.0) returns 1.0")
  void modValue() {
    Block result = MathFunctions.mod().evaluate(twoDoubleArgs(10.0, 3.0), 1);
    assertEquals(1.0, DoubleType.DOUBLE.getDouble(result, 0), 0.001);
  }

  // ---------- sign ----------

  @Test
  @DisplayName("sign(-5.0) returns -1.0")
  void signNegative() {
    Block result = MathFunctions.sign().evaluate(doubleArgs(-5.0), 1);
    assertEquals(-1.0, DoubleType.DOUBLE.getDouble(result, 0), 0.001);
  }

  @Test
  @DisplayName("sign(0.0) returns 0.0")
  void signZero() {
    Block result = MathFunctions.sign().evaluate(doubleArgs(0.0), 1);
    assertEquals(0.0, DoubleType.DOUBLE.getDouble(result, 0), 0.001);
  }

  @Test
  @DisplayName("sign(5.0) returns 1.0")
  void signPositive() {
    Block result = MathFunctions.sign().evaluate(doubleArgs(5.0), 1);
    assertEquals(1.0, DoubleType.DOUBLE.getDouble(result, 0), 0.001);
  }

  // ---------- pi ----------

  @Test
  @DisplayName("pi() returns Math.PI")
  void piConstant() {
    Block result = MathFunctions.pi().evaluate(new Block[0], 1);
    assertEquals(Math.PI, DoubleType.DOUBLE.getDouble(result, 0), 1e-10);
  }

  // ---------- e ----------

  @Test
  @DisplayName("e() returns Math.E")
  void eConstant() {
    Block result = MathFunctions.e().evaluate(new Block[0], 1);
    assertEquals(Math.E, DoubleType.DOUBLE.getDouble(result, 0), 1e-10);
  }

  // ---------- radians / degrees ----------

  @Test
  @DisplayName("radians(180.0) returns PI")
  void radiansOf180() {
    Block result = MathFunctions.radians().evaluate(doubleArgs(180.0), 1);
    assertEquals(Math.PI, DoubleType.DOUBLE.getDouble(result, 0), 0.001);
  }

  @Test
  @DisplayName("degrees(PI) returns 180.0")
  void degreesOfPi() {
    Block result = MathFunctions.degrees().evaluate(doubleArgs(Math.PI), 1);
    assertEquals(180.0, DoubleType.DOUBLE.getDouble(result, 0), 0.001);
  }

  // ---------- vectorized evaluation ----------

  @Test
  @DisplayName("Vectorized abs across multiple positions")
  void absVectorized() {
    BlockBuilder builder = DoubleType.DOUBLE.createBlockBuilder(null, 3);
    DoubleType.DOUBLE.writeDouble(builder, -1.0);
    DoubleType.DOUBLE.writeDouble(builder, 0.0);
    DoubleType.DOUBLE.writeDouble(builder, 2.5);
    Block result = MathFunctions.abs().evaluate(new Block[] {builder.build()}, 3);
    assertEquals(1.0, DoubleType.DOUBLE.getDouble(result, 0), 0.001);
    assertEquals(0.0, DoubleType.DOUBLE.getDouble(result, 1), 0.001);
    assertEquals(2.5, DoubleType.DOUBLE.getDouble(result, 2), 0.001);
  }

  @Test
  @DisplayName("Zero-arg pi produces correct count of values")
  void piMultiplePositions() {
    Block result = MathFunctions.pi().evaluate(new Block[0], 3);
    assertEquals(3, result.getPositionCount());
    for (int i = 0; i < 3; i++) {
      assertEquals(Math.PI, DoubleType.DOUBLE.getDouble(result, i), 1e-10);
    }
  }

  // ---------- helpers ----------

  private static Block[] doubleArgs(double value) {
    BlockBuilder builder = DoubleType.DOUBLE.createBlockBuilder(null, 1);
    DoubleType.DOUBLE.writeDouble(builder, value);
    return new Block[] {builder.build()};
  }

  private static Block[] twoDoubleArgs(double left, double right) {
    BlockBuilder b1 = DoubleType.DOUBLE.createBlockBuilder(null, 1);
    DoubleType.DOUBLE.writeDouble(b1, left);
    BlockBuilder b2 = DoubleType.DOUBLE.createBlockBuilder(null, 1);
    DoubleType.DOUBLE.writeDouble(b2, right);
    return new Block[] {b1.build(), b2.build()};
  }

  private static Block[] doubleBigintArgs(double doubleVal, long longVal) {
    BlockBuilder b1 = DoubleType.DOUBLE.createBlockBuilder(null, 1);
    DoubleType.DOUBLE.writeDouble(b1, doubleVal);
    BlockBuilder b2 = BigintType.BIGINT.createBlockBuilder(null, 1);
    BigintType.BIGINT.writeLong(b2, longVal);
    return new Block[] {b1.build(), b2.build()};
  }

  private static Block buildDoubleBlockWithNull() {
    BlockBuilder builder = DoubleType.DOUBLE.createBlockBuilder(null, 1);
    builder.appendNull();
    return builder.build();
  }
}
