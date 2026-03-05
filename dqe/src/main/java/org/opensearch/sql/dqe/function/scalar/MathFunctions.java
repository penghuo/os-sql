/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.function.scalar;

import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.DoubleType;

/**
 * Vectorized implementations of standard math scalar functions. Each static method returns a {@link
 * ScalarFunctionImplementation} that operates on Trino Blocks.
 */
public final class MathFunctions {

  private MathFunctions() {}

  /** abs(double) -> double: absolute value. */
  public static ScalarFunctionImplementation abs() {
    return unaryDouble(Math::abs);
  }

  /** ceil(double) -> double: smallest integer >= value. */
  public static ScalarFunctionImplementation ceil() {
    return unaryDouble(Math::ceil);
  }

  /** floor(double) -> double: largest integer <= value. */
  public static ScalarFunctionImplementation floor() {
    return unaryDouble(Math::floor);
  }

  /** round(double, bigint) -> double: round to N decimal places. */
  public static ScalarFunctionImplementation round() {
    return (args, positionCount) -> {
      Block valBlock = args[0];
      Block scaleBlock = args[1];
      BlockBuilder builder = DoubleType.DOUBLE.createBlockBuilder(null, positionCount);
      for (int pos = 0; pos < positionCount; pos++) {
        if (valBlock.isNull(pos) || scaleBlock.isNull(pos)) {
          builder.appendNull();
        } else {
          double value = DoubleType.DOUBLE.getDouble(valBlock, pos);
          long scale = BigintType.BIGINT.getLong(scaleBlock, pos);
          double factor = Math.pow(10, scale);
          DoubleType.DOUBLE.writeDouble(builder, Math.round(value * factor) / factor);
        }
      }
      return builder.build();
    };
  }

  /** truncate(double, bigint) -> double: truncate to N decimal places. */
  public static ScalarFunctionImplementation truncate() {
    return (args, positionCount) -> {
      Block valBlock = args[0];
      Block scaleBlock = args[1];
      BlockBuilder builder = DoubleType.DOUBLE.createBlockBuilder(null, positionCount);
      for (int pos = 0; pos < positionCount; pos++) {
        if (valBlock.isNull(pos) || scaleBlock.isNull(pos)) {
          builder.appendNull();
        } else {
          double value = DoubleType.DOUBLE.getDouble(valBlock, pos);
          long scale = BigintType.BIGINT.getLong(scaleBlock, pos);
          double factor = Math.pow(10, scale);
          double truncated = Math.floor(Math.abs(value) * factor) / factor;
          DoubleType.DOUBLE.writeDouble(builder, value < 0 ? -truncated : truncated);
        }
      }
      return builder.build();
    };
  }

  /** power(double, double) -> double: first argument raised to the power of the second. */
  public static ScalarFunctionImplementation power() {
    return binaryDouble(Math::pow);
  }

  /** sqrt(double) -> double: square root. */
  public static ScalarFunctionImplementation sqrt() {
    return unaryDouble(Math::sqrt);
  }

  /** cbrt(double) -> double: cube root. */
  public static ScalarFunctionImplementation cbrt() {
    return unaryDouble(Math::cbrt);
  }

  /** exp(double) -> double: Euler's number raised to the given power. */
  public static ScalarFunctionImplementation exp() {
    return unaryDouble(Math::exp);
  }

  /** ln(double) -> double: natural logarithm. */
  public static ScalarFunctionImplementation ln() {
    return unaryDouble(Math::log);
  }

  /** log2(double) -> double: base-2 logarithm. */
  public static ScalarFunctionImplementation log2() {
    return unaryDouble(x -> Math.log(x) / Math.log(2));
  }

  /** log10(double) -> double: base-10 logarithm. */
  public static ScalarFunctionImplementation log10() {
    return unaryDouble(Math::log10);
  }

  /** mod(double, double) -> double: remainder of division. */
  public static ScalarFunctionImplementation mod() {
    return binaryDouble((a, b) -> a % b);
  }

  /** sign(double) -> double: signum function. */
  public static ScalarFunctionImplementation sign() {
    return unaryDouble(Math::signum);
  }

  /** pi() -> double: the constant Pi. Zero-argument function. */
  public static ScalarFunctionImplementation pi() {
    return constant(Math.PI);
  }

  /** e() -> double: Euler's number. Zero-argument function. */
  public static ScalarFunctionImplementation e() {
    return constant(Math.E);
  }

  /** radians(double) -> double: convert degrees to radians. */
  public static ScalarFunctionImplementation radians() {
    return unaryDouble(Math::toRadians);
  }

  /** degrees(double) -> double: convert radians to degrees. */
  public static ScalarFunctionImplementation degrees() {
    return unaryDouble(Math::toDegrees);
  }

  // --------------- internal helpers ---------------

  @FunctionalInterface
  private interface DoubleUnaryOp {
    double apply(double value);
  }

  @FunctionalInterface
  private interface DoubleBinaryOp {
    double apply(double left, double right);
  }

  private static ScalarFunctionImplementation unaryDouble(DoubleUnaryOp op) {
    return (args, positionCount) -> {
      Block input = args[0];
      BlockBuilder builder = DoubleType.DOUBLE.createBlockBuilder(null, positionCount);
      for (int pos = 0; pos < positionCount; pos++) {
        if (input.isNull(pos)) {
          builder.appendNull();
        } else {
          double value = DoubleType.DOUBLE.getDouble(input, pos);
          DoubleType.DOUBLE.writeDouble(builder, op.apply(value));
        }
      }
      return builder.build();
    };
  }

  private static ScalarFunctionImplementation binaryDouble(DoubleBinaryOp op) {
    return (args, positionCount) -> {
      Block leftBlock = args[0];
      Block rightBlock = args[1];
      BlockBuilder builder = DoubleType.DOUBLE.createBlockBuilder(null, positionCount);
      for (int pos = 0; pos < positionCount; pos++) {
        if (leftBlock.isNull(pos) || rightBlock.isNull(pos)) {
          builder.appendNull();
        } else {
          double l = DoubleType.DOUBLE.getDouble(leftBlock, pos);
          double r = DoubleType.DOUBLE.getDouble(rightBlock, pos);
          DoubleType.DOUBLE.writeDouble(builder, op.apply(l, r));
        }
      }
      return builder.build();
    };
  }

  private static ScalarFunctionImplementation constant(double value) {
    return (args, positionCount) -> {
      BlockBuilder builder = DoubleType.DOUBLE.createBlockBuilder(null, positionCount);
      for (int i = 0; i < positionCount; i++) {
        DoubleType.DOUBLE.writeDouble(builder, value);
      }
      return builder.build();
    };
  }
}
