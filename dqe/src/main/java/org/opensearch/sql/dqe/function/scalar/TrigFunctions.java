/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.function.scalar;

import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.DoubleType;

/**
 * Trigonometric scalar functions. Each reads DOUBLE input, applies the corresponding {@link Math}
 * function, and writes DOUBLE output with NULL propagation.
 */
public final class TrigFunctions {

  private TrigFunctions() {}

  /** Returns the sine of a DOUBLE value (radians). */
  public static ScalarFunctionImplementation sin() {
    return unaryDoubleFunction(Math::sin);
  }

  /** Returns the cosine of a DOUBLE value (radians). */
  public static ScalarFunctionImplementation cos() {
    return unaryDoubleFunction(Math::cos);
  }

  /** Returns the tangent of a DOUBLE value (radians). */
  public static ScalarFunctionImplementation tan() {
    return unaryDoubleFunction(Math::tan);
  }

  /** Returns the arc sine of a DOUBLE value. */
  public static ScalarFunctionImplementation asin() {
    return unaryDoubleFunction(Math::asin);
  }

  /** Returns the arc cosine of a DOUBLE value. */
  public static ScalarFunctionImplementation acos() {
    return unaryDoubleFunction(Math::acos);
  }

  /** Returns the arc tangent of a DOUBLE value. */
  public static ScalarFunctionImplementation atan() {
    return unaryDoubleFunction(Math::atan);
  }

  /** Returns the two-argument arc tangent of DOUBLE values (y, x). */
  public static ScalarFunctionImplementation atan2() {
    return (args, positionCount) -> {
      Block yBlock = args[0];
      Block xBlock = args[1];
      BlockBuilder builder = DoubleType.DOUBLE.createBlockBuilder(null, positionCount);
      for (int pos = 0; pos < positionCount; pos++) {
        if (yBlock.isNull(pos) || xBlock.isNull(pos)) {
          builder.appendNull();
        } else {
          double y = DoubleType.DOUBLE.getDouble(yBlock, pos);
          double x = DoubleType.DOUBLE.getDouble(xBlock, pos);
          DoubleType.DOUBLE.writeDouble(builder, Math.atan2(y, x));
        }
      }
      return builder.build();
    };
  }

  private static ScalarFunctionImplementation unaryDoubleFunction(DoubleUnaryOp op) {
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

  @FunctionalInterface
  private interface DoubleUnaryOp {
    double apply(double value);
  }
}
