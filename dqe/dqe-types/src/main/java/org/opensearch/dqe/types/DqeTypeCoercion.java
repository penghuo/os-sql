/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.types;

import io.trino.spi.type.DecimalType;
import java.util.Objects;
import java.util.Optional;

/**
 * Implicit type widening rules for binary expressions and explicit CAST conversion matrix.
 *
 * <p>Implicit coercion rules (used for binary operations like {@code INTEGER + BIGINT}):
 *
 * <ul>
 *   <li>TINYINT -> SMALLINT -> INTEGER -> BIGINT (integer widening)
 *   <li>REAL -> DOUBLE (float widening)
 *   <li>integer types -> DOUBLE when mixed with floating point
 *   <li>DECIMAL widening follows precision/scale rules
 *   <li>TIMESTAMP(3) -> TIMESTAMP(9) (precision widening)
 * </ul>
 *
 * <p>Explicit CAST allows additional conversions (e.g. VARCHAR to numeric types).
 */
public final class DqeTypeCoercion {

  /**
   * Integer type ordering for implicit widening. TINYINT(0) < SMALLINT(1) < INTEGER(2) < BIGINT(3)
   */
  private static int integerRank(DqeType type) {
    if (type.equals(DqeTypes.TINYINT)) return 0;
    if (type.equals(DqeTypes.SMALLINT)) return 1;
    if (type.equals(DqeTypes.INTEGER)) return 2;
    if (type.equals(DqeTypes.BIGINT)) return 3;
    return -1;
  }

  private static DqeType integerTypeByRank(int rank) {
    return switch (rank) {
      case 0 -> DqeTypes.TINYINT;
      case 1 -> DqeTypes.SMALLINT;
      case 2 -> DqeTypes.INTEGER;
      case 3 -> DqeTypes.BIGINT;
      default -> throw new IllegalStateException("Invalid integer rank: " + rank);
    };
  }

  private static boolean isIntegerType(DqeType type) {
    return integerRank(type) >= 0;
  }

  private static boolean isFloatType(DqeType type) {
    return type.equals(DqeTypes.REAL) || type.equals(DqeTypes.DOUBLE);
  }

  private static boolean isDecimalType(DqeType type) {
    return type.getTrinoType() instanceof DecimalType;
  }

  private static boolean isTimestampType(DqeType type) {
    return type.equals(DqeTypes.TIMESTAMP_MILLIS) || type.equals(DqeTypes.TIMESTAMP_NANOS);
  }

  /**
   * Finds the common supertype for two types (for implicit coercion). Returns empty if no implicit
   * widening is possible.
   *
   * @param left left operand type
   * @param right right operand type
   * @return common supertype, or empty if no implicit widening exists
   */
  public static Optional<DqeType> getCommonSuperType(DqeType left, DqeType right) {
    Objects.requireNonNull(left, "left must not be null");
    Objects.requireNonNull(right, "right must not be null");

    // Same type -> return it
    if (left.equals(right)) {
      return Optional.of(left);
    }

    // Integer widening: TINYINT < SMALLINT < INTEGER < BIGINT
    if (isIntegerType(left) && isIntegerType(right)) {
      int maxRank = Math.max(integerRank(left), integerRank(right));
      return Optional.of(integerTypeByRank(maxRank));
    }

    // Float widening: REAL < DOUBLE
    if (isFloatType(left) && isFloatType(right)) {
      return Optional.of(DqeTypes.DOUBLE);
    }

    // Integer + Float -> DOUBLE
    if (isIntegerType(left) && isFloatType(right)) {
      return Optional.of(DqeTypes.DOUBLE);
    }
    if (isFloatType(left) && isIntegerType(right)) {
      return Optional.of(DqeTypes.DOUBLE);
    }

    // Integer + DECIMAL -> DECIMAL (widen precision)
    if (isIntegerType(left) && isDecimalType(right)) {
      return Optional.of(widenIntegerToDecimal(left, right));
    }
    if (isDecimalType(left) && isIntegerType(right)) {
      return Optional.of(widenIntegerToDecimal(right, left));
    }

    // Float + DECIMAL -> DOUBLE (lossy but conventional)
    if (isFloatType(left) && isDecimalType(right)) {
      return Optional.of(DqeTypes.DOUBLE);
    }
    if (isDecimalType(left) && isFloatType(right)) {
      return Optional.of(DqeTypes.DOUBLE);
    }

    // DECIMAL + DECIMAL -> DECIMAL with widened precision/scale
    if (isDecimalType(left) && isDecimalType(right)) {
      return Optional.of(widenDecimalToDecimal(left, right));
    }

    // Timestamp widening: TIMESTAMP(3) + TIMESTAMP(9) -> TIMESTAMP(9)
    if (isTimestampType(left) && isTimestampType(right)) {
      int maxPrecision = Math.max(left.getPrecision(), right.getPrecision());
      return Optional.of(DqeTypes.timestamp(maxPrecision));
    }

    // No implicit widening for other combinations
    return Optional.empty();
  }

  /**
   * Checks if an implicit coercion from source to target is allowed.
   *
   * @param source source type
   * @param target target type
   * @return true if implicit coercion is allowed
   */
  public static boolean canCoerce(DqeType source, DqeType target) {
    Objects.requireNonNull(source, "source must not be null");
    Objects.requireNonNull(target, "target must not be null");

    if (source.equals(target)) {
      return true;
    }

    // Integer widening
    if (isIntegerType(source) && isIntegerType(target)) {
      return integerRank(source) <= integerRank(target);
    }

    // REAL -> DOUBLE
    if (source.equals(DqeTypes.REAL) && target.equals(DqeTypes.DOUBLE)) {
      return true;
    }

    // Integer -> REAL or DOUBLE
    if (isIntegerType(source) && isFloatType(target)) {
      return true;
    }

    // Integer -> DECIMAL (only if target has sufficient precision for the integer range)
    if (isIntegerType(source) && isDecimalType(target)) {
      int intDigits = integerDigits(source);
      // The target's integer part (precision - scale) must accommodate the source's digit count
      int targetIntegerPart = target.getPrecision() - target.getScale();
      return targetIntegerPart >= intDigits;
    }

    // DECIMAL -> DECIMAL (if target has >= precision and scale)
    if (isDecimalType(source) && isDecimalType(target)) {
      return target.getPrecision() >= source.getPrecision()
          && target.getScale() >= source.getScale();
    }

    // Timestamp precision widening
    if (isTimestampType(source) && isTimestampType(target)) {
      return target.getPrecision() >= source.getPrecision();
    }

    return false;
  }

  /**
   * Checks if an explicit CAST from source to target is allowed. Explicit CAST allows more
   * conversions than implicit coercion.
   *
   * @param source source type
   * @param target target type
   * @return true if explicit CAST is allowed
   */
  public static boolean canCast(DqeType source, DqeType target) {
    Objects.requireNonNull(source, "source must not be null");
    Objects.requireNonNull(target, "target must not be null");

    if (source.equals(target)) {
      return true;
    }

    // Implicit coercion is a subset of explicit CAST
    if (canCoerce(source, target)) {
      return true;
    }

    boolean sourceIsNumeric = source.isNumeric();
    boolean targetIsNumeric = target.isNumeric();

    // Any numeric -> any numeric (may lose precision)
    if (sourceIsNumeric && targetIsNumeric) {
      return true;
    }

    // VARCHAR -> any numeric or boolean (parse at runtime)
    if (source.equals(DqeTypes.VARCHAR) && (targetIsNumeric || target.equals(DqeTypes.BOOLEAN))) {
      return true;
    }

    // Any numeric or boolean -> VARCHAR
    if (target.equals(DqeTypes.VARCHAR) && (sourceIsNumeric || source.equals(DqeTypes.BOOLEAN))) {
      return true;
    }

    // VARCHAR <-> TIMESTAMP
    if (source.equals(DqeTypes.VARCHAR) && isTimestampType(target)) {
      return true;
    }
    if (isTimestampType(source) && target.equals(DqeTypes.VARCHAR)) {
      return true;
    }

    // TIMESTAMP -> TIMESTAMP (precision change)
    if (isTimestampType(source) && isTimestampType(target)) {
      return true;
    }

    // BOOLEAN <-> INTEGER/BIGINT
    if (source.equals(DqeTypes.BOOLEAN)
        && (target.equals(DqeTypes.INTEGER) || target.equals(DqeTypes.BIGINT))) {
      return true;
    }
    if ((source.equals(DqeTypes.INTEGER) || source.equals(DqeTypes.BIGINT))
        && target.equals(DqeTypes.BOOLEAN)) {
      return true;
    }

    // VARCHAR -> VARBINARY and vice versa
    if ((source.equals(DqeTypes.VARCHAR) && target.equals(DqeTypes.VARBINARY))
        || (source.equals(DqeTypes.VARBINARY) && target.equals(DqeTypes.VARCHAR))) {
      return true;
    }

    return false;
  }

  /** Returns the maximum number of decimal digits an integer type can represent. */
  private static int integerDigits(DqeType intType) {
    if (intType.equals(DqeTypes.TINYINT)) return 3; // -128..127
    if (intType.equals(DqeTypes.SMALLINT)) return 5; // -32768..32767
    if (intType.equals(DqeTypes.INTEGER)) return 10; // ~2 billion
    return 19; // BIGINT long range
  }

  private static DqeType widenIntegerToDecimal(DqeType intType, DqeType decType) {
    int intDigits = integerDigits(intType);
    int decPrecision = decType.getPrecision();
    int decScale = decType.getScale();

    // Widen: precision = max(integer_digits + scale, dec_precision)
    int neededPrecision = intDigits + decScale;
    int resultPrecision = Math.min(38, Math.max(neededPrecision, decPrecision));
    return DqeTypes.decimal(resultPrecision, decScale);
  }

  private static DqeType widenDecimalToDecimal(DqeType left, DqeType right) {
    int leftP = left.getPrecision();
    int leftS = left.getScale();
    int rightP = right.getPrecision();
    int rightS = right.getScale();

    // Result scale is max of both scales
    int resultScale = Math.max(leftS, rightS);
    // Result integer part is max of both integer parts
    int leftIntPart = leftP - leftS;
    int rightIntPart = rightP - rightS;
    int resultIntPart = Math.max(leftIntPart, rightIntPart);
    // Total precision, capped at 38
    int resultPrecision = Math.min(38, resultIntPart + resultScale);

    return DqeTypes.decimal(resultPrecision, resultScale);
  }

  private DqeTypeCoercion() {
    // no instantiation
  }
}
