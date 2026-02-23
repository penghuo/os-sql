/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.types;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Optional;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@DisplayName("DqeTypeCoercion")
class DqeTypeCoercionTest {

  @Nested
  @DisplayName("getCommonSuperType")
  class CommonSuperType {

    @Test
    @DisplayName("Same type returns itself")
    void sameType() {
      assertEquals(
          Optional.of(DqeTypes.VARCHAR),
          DqeTypeCoercion.getCommonSuperType(DqeTypes.VARCHAR, DqeTypes.VARCHAR));
      assertEquals(
          Optional.of(DqeTypes.BIGINT),
          DqeTypeCoercion.getCommonSuperType(DqeTypes.BIGINT, DqeTypes.BIGINT));
    }

    @Test
    @DisplayName("TINYINT + SMALLINT -> SMALLINT")
    void tinyintSmallint() {
      assertEquals(
          Optional.of(DqeTypes.SMALLINT),
          DqeTypeCoercion.getCommonSuperType(DqeTypes.TINYINT, DqeTypes.SMALLINT));
    }

    @Test
    @DisplayName("TINYINT + BIGINT -> BIGINT")
    void tinyintBigint() {
      assertEquals(
          Optional.of(DqeTypes.BIGINT),
          DqeTypeCoercion.getCommonSuperType(DqeTypes.TINYINT, DqeTypes.BIGINT));
    }

    @Test
    @DisplayName("INTEGER + BIGINT -> BIGINT")
    void integerBigint() {
      assertEquals(
          Optional.of(DqeTypes.BIGINT),
          DqeTypeCoercion.getCommonSuperType(DqeTypes.INTEGER, DqeTypes.BIGINT));
    }

    @Test
    @DisplayName("SMALLINT + INTEGER -> INTEGER")
    void smallintInteger() {
      assertEquals(
          Optional.of(DqeTypes.INTEGER),
          DqeTypeCoercion.getCommonSuperType(DqeTypes.SMALLINT, DqeTypes.INTEGER));
    }

    @Test
    @DisplayName("REAL + DOUBLE -> DOUBLE")
    void realDouble() {
      assertEquals(
          Optional.of(DqeTypes.DOUBLE),
          DqeTypeCoercion.getCommonSuperType(DqeTypes.REAL, DqeTypes.DOUBLE));
    }

    @Test
    @DisplayName("INTEGER + DOUBLE -> DOUBLE")
    void integerDouble() {
      assertEquals(
          Optional.of(DqeTypes.DOUBLE),
          DqeTypeCoercion.getCommonSuperType(DqeTypes.INTEGER, DqeTypes.DOUBLE));
    }

    @Test
    @DisplayName("BIGINT + REAL -> DOUBLE")
    void bigintReal() {
      assertEquals(
          Optional.of(DqeTypes.DOUBLE),
          DqeTypeCoercion.getCommonSuperType(DqeTypes.BIGINT, DqeTypes.REAL));
    }

    @Test
    @DisplayName("REAL + INTEGER -> DOUBLE")
    void realInteger() {
      assertEquals(
          Optional.of(DqeTypes.DOUBLE),
          DqeTypeCoercion.getCommonSuperType(DqeTypes.REAL, DqeTypes.INTEGER));
    }

    @Test
    @DisplayName("INTEGER + DECIMAL(10,2) -> DECIMAL(12,2)")
    void integerDecimal() {
      DqeType dec = DqeTypes.decimal(10, 2);
      Optional<DqeType> result = DqeTypeCoercion.getCommonSuperType(DqeTypes.INTEGER, dec);
      assertTrue(result.isPresent());
      // INTEGER needs 10 digits, plus 2 scale = 12 precision; max(12, 10) = 12
      assertEquals(12, result.get().getPrecision());
      assertEquals(2, result.get().getScale());
    }

    @Test
    @DisplayName("TINYINT + DECIMAL(5,3) -> DECIMAL(6,3)")
    void tinyintDecimal() {
      DqeType dec = DqeTypes.decimal(5, 3);
      Optional<DqeType> result = DqeTypeCoercion.getCommonSuperType(DqeTypes.TINYINT, dec);
      assertTrue(result.isPresent());
      // TINYINT needs 3 digits + 3 scale = 6; max(6, 5) = 6
      assertEquals(6, result.get().getPrecision());
      assertEquals(3, result.get().getScale());
    }

    @Test
    @DisplayName("BIGINT + DECIMAL(10,2) -> DECIMAL(21,2)")
    void bigintDecimal() {
      DqeType dec = DqeTypes.decimal(10, 2);
      Optional<DqeType> result = DqeTypeCoercion.getCommonSuperType(DqeTypes.BIGINT, dec);
      assertTrue(result.isPresent());
      // BIGINT needs 19 digits + 2 scale = 21; max(21, 10) = 21
      assertEquals(21, result.get().getPrecision());
      assertEquals(2, result.get().getScale());
    }

    @Test
    @DisplayName("DECIMAL + DECIMAL -> widened DECIMAL")
    void decimalDecimal() {
      DqeType dec1 = DqeTypes.decimal(10, 2);
      DqeType dec2 = DqeTypes.decimal(15, 5);
      Optional<DqeType> result = DqeTypeCoercion.getCommonSuperType(dec1, dec2);
      assertTrue(result.isPresent());
      DqeType widened = result.get();
      // Scale should be max(2, 5) = 5
      assertEquals(5, widened.getScale());
      // Integer part should be max(10-2, 15-5) = max(8, 10) = 10
      // Total precision = 10 + 5 = 15
      assertEquals(15, widened.getPrecision());
    }

    @Test
    @DisplayName("DOUBLE + DECIMAL -> DOUBLE")
    void doubleDecimal() {
      assertEquals(
          Optional.of(DqeTypes.DOUBLE),
          DqeTypeCoercion.getCommonSuperType(DqeTypes.DOUBLE, DqeTypes.decimal(10, 2)));
    }

    @Test
    @DisplayName("TIMESTAMP(3) + TIMESTAMP(9) -> TIMESTAMP(9)")
    void timestampWidening() {
      assertEquals(
          Optional.of(DqeTypes.TIMESTAMP_NANOS),
          DqeTypeCoercion.getCommonSuperType(DqeTypes.TIMESTAMP_MILLIS, DqeTypes.TIMESTAMP_NANOS));
    }

    @Test
    @DisplayName("VARCHAR + BIGINT -> empty (no implicit widening)")
    void varcharBigint() {
      assertTrue(DqeTypeCoercion.getCommonSuperType(DqeTypes.VARCHAR, DqeTypes.BIGINT).isEmpty());
    }

    @Test
    @DisplayName("BOOLEAN + INTEGER -> empty")
    void booleanInteger() {
      assertTrue(DqeTypeCoercion.getCommonSuperType(DqeTypes.BOOLEAN, DqeTypes.INTEGER).isEmpty());
    }

    @Test
    @DisplayName("ARRAY + VARCHAR -> empty")
    void arrayVarchar() {
      DqeType arr = DqeTypes.array(DqeTypes.INTEGER);
      assertTrue(DqeTypeCoercion.getCommonSuperType(arr, DqeTypes.VARCHAR).isEmpty());
    }
  }

  @Nested
  @DisplayName("canCoerce")
  class CanCoerce {

    @Test
    @DisplayName("Same type always coercible")
    void sameType() {
      assertTrue(DqeTypeCoercion.canCoerce(DqeTypes.VARCHAR, DqeTypes.VARCHAR));
      assertTrue(DqeTypeCoercion.canCoerce(DqeTypes.BIGINT, DqeTypes.BIGINT));
    }

    @Test
    @DisplayName("TINYINT -> BIGINT is coercible")
    void tinyintToBigint() {
      assertTrue(DqeTypeCoercion.canCoerce(DqeTypes.TINYINT, DqeTypes.BIGINT));
    }

    @Test
    @DisplayName("BIGINT -> TINYINT is NOT coercible")
    void bigintToTinyint() {
      assertFalse(DqeTypeCoercion.canCoerce(DqeTypes.BIGINT, DqeTypes.TINYINT));
    }

    @Test
    @DisplayName("REAL -> DOUBLE is coercible")
    void realToDouble() {
      assertTrue(DqeTypeCoercion.canCoerce(DqeTypes.REAL, DqeTypes.DOUBLE));
    }

    @Test
    @DisplayName("DOUBLE -> REAL is NOT coercible")
    void doubleToReal() {
      assertFalse(DqeTypeCoercion.canCoerce(DqeTypes.DOUBLE, DqeTypes.REAL));
    }

    @Test
    @DisplayName("INTEGER -> DOUBLE is coercible")
    void integerToDouble() {
      assertTrue(DqeTypeCoercion.canCoerce(DqeTypes.INTEGER, DqeTypes.DOUBLE));
    }

    @Test
    @DisplayName("INTEGER -> DECIMAL(20,2) is coercible (sufficient precision)")
    void integerToDecimalSufficient() {
      // DECIMAL(20,2) has 18 integer digits, INTEGER needs 10 -> fits
      assertTrue(DqeTypeCoercion.canCoerce(DqeTypes.INTEGER, DqeTypes.decimal(20, 2)));
    }

    @Test
    @DisplayName("TINYINT -> DECIMAL(5,2) is coercible (sufficient precision)")
    void tinyintToDecimalSufficient() {
      // DECIMAL(5,2) has 3 integer digits, TINYINT needs 3 -> fits exactly
      assertTrue(DqeTypeCoercion.canCoerce(DqeTypes.TINYINT, DqeTypes.decimal(5, 2)));
    }

    @Test
    @DisplayName("BIGINT -> DECIMAL(3,2) is NOT coercible (insufficient precision)")
    void bigintToSmallDecimal() {
      // DECIMAL(3,2) has 1 integer digit, BIGINT needs 19 -> does not fit
      assertFalse(DqeTypeCoercion.canCoerce(DqeTypes.BIGINT, DqeTypes.decimal(3, 2)));
    }

    @Test
    @DisplayName("INTEGER -> DECIMAL(10,2) is NOT coercible (insufficient precision)")
    void integerToDecimalInsufficient() {
      // DECIMAL(10,2) has 8 integer digits, INTEGER needs 10 -> does not fit
      assertFalse(DqeTypeCoercion.canCoerce(DqeTypes.INTEGER, DqeTypes.decimal(10, 2)));
    }

    @Test
    @DisplayName("SMALLINT -> DECIMAL(4,2) is NOT coercible (insufficient precision)")
    void smallintToDecimalInsufficient() {
      // DECIMAL(4,2) has 2 integer digits, SMALLINT needs 5 -> does not fit
      assertFalse(DqeTypeCoercion.canCoerce(DqeTypes.SMALLINT, DqeTypes.decimal(4, 2)));
    }

    @Test
    @DisplayName("TIMESTAMP(3) -> TIMESTAMP(9) is coercible")
    void timestampMillisToNanos() {
      assertTrue(DqeTypeCoercion.canCoerce(DqeTypes.TIMESTAMP_MILLIS, DqeTypes.TIMESTAMP_NANOS));
    }

    @Test
    @DisplayName("TIMESTAMP(9) -> TIMESTAMP(3) is NOT coercible")
    void timestampNanosToMillis() {
      assertFalse(DqeTypeCoercion.canCoerce(DqeTypes.TIMESTAMP_NANOS, DqeTypes.TIMESTAMP_MILLIS));
    }

    @Test
    @DisplayName("VARCHAR -> BIGINT is NOT coercible")
    void varcharToBigint() {
      assertFalse(DqeTypeCoercion.canCoerce(DqeTypes.VARCHAR, DqeTypes.BIGINT));
    }
  }

  @Nested
  @DisplayName("canCast")
  class CanCast {

    @Test
    @DisplayName("Same type always castable")
    void sameType() {
      assertTrue(DqeTypeCoercion.canCast(DqeTypes.VARCHAR, DqeTypes.VARCHAR));
    }

    @Test
    @DisplayName("Implicit coercion implies castability")
    void implicitCoercionImpliesCast() {
      assertTrue(DqeTypeCoercion.canCast(DqeTypes.TINYINT, DqeTypes.BIGINT));
      assertTrue(DqeTypeCoercion.canCast(DqeTypes.REAL, DqeTypes.DOUBLE));
    }

    @Test
    @DisplayName("VARCHAR -> BIGINT is castable (explicit)")
    void varcharToBigint() {
      assertTrue(DqeTypeCoercion.canCast(DqeTypes.VARCHAR, DqeTypes.BIGINT));
    }

    @Test
    @DisplayName("VARCHAR -> DOUBLE is castable")
    void varcharToDouble() {
      assertTrue(DqeTypeCoercion.canCast(DqeTypes.VARCHAR, DqeTypes.DOUBLE));
    }

    @Test
    @DisplayName("VARCHAR -> BOOLEAN is castable")
    void varcharToBoolean() {
      assertTrue(DqeTypeCoercion.canCast(DqeTypes.VARCHAR, DqeTypes.BOOLEAN));
    }

    @Test
    @DisplayName("BIGINT -> VARCHAR is castable")
    void bigintToVarchar() {
      assertTrue(DqeTypeCoercion.canCast(DqeTypes.BIGINT, DqeTypes.VARCHAR));
    }

    @Test
    @DisplayName("DOUBLE -> INTEGER is castable (lossy)")
    void doubleToInteger() {
      assertTrue(DqeTypeCoercion.canCast(DqeTypes.DOUBLE, DqeTypes.INTEGER));
    }

    @Test
    @DisplayName("BIGINT -> TINYINT is castable (lossy)")
    void bigintToTinyint() {
      assertTrue(DqeTypeCoercion.canCast(DqeTypes.BIGINT, DqeTypes.TINYINT));
    }

    @Test
    @DisplayName("VARCHAR -> TIMESTAMP is castable")
    void varcharToTimestamp() {
      assertTrue(DqeTypeCoercion.canCast(DqeTypes.VARCHAR, DqeTypes.TIMESTAMP_MILLIS));
    }

    @Test
    @DisplayName("TIMESTAMP -> VARCHAR is castable")
    void timestampToVarchar() {
      assertTrue(DqeTypeCoercion.canCast(DqeTypes.TIMESTAMP_MILLIS, DqeTypes.VARCHAR));
    }

    @Test
    @DisplayName("TIMESTAMP(9) -> TIMESTAMP(3) is castable (explicit)")
    void timestampNanosToMillis() {
      assertTrue(DqeTypeCoercion.canCast(DqeTypes.TIMESTAMP_NANOS, DqeTypes.TIMESTAMP_MILLIS));
    }

    @Test
    @DisplayName("BOOLEAN -> INTEGER is castable")
    void booleanToInteger() {
      assertTrue(DqeTypeCoercion.canCast(DqeTypes.BOOLEAN, DqeTypes.INTEGER));
    }

    @Test
    @DisplayName("VARCHAR -> VARBINARY is castable")
    void varcharToVarbinary() {
      assertTrue(DqeTypeCoercion.canCast(DqeTypes.VARCHAR, DqeTypes.VARBINARY));
    }

    @Test
    @DisplayName("ARRAY -> VARCHAR is NOT castable")
    void arrayToVarchar() {
      DqeType arr = DqeTypes.array(DqeTypes.INTEGER);
      assertFalse(DqeTypeCoercion.canCast(arr, DqeTypes.VARCHAR));
    }

    @Test
    @DisplayName("MAP -> BIGINT is NOT castable")
    void mapToBigint() {
      DqeType m = DqeTypes.map(DqeTypes.VARCHAR, DqeTypes.VARCHAR);
      assertFalse(DqeTypeCoercion.canCast(m, DqeTypes.BIGINT));
    }
  }
}
