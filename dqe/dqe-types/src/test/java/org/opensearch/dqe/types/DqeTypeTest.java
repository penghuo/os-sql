/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.types;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@DisplayName("DqeType")
class DqeTypeTest {

  @Nested
  @DisplayName("Basic properties")
  class BasicProperties {

    @Test
    @DisplayName("VARCHAR has correct display name and sortability")
    void varcharProperties() {
      assertEquals("VARCHAR", DqeTypes.VARCHAR.getDisplayName());
      assertTrue(DqeTypes.VARCHAR.isSortable());
      assertFalse(DqeTypes.VARCHAR.isNumeric());
      assertTrue(DqeTypes.VARCHAR.isComparable());
      assertFalse(DqeTypes.VARCHAR.isParameterized());
    }

    @Test
    @DisplayName("BIGINT is numeric and sortable")
    void bigintProperties() {
      assertEquals("BIGINT", DqeTypes.BIGINT.getDisplayName());
      assertTrue(DqeTypes.BIGINT.isSortable());
      assertTrue(DqeTypes.BIGINT.isNumeric());
    }

    @Test
    @DisplayName("INTEGER is numeric and sortable")
    void integerProperties() {
      assertEquals("INTEGER", DqeTypes.INTEGER.getDisplayName());
      assertTrue(DqeTypes.INTEGER.isSortable());
      assertTrue(DqeTypes.INTEGER.isNumeric());
    }

    @Test
    @DisplayName("SMALLINT is numeric and sortable")
    void smallintProperties() {
      assertEquals("SMALLINT", DqeTypes.SMALLINT.getDisplayName());
      assertTrue(DqeTypes.SMALLINT.isSortable());
      assertTrue(DqeTypes.SMALLINT.isNumeric());
    }

    @Test
    @DisplayName("TINYINT is numeric and sortable")
    void tinyintProperties() {
      assertEquals("TINYINT", DqeTypes.TINYINT.getDisplayName());
      assertTrue(DqeTypes.TINYINT.isSortable());
      assertTrue(DqeTypes.TINYINT.isNumeric());
    }

    @Test
    @DisplayName("DOUBLE is numeric and sortable")
    void doubleProperties() {
      assertEquals("DOUBLE", DqeTypes.DOUBLE.getDisplayName());
      assertTrue(DqeTypes.DOUBLE.isSortable());
      assertTrue(DqeTypes.DOUBLE.isNumeric());
    }

    @Test
    @DisplayName("REAL is numeric and sortable")
    void realProperties() {
      assertEquals("REAL", DqeTypes.REAL.getDisplayName());
      assertTrue(DqeTypes.REAL.isSortable());
      assertTrue(DqeTypes.REAL.isNumeric());
    }

    @Test
    @DisplayName("BOOLEAN is sortable but not numeric")
    void booleanProperties() {
      assertEquals("BOOLEAN", DqeTypes.BOOLEAN.getDisplayName());
      assertTrue(DqeTypes.BOOLEAN.isSortable());
      assertFalse(DqeTypes.BOOLEAN.isNumeric());
    }

    @Test
    @DisplayName("VARBINARY is not sortable")
    void varbinaryProperties() {
      assertEquals("VARBINARY", DqeTypes.VARBINARY.getDisplayName());
      assertFalse(DqeTypes.VARBINARY.isSortable());
      assertFalse(DqeTypes.VARBINARY.isNumeric());
    }

    @Test
    @DisplayName("TIMESTAMP_MILLIS has precision 3")
    void timestampMillisProperties() {
      assertEquals("TIMESTAMP(3)", DqeTypes.TIMESTAMP_MILLIS.getDisplayName());
      assertTrue(DqeTypes.TIMESTAMP_MILLIS.isSortable());
      assertEquals(3, DqeTypes.TIMESTAMP_MILLIS.getPrecision());
      assertTrue(DqeTypes.TIMESTAMP_MILLIS.isParameterized());
    }

    @Test
    @DisplayName("TIMESTAMP_NANOS has precision 9")
    void timestampNanosProperties() {
      assertEquals("TIMESTAMP(9)", DqeTypes.TIMESTAMP_NANOS.getDisplayName());
      assertTrue(DqeTypes.TIMESTAMP_NANOS.isSortable());
      assertEquals(9, DqeTypes.TIMESTAMP_NANOS.getPrecision());
    }
  }

  @Nested
  @DisplayName("Parameterized types")
  class ParameterizedTypes {

    @Test
    @DisplayName("DECIMAL has correct precision and scale")
    void decimalType() {
      DqeType dec = DqeTypes.decimal(10, 2);
      assertEquals("DECIMAL(10,2)", dec.getDisplayName());
      assertTrue(dec.isSortable());
      assertTrue(dec.isNumeric());
      assertTrue(dec.isParameterized());
      assertEquals(10, dec.getPrecision());
      assertEquals(2, dec.getScale());
    }

    @Test
    @DisplayName("ARRAY type wraps element type")
    void arrayType() {
      DqeType arr = DqeTypes.array(DqeTypes.VARCHAR);
      assertEquals("ARRAY(VARCHAR)", arr.getDisplayName());
      assertFalse(arr.isSortable());
      assertTrue(arr.isArrayType());
      assertFalse(arr.isRowType());
      assertTrue(arr.isParameterized());
      assertEquals(DqeTypes.VARCHAR, arr.getElementType());
    }

    @Test
    @DisplayName("Nested ARRAY types")
    void nestedArrayType() {
      DqeType nested = DqeTypes.array(DqeTypes.array(DqeTypes.INTEGER));
      assertTrue(nested.isArrayType());
      DqeType inner = nested.getElementType();
      assertTrue(inner.isArrayType());
      assertEquals(DqeTypes.INTEGER, inner.getElementType());
    }

    @Test
    @DisplayName("ROW type with named fields")
    void rowType() {
      List<DqeType.RowField> fields =
          List.of(
              new DqeType.RowField("lat", DqeTypes.DOUBLE),
              new DqeType.RowField("lon", DqeTypes.DOUBLE));
      DqeType row = DqeTypes.row(fields);
      assertTrue(row.isRowType());
      assertFalse(row.isSortable());
      assertTrue(row.isParameterized());
      List<DqeType.RowField> resolved = row.getRowFields();
      assertEquals(2, resolved.size());
      assertEquals("lat", resolved.get(0).getName());
      assertEquals(DqeTypes.DOUBLE, resolved.get(0).getType());
    }

    @Test
    @DisplayName("MAP type wraps key and value types")
    void mapType() {
      DqeType m = DqeTypes.map(DqeTypes.VARCHAR, DqeTypes.VARCHAR);
      assertEquals("MAP(VARCHAR, VARCHAR)", m.getDisplayName());
      assertFalse(m.isSortable());
      assertTrue(m.isMapType());
      assertTrue(m.isParameterized());
    }

    @Test
    @DisplayName("timestamp factory returns canonical instances for 3 and 9")
    void timestampFactory() {
      assertSame(DqeTypes.TIMESTAMP_MILLIS, DqeTypes.timestamp(3));
      assertSame(DqeTypes.TIMESTAMP_NANOS, DqeTypes.timestamp(9));
      DqeType ts6 = DqeTypes.timestamp(6);
      assertEquals("TIMESTAMP(6)", ts6.getDisplayName());
      assertEquals(6, ts6.getPrecision());
    }
  }

  @Nested
  @DisplayName("Type accessors throw on wrong type kind")
  class AccessorGuards {

    @Test
    @DisplayName("getElementType throws on non-ARRAY")
    void getElementTypeThrowsOnNonArray() {
      assertThrows(IllegalStateException.class, () -> DqeTypes.VARCHAR.getElementType());
    }

    @Test
    @DisplayName("getRowFields throws on non-ROW")
    void getRowFieldsThrowsOnNonRow() {
      assertThrows(IllegalStateException.class, () -> DqeTypes.BIGINT.getRowFields());
    }

    @Test
    @DisplayName("getPrecision throws on non-DECIMAL/TIMESTAMP")
    void getPrecisionThrowsOnVarchar() {
      assertThrows(IllegalStateException.class, () -> DqeTypes.VARCHAR.getPrecision());
    }

    @Test
    @DisplayName("getScale throws on non-DECIMAL")
    void getScaleThrowsOnBigint() {
      assertThrows(IllegalStateException.class, () -> DqeTypes.BIGINT.getScale());
    }
  }

  @Nested
  @DisplayName("fromTrinoType round-trip")
  class FromTrinoType {

    @Test
    @DisplayName("fromTrinoType returns canonical VARCHAR")
    void fromTrinoVarchar() {
      DqeType result = DqeTypes.fromTrinoType(DqeTypes.VARCHAR.getTrinoType());
      assertSame(DqeTypes.VARCHAR, result);
    }

    @Test
    @DisplayName("fromTrinoType returns canonical BIGINT")
    void fromTrinoBigint() {
      assertSame(DqeTypes.BIGINT, DqeTypes.fromTrinoType(DqeTypes.BIGINT.getTrinoType()));
    }

    @Test
    @DisplayName("fromTrinoType returns canonical BOOLEAN")
    void fromTrinoBoolean() {
      assertSame(DqeTypes.BOOLEAN, DqeTypes.fromTrinoType(DqeTypes.BOOLEAN.getTrinoType()));
    }

    @Test
    @DisplayName("fromTrinoType round-trips DECIMAL")
    void fromTrinoDecimal() {
      DqeType original = DqeTypes.decimal(20, 5);
      DqeType roundTripped = DqeTypes.fromTrinoType(original.getTrinoType());
      assertEquals(original, roundTripped);
    }

    @Test
    @DisplayName("fromTrinoType round-trips ARRAY")
    void fromTrinoArray() {
      DqeType original = DqeTypes.array(DqeTypes.REAL);
      DqeType roundTripped = DqeTypes.fromTrinoType(original.getTrinoType());
      assertEquals(original, roundTripped);
    }

    @Test
    @DisplayName("fromTrinoType round-trips ROW")
    void fromTrinoRow() {
      DqeType original =
          DqeTypes.row(
              List.of(
                  new DqeType.RowField("a", DqeTypes.INTEGER),
                  new DqeType.RowField("b", DqeTypes.VARCHAR)));
      DqeType roundTripped = DqeTypes.fromTrinoType(original.getTrinoType());
      assertEquals(original.getDisplayName(), roundTripped.getDisplayName());
      assertTrue(roundTripped.isRowType());
    }

    @Test
    @DisplayName("fromTrinoType round-trips MAP")
    void fromTrinoMap() {
      DqeType original = DqeTypes.map(DqeTypes.VARCHAR, DqeTypes.BIGINT);
      DqeType roundTripped = DqeTypes.fromTrinoType(original.getTrinoType());
      assertTrue(roundTripped.isMapType());
    }

    @Test
    @DisplayName("fromTrinoType returns canonical TIMESTAMP_MILLIS")
    void fromTrinoTimestampMillis() {
      DqeType result = DqeTypes.fromTrinoType(DqeTypes.TIMESTAMP_MILLIS.getTrinoType());
      assertSame(DqeTypes.TIMESTAMP_MILLIS, result);
    }
  }

  @Nested
  @DisplayName("Equality and hashCode")
  class EqualityTests {

    @Test
    @DisplayName("Same type constants are equal")
    void sameConstantsEqual() {
      assertEquals(DqeTypes.VARCHAR, DqeTypes.VARCHAR);
      assertEquals(DqeTypes.BIGINT, DqeTypes.BIGINT);
    }

    @Test
    @DisplayName("Different types are not equal")
    void differentTypesNotEqual() {
      assertNotEquals(DqeTypes.VARCHAR, DqeTypes.BIGINT);
      assertNotEquals(DqeTypes.INTEGER, DqeTypes.BIGINT);
    }

    @Test
    @DisplayName("DECIMAL with same precision/scale are equal")
    void sameDecimalsEqual() {
      assertEquals(DqeTypes.decimal(10, 2), DqeTypes.decimal(10, 2));
    }

    @Test
    @DisplayName("DECIMAL with different precision are not equal")
    void differentDecimalsNotEqual() {
      assertNotEquals(DqeTypes.decimal(10, 2), DqeTypes.decimal(12, 2));
    }

    @Test
    @DisplayName("hashCode consistent with equals")
    void hashCodeConsistent() {
      assertEquals(DqeTypes.VARCHAR.hashCode(), DqeTypes.VARCHAR.hashCode());
      assertEquals(DqeTypes.decimal(10, 2).hashCode(), DqeTypes.decimal(10, 2).hashCode());
    }

    @Test
    @DisplayName("Equality ignores sortability — type identity based on Trino type only")
    void equalityIgnoresSortable() {
      // Create two VARCHARs with different sortable flags
      DqeType sortableVarchar = new DqeType(io.trino.spi.type.VarcharType.VARCHAR, "VARCHAR", true);
      DqeType unsortableVarchar =
          new DqeType(io.trino.spi.type.VarcharType.VARCHAR, "VARCHAR", false);
      assertEquals(sortableVarchar, unsortableVarchar, "Types with same Trino type must be equal");
      assertEquals(
          sortableVarchar.hashCode(),
          unsortableVarchar.hashCode(),
          "hashCode must be consistent with equals");
    }
  }

  @Nested
  @DisplayName("RowField")
  class RowFieldTests {

    @Test
    @DisplayName("RowField getters")
    void rowFieldGetters() {
      DqeType.RowField field = new DqeType.RowField("name", DqeTypes.VARCHAR);
      assertEquals("name", field.getName());
      assertEquals(DqeTypes.VARCHAR, field.getType());
    }

    @Test
    @DisplayName("RowField equality")
    void rowFieldEquality() {
      DqeType.RowField a = new DqeType.RowField("x", DqeTypes.BIGINT);
      DqeType.RowField b = new DqeType.RowField("x", DqeTypes.BIGINT);
      assertEquals(a, b);
      assertEquals(a.hashCode(), b.hashCode());
    }

    @Test
    @DisplayName("RowField toString")
    void rowFieldToString() {
      DqeType.RowField field = new DqeType.RowField("age", DqeTypes.INTEGER);
      assertEquals("age INTEGER", field.toString());
    }
  }

  @Test
  @DisplayName("toString returns display name")
  void toStringReturnsDisplayName() {
    assertEquals("VARCHAR", DqeTypes.VARCHAR.toString());
    assertEquals("DECIMAL(10,2)", DqeTypes.decimal(10, 2).toString());
  }

  @Test
  @DisplayName("ROW type requires at least one field")
  void rowRequiresField() {
    assertThrows(IllegalArgumentException.class, () -> DqeTypes.row(List.of()));
  }

  @Test
  @DisplayName("null arguments rejected")
  void nullRejected() {
    assertThrows(NullPointerException.class, () -> DqeTypes.array(null));
    assertThrows(NullPointerException.class, () -> DqeTypes.map(null, DqeTypes.VARCHAR));
    assertThrows(NullPointerException.class, () -> DqeTypes.map(DqeTypes.VARCHAR, null));
    assertThrows(NullPointerException.class, () -> DqeTypes.fromTrinoType(null));
  }
}
