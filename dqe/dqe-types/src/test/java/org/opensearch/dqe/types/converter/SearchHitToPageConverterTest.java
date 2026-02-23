/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.types.converter;

import static org.junit.jupiter.api.Assertions.*;

import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.SqlRow;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.VarcharType;
import java.math.BigDecimal;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.opensearch.dqe.types.DqeType;
import org.opensearch.dqe.types.DqeTypes;
import org.opensearch.dqe.types.mapping.DateFormatResolver;

@DisplayName("SearchHitToPageConverter (T-7)")
class SearchHitToPageConverterTest {

  @Nested
  @DisplayName("Basic type conversions")
  class BasicTypes {

    @Test
    @DisplayName("Converts VARCHAR values")
    void varcharConversion() {
      List<ColumnDescriptor> cols = List.of(new ColumnDescriptor("name", DqeTypes.VARCHAR));
      SearchHitToPageConverter converter = new SearchHitToPageConverter(cols);

      Page page = converter.convert(List.of(Map.of("name", "Alice"), Map.of("name", "Bob")));

      assertEquals(2, page.getPositionCount());
      assertEquals(1, page.getChannelCount());
      Block block = page.getBlock(0);
      assertEquals("Alice", VarcharType.VARCHAR.getSlice(block, 0).toStringUtf8());
      assertEquals("Bob", VarcharType.VARCHAR.getSlice(block, 1).toStringUtf8());
    }

    @Test
    @DisplayName("Converts BIGINT values")
    void bigintConversion() {
      List<ColumnDescriptor> cols = List.of(new ColumnDescriptor("count", DqeTypes.BIGINT));
      SearchHitToPageConverter converter = new SearchHitToPageConverter(cols);

      Page page = converter.convert(List.of(Map.of("count", 42L), Map.of("count", Long.MAX_VALUE)));

      assertEquals(2, page.getPositionCount());
      assertEquals(42L, BigintType.BIGINT.getLong(page.getBlock(0), 0));
      assertEquals(Long.MAX_VALUE, BigintType.BIGINT.getLong(page.getBlock(0), 1));
    }

    @Test
    @DisplayName("Converts INTEGER values")
    void integerConversion() {
      List<ColumnDescriptor> cols = List.of(new ColumnDescriptor("age", DqeTypes.INTEGER));
      SearchHitToPageConverter converter = new SearchHitToPageConverter(cols);

      Page page = converter.convert(List.of(Map.of("age", 25)));

      assertEquals(25, IntegerType.INTEGER.getLong(page.getBlock(0), 0));
    }

    @Test
    @DisplayName("Converts SMALLINT values")
    void smallintConversion() {
      List<ColumnDescriptor> cols = List.of(new ColumnDescriptor("val", DqeTypes.SMALLINT));
      SearchHitToPageConverter converter = new SearchHitToPageConverter(cols);

      Page page = converter.convert(List.of(Map.of("val", (short) 100)));

      assertEquals(100L, SmallintType.SMALLINT.getLong(page.getBlock(0), 0));
    }

    @Test
    @DisplayName("Converts TINYINT values")
    void tinyintConversion() {
      List<ColumnDescriptor> cols = List.of(new ColumnDescriptor("flag", DqeTypes.TINYINT));
      SearchHitToPageConverter converter = new SearchHitToPageConverter(cols);

      Page page = converter.convert(List.of(Map.of("flag", (byte) 1)));

      assertEquals(1L, TinyintType.TINYINT.getLong(page.getBlock(0), 0));
    }

    @Test
    @DisplayName("Converts DOUBLE values")
    void doubleConversion() {
      List<ColumnDescriptor> cols = List.of(new ColumnDescriptor("score", DqeTypes.DOUBLE));
      SearchHitToPageConverter converter = new SearchHitToPageConverter(cols);

      Page page = converter.convert(List.of(Map.of("score", 3.14)));

      assertEquals(3.14, DoubleType.DOUBLE.getDouble(page.getBlock(0), 0), 0.001);
    }

    @Test
    @DisplayName("Converts REAL values")
    void realConversion() {
      List<ColumnDescriptor> cols = List.of(new ColumnDescriptor("temp", DqeTypes.REAL));
      SearchHitToPageConverter converter = new SearchHitToPageConverter(cols);

      Page page = converter.convert(List.of(Map.of("temp", 2.5f)));

      float actual = Float.intBitsToFloat((int) RealType.REAL.getLong(page.getBlock(0), 0));
      assertEquals(2.5f, actual, 0.001f);
    }

    @Test
    @DisplayName("Converts BOOLEAN values")
    void booleanConversion() {
      List<ColumnDescriptor> cols = List.of(new ColumnDescriptor("active", DqeTypes.BOOLEAN));
      SearchHitToPageConverter converter = new SearchHitToPageConverter(cols);

      Page page = converter.convert(List.of(Map.of("active", true), Map.of("active", false)));

      assertTrue(BooleanType.BOOLEAN.getBoolean(page.getBlock(0), 0));
      assertFalse(BooleanType.BOOLEAN.getBoolean(page.getBlock(0), 1));
    }

    @Test
    @DisplayName("Converts TIMESTAMP(3) from epoch millis")
    void timestampFromEpochMillis() {
      List<ColumnDescriptor> cols = List.of(new ColumnDescriptor("ts", DqeTypes.TIMESTAMP_MILLIS));
      SearchHitToPageConverter converter = new SearchHitToPageConverter(cols);

      // 1620000000000L millis = some date in 2021
      Page page = converter.convert(List.of(Map.of("ts", 1620000000000L)));

      long micros = TimestampType.createTimestampType(3).getLong(page.getBlock(0), 0);
      // 1620000000000 millis = 1620000000000000 micros
      assertEquals(1620000000000000L, micros);
    }

    @Test
    @DisplayName("Converts TIMESTAMP from ISO string")
    void timestampFromIsoString() {
      List<ColumnDescriptor> cols = List.of(new ColumnDescriptor("ts", DqeTypes.TIMESTAMP_MILLIS));
      SearchHitToPageConverter converter = new SearchHitToPageConverter(cols);

      Page page = converter.convert(List.of(Map.of("ts", "2021-05-03T00:00:00Z")));

      long micros = TimestampType.createTimestampType(3).getLong(page.getBlock(0), 0);
      assertEquals(1620000000L * 1_000_000L, micros);
    }
  }

  @Nested
  @DisplayName("NULL handling")
  class NullHandling {

    @Test
    @DisplayName("Missing field produces NULL in block")
    void missingFieldIsNull() {
      List<ColumnDescriptor> cols = List.of(new ColumnDescriptor("name", DqeTypes.VARCHAR));
      SearchHitToPageConverter converter = new SearchHitToPageConverter(cols);

      Page page = converter.convert(List.of(Map.of("other", "value")));

      assertTrue(page.getBlock(0).isNull(0));
    }

    @Test
    @DisplayName("Explicit null value produces NULL in block")
    void explicitNullIsNull() {
      List<ColumnDescriptor> cols = List.of(new ColumnDescriptor("name", DqeTypes.VARCHAR));
      SearchHitToPageConverter converter = new SearchHitToPageConverter(cols);

      Map<String, Object> doc = new HashMap<>();
      doc.put("name", null);
      Page page = converter.convert(List.of(doc));

      assertTrue(page.getBlock(0).isNull(0));
    }

    @Test
    @DisplayName("Mix of NULL and non-NULL values")
    void mixedNulls() {
      List<ColumnDescriptor> cols = List.of(new ColumnDescriptor("age", DqeTypes.INTEGER));
      SearchHitToPageConverter converter = new SearchHitToPageConverter(cols);

      Map<String, Object> doc1 = Map.of("age", 25);
      Map<String, Object> doc2 = Map.of("other", "x");
      Map<String, Object> doc3 = Map.of("age", 30);

      Page page = converter.convert(List.of(doc1, doc2, doc3));

      Block block = page.getBlock(0);
      assertFalse(block.isNull(0));
      assertEquals(25, IntegerType.INTEGER.getLong(block, 0));
      assertTrue(block.isNull(1));
      assertFalse(block.isNull(2));
      assertEquals(30, IntegerType.INTEGER.getLong(block, 2));
    }
  }

  @Nested
  @DisplayName("Nested field extraction")
  class NestedFields {

    @Test
    @DisplayName("Extracts nested field values with dot notation")
    void nestedFieldExtraction() {
      List<ColumnDescriptor> cols = List.of(new ColumnDescriptor("address.city", DqeTypes.VARCHAR));
      SearchHitToPageConverter converter = new SearchHitToPageConverter(cols);

      Map<String, Object> doc = Map.of("address", Map.of("city", "NYC"));
      Page page = converter.convert(List.of(doc));

      assertEquals("NYC", VarcharType.VARCHAR.getSlice(page.getBlock(0), 0).toStringUtf8());
    }

    @Test
    @DisplayName("Missing nested path produces NULL")
    void missingNestedPath() {
      List<ColumnDescriptor> cols = List.of(new ColumnDescriptor("address.zip", DqeTypes.VARCHAR));
      SearchHitToPageConverter converter = new SearchHitToPageConverter(cols);

      Map<String, Object> doc = Map.of("address", Map.of("city", "NYC"));
      Page page = converter.convert(List.of(doc));

      assertTrue(page.getBlock(0).isNull(0));
    }
  }

  @Nested
  @DisplayName("Multiple columns")
  class MultipleColumns {

    @Test
    @DisplayName("Converts multiple columns in a single page")
    void multipleColumns() {
      List<ColumnDescriptor> cols =
          List.of(
              new ColumnDescriptor("name", DqeTypes.VARCHAR),
              new ColumnDescriptor("age", DqeTypes.INTEGER),
              new ColumnDescriptor("active", DqeTypes.BOOLEAN));
      SearchHitToPageConverter converter = new SearchHitToPageConverter(cols);

      Map<String, Object> doc = Map.of("name", "Alice", "age", 30, "active", true);
      Page page = converter.convert(List.of(doc));

      assertEquals(1, page.getPositionCount());
      assertEquals(3, page.getChannelCount());
      assertEquals("Alice", VarcharType.VARCHAR.getSlice(page.getBlock(0), 0).toStringUtf8());
      assertEquals(30, IntegerType.INTEGER.getLong(page.getBlock(1), 0));
      assertTrue(BooleanType.BOOLEAN.getBoolean(page.getBlock(2), 0));
    }

    @Test
    @DisplayName("Empty hit list produces page with 0 rows")
    void emptyHits() {
      List<ColumnDescriptor> cols = List.of(new ColumnDescriptor("name", DqeTypes.VARCHAR));
      SearchHitToPageConverter converter = new SearchHitToPageConverter(cols);

      Page page = converter.convert(List.of());

      assertEquals(0, page.getPositionCount());
    }
  }

  @Nested
  @DisplayName("String-to-type coercion")
  class StringCoercion {

    @Test
    @DisplayName("String value coerced to BIGINT")
    void stringToBigint() {
      List<ColumnDescriptor> cols = List.of(new ColumnDescriptor("count", DqeTypes.BIGINT));
      SearchHitToPageConverter converter = new SearchHitToPageConverter(cols);

      Page page = converter.convert(List.of(Map.of("count", "42")));

      assertEquals(42L, BigintType.BIGINT.getLong(page.getBlock(0), 0));
    }

    @Test
    @DisplayName("String value coerced to DOUBLE")
    void stringToDouble() {
      List<ColumnDescriptor> cols = List.of(new ColumnDescriptor("score", DqeTypes.DOUBLE));
      SearchHitToPageConverter converter = new SearchHitToPageConverter(cols);

      Page page = converter.convert(List.of(Map.of("score", "3.14")));

      assertEquals(3.14, DoubleType.DOUBLE.getDouble(page.getBlock(0), 0), 0.001);
    }

    @Test
    @DisplayName("String value coerced to BOOLEAN")
    void stringToBoolean() {
      List<ColumnDescriptor> cols = List.of(new ColumnDescriptor("active", DqeTypes.BOOLEAN));
      SearchHitToPageConverter converter = new SearchHitToPageConverter(cols);

      Page page = converter.convert(List.of(Map.of("active", "true"), Map.of("active", "false")));

      assertTrue(BooleanType.BOOLEAN.getBoolean(page.getBlock(0), 0));
      assertFalse(BooleanType.BOOLEAN.getBoolean(page.getBlock(0), 1));
    }
  }

  @Nested
  @DisplayName("Configuration")
  class Configuration {

    @Test
    @DisplayName("Default batch size is 1000")
    void defaultBatchSize() {
      SearchHitToPageConverter converter =
          new SearchHitToPageConverter(List.of(new ColumnDescriptor("x", DqeTypes.VARCHAR)));
      assertEquals(1000, converter.getBatchSize());
    }

    @Test
    @DisplayName("Custom batch size")
    void customBatchSize() {
      SearchHitToPageConverter converter =
          new SearchHitToPageConverter(List.of(new ColumnDescriptor("x", DqeTypes.VARCHAR)), 500);
      assertEquals(500, converter.getBatchSize());
    }

    @Test
    @DisplayName("Invalid batch size throws")
    void invalidBatchSize() {
      assertThrows(
          IllegalArgumentException.class,
          () ->
              new SearchHitToPageConverter(
                  List.of(new ColumnDescriptor("x", DqeTypes.VARCHAR)), 0));
    }

    @Test
    @DisplayName("Columns are accessible")
    void columnsAccessible() {
      List<ColumnDescriptor> cols =
          List.of(
              new ColumnDescriptor("a", DqeTypes.VARCHAR),
              new ColumnDescriptor("b", DqeTypes.BIGINT));
      SearchHitToPageConverter converter = new SearchHitToPageConverter(cols);
      assertEquals(2, converter.getColumns().size());
    }
  }

  @Nested
  @DisplayName("Custom date format parsing")
  class CustomDateFormat {

    @Test
    @DisplayName("Custom date format uses DateFormatInfo formatter")
    void customDateFormatParsing() {
      DateTimeFormatter customFormatter =
          DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss").withZone(ZoneOffset.UTC);
      DateFormatResolver.DateFormatInfo dateFormat =
          new DateFormatResolver.DateFormatInfo(customFormatter, 3);
      List<ColumnDescriptor> cols =
          List.of(new ColumnDescriptor("ts", DqeTypes.TIMESTAMP_MILLIS, dateFormat));
      SearchHitToPageConverter converter = new SearchHitToPageConverter(cols);

      Page page = converter.convert(List.of(Map.of("ts", "2021/05/03 00:00:00")));

      long micros = TimestampType.createTimestampType(3).getLong(page.getBlock(0), 0);
      // 2021-05-03T00:00:00Z = 1620000000 seconds = 1620000000000000 micros
      assertEquals(1620000000L * 1_000_000L, micros);
    }

    @Test
    @DisplayName("Custom date format with fallback to ISO when formatter fails")
    void customDateFormatFallbackToIso() {
      // Provide a formatter that won't match the ISO string
      DateTimeFormatter customFormatter =
          DateTimeFormatter.ofPattern("yyyy/MM/dd").withZone(ZoneOffset.UTC);
      DateFormatResolver.DateFormatInfo dateFormat =
          new DateFormatResolver.DateFormatInfo(customFormatter, 3);
      List<ColumnDescriptor> cols =
          List.of(new ColumnDescriptor("ts", DqeTypes.TIMESTAMP_MILLIS, dateFormat));
      SearchHitToPageConverter converter = new SearchHitToPageConverter(cols);

      // ISO string should fall through custom formatter to ISO fallback
      Page page = converter.convert(List.of(Map.of("ts", "2021-05-03T00:00:00Z")));

      long micros = TimestampType.createTimestampType(3).getLong(page.getBlock(0), 0);
      assertEquals(1620000000L * 1_000_000L, micros);
    }

    @Test
    @DisplayName("Epoch millis string without DateFormatInfo")
    void epochMillisStringWithoutDateFormat() {
      List<ColumnDescriptor> cols = List.of(new ColumnDescriptor("ts", DqeTypes.TIMESTAMP_MILLIS));
      SearchHitToPageConverter converter = new SearchHitToPageConverter(cols);

      Page page = converter.convert(List.of(Map.of("ts", "1620000000000")));

      long micros = TimestampType.createTimestampType(3).getLong(page.getBlock(0), 0);
      assertEquals(1620000000000000L, micros);
    }
  }

  @Nested
  @DisplayName("Complex type conversions (ROW, ARRAY, MAP)")
  class ComplexTypes {

    @Test
    @DisplayName("Converts ARRAY(VARCHAR) from list of strings")
    void arrayOfVarchar() {
      DqeType arrayType = DqeTypes.array(DqeTypes.VARCHAR);
      List<ColumnDescriptor> cols = List.of(new ColumnDescriptor("tags", arrayType));
      SearchHitToPageConverter converter = new SearchHitToPageConverter(cols);

      Page page = converter.convert(List.of(Map.of("tags", List.of("java", "python", "go"))));

      assertEquals(1, page.getPositionCount());
      Block block = page.getBlock(0);
      assertFalse(block.isNull(0));

      // Extract the array elements
      ArrayType trinoArrayType = (ArrayType) arrayType.getTrinoType();
      Block arrayBlock = trinoArrayType.getObject(block, 0);
      assertEquals(3, arrayBlock.getPositionCount());
      assertEquals("java", VarcharType.VARCHAR.getSlice(arrayBlock, 0).toStringUtf8());
      assertEquals("python", VarcharType.VARCHAR.getSlice(arrayBlock, 1).toStringUtf8());
      assertEquals("go", VarcharType.VARCHAR.getSlice(arrayBlock, 2).toStringUtf8());
    }

    @Test
    @DisplayName("Converts ARRAY(BIGINT) from list of numbers")
    void arrayOfBigint() {
      DqeType arrayType = DqeTypes.array(DqeTypes.BIGINT);
      List<ColumnDescriptor> cols = List.of(new ColumnDescriptor("scores", arrayType));
      SearchHitToPageConverter converter = new SearchHitToPageConverter(cols);

      Page page = converter.convert(List.of(Map.of("scores", List.of(10L, 20L, 30L))));

      Block block = page.getBlock(0);
      ArrayType trinoArrayType = (ArrayType) arrayType.getTrinoType();
      Block arrayBlock = trinoArrayType.getObject(block, 0);
      assertEquals(3, arrayBlock.getPositionCount());
      assertEquals(10L, BigintType.BIGINT.getLong(arrayBlock, 0));
      assertEquals(20L, BigintType.BIGINT.getLong(arrayBlock, 1));
      assertEquals(30L, BigintType.BIGINT.getLong(arrayBlock, 2));
    }

    @Test
    @DisplayName("Converts ROW(lat DOUBLE, lon DOUBLE) from map")
    void rowFromMap() {
      DqeType rowType =
          DqeTypes.row(
              List.of(
                  new DqeType.RowField("lat", DqeTypes.DOUBLE),
                  new DqeType.RowField("lon", DqeTypes.DOUBLE)));
      List<ColumnDescriptor> cols = List.of(new ColumnDescriptor("location", rowType));
      SearchHitToPageConverter converter = new SearchHitToPageConverter(cols);

      Map<String, Object> doc = Map.of("location", Map.of("lat", 40.7128, "lon", -74.0060));
      Page page = converter.convert(List.of(doc));

      Block block = page.getBlock(0);
      assertFalse(block.isNull(0));

      RowType trinoRowType = (RowType) rowType.getTrinoType();
      SqlRow sqlRow = trinoRowType.getObject(block, 0);
      int rawIndex0 = sqlRow.getRawIndex();
      assertEquals(
          40.7128, DoubleType.DOUBLE.getDouble(sqlRow.getRawFieldBlock(0), rawIndex0), 0.0001);
      assertEquals(
          -74.0060, DoubleType.DOUBLE.getDouble(sqlRow.getRawFieldBlock(1), rawIndex0), 0.0001);
    }

    @Test
    @DisplayName("ROW with non-map value produces NULL")
    void rowNonMapValueProducesNull() {
      DqeType rowType = DqeTypes.row(List.of(new DqeType.RowField("x", DqeTypes.VARCHAR)));
      List<ColumnDescriptor> cols = List.of(new ColumnDescriptor("data", rowType));
      SearchHitToPageConverter converter = new SearchHitToPageConverter(cols);

      Page page = converter.convert(List.of(Map.of("data", "not-a-map")));

      assertTrue(page.getBlock(0).isNull(0));
    }

    @Test
    @DisplayName("MAP with non-map value produces NULL")
    void mapNonMapValueProducesNull() {
      DqeType mapType = DqeTypes.map(DqeTypes.VARCHAR, DqeTypes.VARCHAR);
      List<ColumnDescriptor> cols = List.of(new ColumnDescriptor("meta", mapType));
      SearchHitToPageConverter converter = new SearchHitToPageConverter(cols);

      Page page = converter.convert(List.of(Map.of("meta", "not-a-map")));

      assertTrue(page.getBlock(0).isNull(0));
    }
  }

  @Nested
  @DisplayName("DECIMAL precision with large values")
  class DecimalPrecision {

    @Test
    @DisplayName("Large long value preserves precision in DECIMAL")
    void largeLongPreservesPrecision() {
      // Long.MAX_VALUE = 9223372036854775807 (19 digits)
      DqeType decimalType = DqeTypes.decimal(20, 0);
      List<ColumnDescriptor> cols = List.of(new ColumnDescriptor("val", decimalType));
      SearchHitToPageConverter converter = new SearchHitToPageConverter(cols);

      Page page = converter.convert(List.of(Map.of("val", Long.MAX_VALUE)));

      DecimalType trinoDecimal = (DecimalType) decimalType.getTrinoType();
      BigDecimal actual = Decimals.readBigDecimal(trinoDecimal, page.getBlock(0), 0);
      assertEquals(new BigDecimal(Long.MAX_VALUE), actual);
    }

    @Test
    @DisplayName("String representation of large unsigned_long preserves precision")
    void stringUnsignedLongPreservesPrecision() {
      // Simulate unsigned_long near 2^64: "18446744073709551615"
      String largeVal = "18446744073709551615";
      DqeType decimalType = DqeTypes.decimal(20, 0);
      List<ColumnDescriptor> cols = List.of(new ColumnDescriptor("val", decimalType));
      SearchHitToPageConverter converter = new SearchHitToPageConverter(cols);

      Page page = converter.convert(List.of(Map.of("val", largeVal)));

      DecimalType trinoDecimal = (DecimalType) decimalType.getTrinoType();
      BigDecimal actual = Decimals.readBigDecimal(trinoDecimal, page.getBlock(0), 0);
      assertEquals(new BigDecimal(largeVal), actual);
    }

    @Test
    @DisplayName("Integer Number uses longValue not doubleValue for DECIMAL")
    void integerNumberUsesLongValue() {
      // A value like 9007199254740993L (2^53 + 1) loses precision in double
      long preciseValue = 9007199254740993L;
      DqeType decimalType = DqeTypes.decimal(19, 0);
      List<ColumnDescriptor> cols = List.of(new ColumnDescriptor("val", decimalType));
      SearchHitToPageConverter converter = new SearchHitToPageConverter(cols);

      Page page = converter.convert(List.of(Map.of("val", preciseValue)));

      DecimalType trinoDecimal = (DecimalType) decimalType.getTrinoType();
      BigDecimal actual = Decimals.readBigDecimal(trinoDecimal, page.getBlock(0), 0);
      assertEquals(BigDecimal.valueOf(preciseValue), actual);
    }
  }

  @Nested
  @DisplayName("Conversion error paths")
  class ErrorPaths {

    @Test
    @DisplayName("Unparseable BIGINT string throws NumberFormatException")
    void unparseableBigint() {
      List<ColumnDescriptor> cols = List.of(new ColumnDescriptor("count", DqeTypes.BIGINT));
      SearchHitToPageConverter converter = new SearchHitToPageConverter(cols);

      assertThrows(
          NumberFormatException.class,
          () -> converter.convert(List.of(Map.of("count", "not-a-number"))));
    }

    @Test
    @DisplayName("Unparseable DOUBLE string throws NumberFormatException")
    void unparseableDouble() {
      List<ColumnDescriptor> cols = List.of(new ColumnDescriptor("score", DqeTypes.DOUBLE));
      SearchHitToPageConverter converter = new SearchHitToPageConverter(cols);

      assertThrows(
          NumberFormatException.class, () -> converter.convert(List.of(Map.of("score", "abc"))));
    }

    @Test
    @DisplayName("Unparseable DECIMAL string throws NumberFormatException")
    void unparseableDecimal() {
      DqeType decimalType = DqeTypes.decimal(10, 2);
      List<ColumnDescriptor> cols = List.of(new ColumnDescriptor("price", decimalType));
      SearchHitToPageConverter converter = new SearchHitToPageConverter(cols);

      assertThrows(
          NumberFormatException.class, () -> converter.convert(List.of(Map.of("price", "xyz"))));
    }

    @Test
    @DisplayName("Unparseable TIMESTAMP string throws NumberFormatException")
    void unparseableTimestamp() {
      List<ColumnDescriptor> cols = List.of(new ColumnDescriptor("ts", DqeTypes.TIMESTAMP_MILLIS));
      SearchHitToPageConverter converter = new SearchHitToPageConverter(cols);

      assertThrows(
          NumberFormatException.class,
          () -> converter.convert(List.of(Map.of("ts", "not-a-date"))));
    }
  }
}
