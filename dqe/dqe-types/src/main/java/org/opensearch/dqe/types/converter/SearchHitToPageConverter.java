/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.types.converter;

import io.airlift.slice.Slices;
import io.trino.spi.Page;
import io.trino.spi.block.ArrayBlockBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.MapBlockBuilder;
import io.trino.spi.block.RowBlockBuilder;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.Int128;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.temporal.TemporalAccessor;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.opensearch.dqe.types.mapping.DateFormatResolver;

/**
 * Converts OpenSearch SearchHit source documents into Trino's columnar {@link Page}/{@link Block}
 * format.
 *
 * <p>Each call to {@link #convert(List)} takes a batch of source documents (as Maps) and produces a
 * single Page. The Page contains one Block per column, where each Block contains the values for all
 * rows in the batch.
 *
 * <p>This converter handles NULL values by appending nulls to the appropriate block positions.
 *
 * <p><b>R-note (signature deviation):</b> The spec defines {@code Page convert(SearchHit[] hits)},
 * but dqe-types must not depend on the OpenSearch SearchHit class. This implementation accepts
 * {@code List<Map<String, Object>>} instead. The caller (ShardScanOperator in dqe-execution) is
 * responsible for converting SearchHit source maps before calling this method.
 *
 * <p>Usage:
 *
 * <pre>{@code
 * List<ColumnDescriptor> columns = List.of(
 *     new ColumnDescriptor("name", DqeTypes.VARCHAR),
 *     new ColumnDescriptor("age", DqeTypes.INTEGER)
 * );
 * SearchHitToPageConverter converter = new SearchHitToPageConverter(columns, 1000);
 * Page page = converter.convert(hits);
 * }</pre>
 */
public class SearchHitToPageConverter {

  /** Default batch size (number of documents per page). */
  public static final int DEFAULT_BATCH_SIZE = 1000;

  private final List<ColumnDescriptor> columns;
  private final int batchSize;

  /**
   * Creates a converter.
   *
   * @param columns the column descriptors defining the output schema
   * @param batchSize maximum number of rows per page
   */
  public SearchHitToPageConverter(List<ColumnDescriptor> columns, int batchSize) {
    this.columns = List.copyOf(Objects.requireNonNull(columns, "columns must not be null"));
    if (batchSize <= 0) {
      throw new IllegalArgumentException("batchSize must be positive, got: " + batchSize);
    }
    this.batchSize = batchSize;
  }

  /** Creates a converter with the default batch size. */
  public SearchHitToPageConverter(List<ColumnDescriptor> columns) {
    this(columns, DEFAULT_BATCH_SIZE);
  }

  /**
   * Converts a batch of source documents into a Trino Page.
   *
   * @param hits list of source documents (each is a Map from field path to value)
   * @return a Page with one Block per column
   */
  public Page convert(List<Map<String, Object>> hits) {
    Objects.requireNonNull(hits, "hits must not be null");
    int rowCount = hits.size();

    Block[] blocks = new Block[columns.size()];

    for (int col = 0; col < columns.size(); col++) {
      ColumnDescriptor descriptor = columns.get(col);
      Type trinoType = descriptor.getType().getTrinoType();
      BlockBuilder builder = trinoType.createBlockBuilder(null, rowCount);

      for (int row = 0; row < rowCount; row++) {
        Map<String, Object> source = hits.get(row);
        Object value = extractNestedValue(source, descriptor.getFieldPath());
        appendValue(builder, trinoType, value, descriptor);
      }

      blocks[col] = builder.build();
    }

    return new Page(rowCount, blocks);
  }

  /** Returns the configured batch size. */
  public int getBatchSize() {
    return batchSize;
  }

  /** Returns the column descriptors. */
  public List<ColumnDescriptor> getColumns() {
    return columns;
  }

  /**
   * Appends a single value to the block builder, handling NULL and type-specific conversion.
   *
   * @param builder the block builder
   * @param trinoType the Trino type for this column
   * @param value the raw value from the SearchHit source (may be null)
   * @param descriptor the column descriptor (for date format info)
   */
  private void appendValue(
      BlockBuilder builder, Type trinoType, Object value, ColumnDescriptor descriptor) {
    if (value == null) {
      builder.appendNull();
      return;
    }

    try {
      String baseName = trinoType.getBaseName();
      switch (baseName) {
        case "varchar" -> appendVarchar(builder, value);
        case "bigint" -> appendLong(builder, trinoType, toLong(value));
        case "integer" -> appendLong(builder, trinoType, toInt(value));
        case "smallint" -> appendLong(builder, trinoType, toShort(value));
        case "tinyint" -> appendLong(builder, trinoType, toByte(value));
        case "double" -> appendDouble(builder, trinoType, toDouble(value));
        case "real" -> appendLong(builder, trinoType, Float.floatToIntBits(toFloat(value)));
        case "boolean" -> appendBoolean(builder, trinoType, toBoolean(value));
        case "decimal" -> appendDecimal(builder, (DecimalType) trinoType, value);
        case "timestamp" -> appendTimestamp(builder, (TimestampType) trinoType, value, descriptor);
        case "varbinary" -> appendVarbinary(builder, value);
        case "array" -> appendArray(builder, (ArrayType) trinoType, value);
        case "row" -> appendRow(builder, (RowType) trinoType, value);
        case "map" -> appendMap(builder, (MapType) trinoType, value);
        default -> appendVarchar(builder, value);
      }
    } catch (Exception e) {
      // Gracefully handle conversion errors: append null instead of crashing the scan.
      // This covers cases like geo_point stored as [lon,lat] array when type expects ROW,
      // or numeric fields containing non-numeric values.
      builder.appendNull();
    }
  }

  private void appendVarchar(BlockBuilder builder, Object value) {
    String str = value.toString();
    byte[] bytes = str.getBytes(StandardCharsets.UTF_8);
    ((VarcharType) VarcharType.VARCHAR).writeSlice(builder, Slices.wrappedBuffer(bytes));
  }

  private void appendLong(BlockBuilder builder, Type type, long value) {
    type.writeLong(builder, value);
  }

  private void appendDouble(BlockBuilder builder, Type type, double value) {
    type.writeDouble(builder, value);
  }

  private void appendBoolean(BlockBuilder builder, Type type, boolean value) {
    type.writeBoolean(builder, value);
  }

  private void appendDecimal(BlockBuilder builder, DecimalType decimalType, Object value) {
    BigDecimal bd;
    if (value instanceof BigDecimal) {
      bd = (BigDecimal) value;
    } else if (value instanceof Long
        || value instanceof Integer
        || value instanceof Short
        || value instanceof Byte) {
      // Use longValue for integer Number instances to avoid precision loss from double conversion.
      // Values near 2^64 (e.g. unsigned_long) would lose precision via doubleValue().
      bd = BigDecimal.valueOf(((Number) value).longValue());
    } else if (value instanceof Number) {
      // Floating-point Number instances: use toString() to preserve all digits.
      bd = new BigDecimal(value.toString());
    } else {
      bd = new BigDecimal(value.toString());
    }

    bd = bd.setScale(decimalType.getScale(), RoundingMode.HALF_UP);

    if (decimalType.isShort()) {
      long unscaledValue = bd.unscaledValue().longValueExact();
      decimalType.writeLong(builder, unscaledValue);
    } else {
      Int128 int128 = Decimals.valueOf(bd);
      decimalType.writeObject(builder, int128);
    }
  }

  private void appendTimestamp(
      BlockBuilder builder, TimestampType tsType, Object value, ColumnDescriptor descriptor) {
    // Convert the value to microseconds since epoch (Trino's internal timestamp format)
    long epochMicros;
    if (value instanceof Number) {
      long numVal = ((Number) value).longValue();
      if (tsType.getPrecision() <= 3) {
        // Assume millis
        epochMicros = numVal * 1000L;
      } else {
        // Assume nanos, convert to micros
        epochMicros = numVal / 1000L;
      }
    } else {
      // String value: use DateFormatInfo formatter when available for custom date formats
      String str = value.toString();
      DateFormatResolver.DateFormatInfo dateFormat = descriptor.getDateFormat();
      if (dateFormat != null) {
        epochMicros = parseWithFormatter(str, dateFormat);
      } else {
        // No date format info: try ISO parse, then epoch millis as fallback
        epochMicros = parseIsoOrEpochMillis(str);
      }
    }

    tsType.writeLong(builder, epochMicros);
  }

  private static long parseWithFormatter(String str, DateFormatResolver.DateFormatInfo dateFormat) {
    try {
      TemporalAccessor parsed = dateFormat.getFormatter().parse(str);
      Instant instant = Instant.from(parsed);
      return instant.getEpochSecond() * 1_000_000L + instant.getNano() / 1000L;
    } catch (java.time.DateTimeException e) {
      // Formatter failed; try ISO parse then epoch millis as fallback
      return parseIsoOrEpochMillis(str);
    }
  }

  private static long parseIsoOrEpochMillis(String str) {
    try {
      Instant instant = Instant.parse(str);
      return instant.getEpochSecond() * 1_000_000L + instant.getNano() / 1000L;
    } catch (java.time.format.DateTimeParseException e) {
      // Try epoch millis as string
      return Long.parseLong(str) * 1000L;
    }
  }

  private void appendVarbinary(BlockBuilder builder, Object value) {
    byte[] bytes;
    if (value instanceof byte[]) {
      bytes = (byte[]) value;
    } else {
      // Base64 encoded string from OpenSearch
      bytes = java.util.Base64.getDecoder().decode(value.toString());
    }
    io.trino.spi.type.VarbinaryType.VARBINARY.writeSlice(builder, Slices.wrappedBuffer(bytes));
  }

  @SuppressWarnings("unchecked")
  private void appendArray(BlockBuilder builder, ArrayType arrayType, Object value) {
    ArrayBlockBuilder arrayBuilder = (ArrayBlockBuilder) builder;
    if (!(value instanceof List)) {
      // Single value treated as a one-element array
      arrayBuilder.buildEntry(
          elementBuilder -> {
            appendValue(elementBuilder, arrayType.getElementType(), value, null);
          });
      return;
    }

    List<?> list = (List<?>) value;
    arrayBuilder.buildEntry(
        elementBuilder -> {
          Type elementType = arrayType.getElementType();
          for (Object item : list) {
            appendValue(elementBuilder, elementType, item, null);
          }
        });
  }

  @SuppressWarnings("unchecked")
  private void appendRow(BlockBuilder builder, RowType rowType, Object value) {
    if (!(value instanceof Map)) {
      builder.appendNull();
      return;
    }

    Map<String, Object> map = (Map<String, Object>) value;
    List<RowType.Field> fields = rowType.getFields();
    RowBlockBuilder rowBuilder = (RowBlockBuilder) builder;

    rowBuilder.buildEntry(
        fieldBuilders -> {
          for (int i = 0; i < fields.size(); i++) {
            RowType.Field field = fields.get(i);
            String fieldName = field.getName().orElse("");
            Object fieldValue = map.get(fieldName);
            appendValue(fieldBuilders.get(i), field.getType(), fieldValue, null);
          }
        });
  }

  @SuppressWarnings("unchecked")
  private void appendMap(BlockBuilder builder, MapType mapType, Object value) {
    if (!(value instanceof Map)) {
      builder.appendNull();
      return;
    }

    Map<String, Object> map = (Map<String, Object>) value;
    MapBlockBuilder mapBuilder = (MapBlockBuilder) builder;
    mapBuilder.buildEntry(
        (keyBuilder, valueBuilder) -> {
          for (Map.Entry<String, Object> entry : map.entrySet()) {
            appendValue(keyBuilder, mapType.getKeyType(), entry.getKey(), null);
            appendValue(valueBuilder, mapType.getValueType(), entry.getValue(), null);
          }
        });
  }

  // ---- Type conversion helpers ----

  private static long toLong(Object value) {
    if (value instanceof Number) {
      return ((Number) value).longValue();
    }
    return Long.parseLong(value.toString());
  }

  private static int toInt(Object value) {
    if (value instanceof Number) {
      return ((Number) value).intValue();
    }
    return Integer.parseInt(value.toString());
  }

  private static short toShort(Object value) {
    if (value instanceof Number) {
      return ((Number) value).shortValue();
    }
    return Short.parseShort(value.toString());
  }

  private static byte toByte(Object value) {
    if (value instanceof Number) {
      return ((Number) value).byteValue();
    }
    return Byte.parseByte(value.toString());
  }

  private static double toDouble(Object value) {
    if (value instanceof Number) {
      return ((Number) value).doubleValue();
    }
    return Double.parseDouble(value.toString());
  }

  private static float toFloat(Object value) {
    if (value instanceof Number) {
      return ((Number) value).floatValue();
    }
    return Float.parseFloat(value.toString());
  }

  private static boolean toBoolean(Object value) {
    if (value instanceof Boolean) {
      return (Boolean) value;
    }
    return Boolean.parseBoolean(value.toString());
  }

  /**
   * Extracts a value from a nested source map using dot-notation path.
   *
   * @param source the document source map
   * @param fieldPath dot-separated field path (e.g., "address.city")
   * @return the value at the path, or null if not found
   */
  @SuppressWarnings("unchecked")
  static Object extractNestedValue(Map<String, Object> source, String fieldPath) {
    // Fast path for simple field names (no dots)
    if (fieldPath.indexOf('.') < 0) {
      return source.get(fieldPath);
    }

    String[] parts = fieldPath.split("\\.");
    Object current = source;
    for (String part : parts) {
      if (!(current instanceof Map)) {
        return null;
      }
      current = ((Map<String, Object>) current).get(part);
      if (current == null) {
        return null;
      }
    }
    return current;
  }
}
