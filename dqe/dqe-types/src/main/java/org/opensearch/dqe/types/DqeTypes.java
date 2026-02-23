/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.types;

import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import java.util.List;
import java.util.Objects;

/**
 * Static factory for all {@link DqeType} instances. Provides canonical constants for
 * non-parameterized types and factory methods for parameterized types (DECIMAL, ARRAY, ROW, MAP,
 * TIMESTAMP).
 */
public final class DqeTypes {

  /** Shared TypeOperators instance used for creating MapType. */
  private static final TypeOperators TYPE_OPERATORS = new TypeOperators();

  // ---- Canonical type constants ----

  /** Variable-length character string. Maps to OpenSearch keyword, text, ip, geo_shape. */
  public static final DqeType VARCHAR = new DqeType(VarcharType.VARCHAR, "VARCHAR", true);

  /** 64-bit signed integer. Maps to OpenSearch long. */
  public static final DqeType BIGINT = new DqeType(BigintType.BIGINT, "BIGINT", true);

  /** 32-bit signed integer. Maps to OpenSearch integer. */
  public static final DqeType INTEGER = new DqeType(IntegerType.INTEGER, "INTEGER", true);

  /** 16-bit signed integer. Maps to OpenSearch short. */
  public static final DqeType SMALLINT = new DqeType(SmallintType.SMALLINT, "SMALLINT", true);

  /** 8-bit signed integer. Maps to OpenSearch byte. */
  public static final DqeType TINYINT = new DqeType(TinyintType.TINYINT, "TINYINT", true);

  /** 64-bit IEEE 754 floating point. Maps to OpenSearch double. */
  public static final DqeType DOUBLE = new DqeType(DoubleType.DOUBLE, "DOUBLE", true);

  /** 32-bit IEEE 754 floating point. Maps to OpenSearch float, half_float. */
  public static final DqeType REAL = new DqeType(RealType.REAL, "REAL", true);

  /** Boolean value. Maps to OpenSearch boolean. */
  public static final DqeType BOOLEAN = new DqeType(BooleanType.BOOLEAN, "BOOLEAN", true);

  /** Variable-length binary data. Maps to OpenSearch binary (base64). */
  public static final DqeType VARBINARY = new DqeType(VarbinaryType.VARBINARY, "VARBINARY", false);

  /** Timestamp with millisecond precision. Maps to OpenSearch date. */
  public static final DqeType TIMESTAMP_MILLIS =
      new DqeType(TimestampType.createTimestampType(3), "TIMESTAMP(3)", true);

  /** Timestamp with nanosecond precision. Maps to OpenSearch date_nanos. */
  public static final DqeType TIMESTAMP_NANOS =
      new DqeType(TimestampType.createTimestampType(9), "TIMESTAMP(9)", true);

  // ---- Factory methods for parameterized types ----

  /**
   * Creates a DECIMAL type with the given precision and scale.
   *
   * @param precision total number of digits (1-38)
   * @param scale number of fractional digits (0-precision)
   * @return DECIMAL(precision, scale) type, always sortable
   */
  public static DqeType decimal(int precision, int scale) {
    DecimalType decimalType = DecimalType.createDecimalType(precision, scale);
    return new DqeType(decimalType, "DECIMAL(" + precision + "," + scale + ")", true);
  }

  /**
   * Creates an ARRAY type with the given element type.
   *
   * @param elementType the type of array elements
   * @return ARRAY(elementType), not sortable
   */
  public static DqeType array(DqeType elementType) {
    Objects.requireNonNull(elementType, "elementType must not be null");
    ArrayType arrayType = new ArrayType(elementType.getTrinoType());
    return new DqeType(arrayType, "ARRAY(" + elementType.getDisplayName() + ")", false);
  }

  /**
   * Creates a ROW type with the given fields.
   *
   * @param fields list of field descriptors (name + type)
   * @return ROW type, not sortable
   */
  public static DqeType row(List<DqeType.RowField> fields) {
    Objects.requireNonNull(fields, "fields must not be null");
    if (fields.isEmpty()) {
      throw new IllegalArgumentException("ROW type must have at least one field");
    }
    List<RowType.Field> trinoFields =
        fields.stream()
            .map(
                f ->
                    new RowType.Field(
                        java.util.Optional.of(f.getName()), f.getType().getTrinoType()))
            .toList();
    RowType rowType = RowType.from(trinoFields);
    StringBuilder sb = new StringBuilder("ROW(");
    for (int i = 0; i < fields.size(); i++) {
      if (i > 0) {
        sb.append(", ");
      }
      sb.append(fields.get(i).getName())
          .append(" ")
          .append(fields.get(i).getType().getDisplayName());
    }
    sb.append(")");
    return new DqeType(rowType, sb.toString(), false);
  }

  /**
   * Creates a MAP type with the given key and value types.
   *
   * @param keyType the type of map keys
   * @param valueType the type of map values
   * @return MAP(keyType, valueType), not sortable
   */
  public static DqeType map(DqeType keyType, DqeType valueType) {
    Objects.requireNonNull(keyType, "keyType must not be null");
    Objects.requireNonNull(valueType, "valueType must not be null");
    MapType mapType = new MapType(keyType.getTrinoType(), valueType.getTrinoType(), TYPE_OPERATORS);
    return new DqeType(
        mapType,
        "MAP(" + keyType.getDisplayName() + ", " + valueType.getDisplayName() + ")",
        false);
  }

  /**
   * Creates a TIMESTAMP type with the given precision.
   *
   * @param precision 0-12 (fractional second digits). 3 = millis, 9 = nanos.
   * @return TIMESTAMP(precision), sortable
   */
  public static DqeType timestamp(int precision) {
    if (precision == 3) {
      return TIMESTAMP_MILLIS;
    }
    if (precision == 9) {
      return TIMESTAMP_NANOS;
    }
    TimestampType tsType = TimestampType.createTimestampType(precision);
    return new DqeType(tsType, "TIMESTAMP(" + precision + ")", true);
  }

  /**
   * Converts a Trino {@link Type} to its corresponding {@link DqeType}. Uses canonical constants
   * where possible.
   *
   * @param trinoType the Trino type to convert
   * @return corresponding DqeType
   * @throws IllegalArgumentException if the Trino type is not supported
   */
  public static DqeType fromTrinoType(Type trinoType) {
    Objects.requireNonNull(trinoType, "trinoType must not be null");

    if (trinoType instanceof VarcharType) {
      return VARCHAR;
    }
    if (trinoType instanceof BigintType) {
      return BIGINT;
    }
    if (trinoType instanceof IntegerType) {
      return INTEGER;
    }
    if (trinoType instanceof SmallintType) {
      return SMALLINT;
    }
    if (trinoType instanceof TinyintType) {
      return TINYINT;
    }
    if (trinoType instanceof DoubleType) {
      return DOUBLE;
    }
    if (trinoType instanceof RealType) {
      return REAL;
    }
    if (trinoType instanceof BooleanType) {
      return BOOLEAN;
    }
    if (trinoType instanceof VarbinaryType) {
      return VARBINARY;
    }
    if (trinoType instanceof TimestampType ts) {
      return timestamp(ts.getPrecision());
    }
    if (trinoType instanceof DecimalType dt) {
      return decimal(dt.getPrecision(), dt.getScale());
    }
    if (trinoType instanceof ArrayType at) {
      return array(fromTrinoType(at.getElementType()));
    }
    if (trinoType instanceof RowType rt) {
      List<DqeType.RowField> fields =
          rt.getFields().stream()
              .map(f -> new DqeType.RowField(f.getName().orElse(""), fromTrinoType(f.getType())))
              .toList();
      return row(fields);
    }
    if (trinoType instanceof MapType mt) {
      return map(fromTrinoType(mt.getKeyType()), fromTrinoType(mt.getValueType()));
    }

    throw new IllegalArgumentException("Unsupported Trino type: " + trinoType);
  }

  private DqeTypes() {
    // no instantiation
  }
}
