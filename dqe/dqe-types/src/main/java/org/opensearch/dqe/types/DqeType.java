/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.types;

import io.trino.spi.type.ArrayType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import java.util.List;
import java.util.Objects;

/**
 * Immutable type representation wrapping a Trino {@link Type}. Adds sortability metadata and
 * OpenSearch-specific concerns on top of the Trino type.
 *
 * <p>Instances are created via the {@link DqeTypes} static factory. The constructor is
 * package-private to enforce canonical type instances for non-parameterized types.
 */
public final class DqeType {

  private final Type trinoType;
  private final String displayName;
  private final boolean sortable;

  DqeType(Type trinoType, String displayName, boolean sortable) {
    this.trinoType = Objects.requireNonNull(trinoType, "trinoType must not be null");
    this.displayName = Objects.requireNonNull(displayName, "displayName must not be null");
    this.sortable = sortable;
  }

  /** Returns the underlying Trino {@link Type} for use by operators and serialization. */
  public Type getTrinoType() {
    return trinoType;
  }

  /** Returns a human-readable display name, e.g. "VARCHAR", "BIGINT", "TIMESTAMP(3)". */
  public String getDisplayName() {
    return displayName;
  }

  /** Returns whether this type supports sorting in OpenSearch pushdown. */
  public boolean isSortable() {
    return sortable;
  }

  /** Returns true for numeric types: BIGINT, INTEGER, SMALLINT, TINYINT, DOUBLE, REAL, DECIMAL. */
  public boolean isNumeric() {
    String name = trinoType.getBaseName();
    return "bigint".equals(name)
        || "integer".equals(name)
        || "smallint".equals(name)
        || "tinyint".equals(name)
        || "double".equals(name)
        || "real".equals(name)
        || "decimal".equals(name);
  }

  /**
   * Returns true if values of this type can be compared with {@code =} and ordering operators. All
   * types except ROW, MAP, and unsortable complex types are comparable.
   */
  public boolean isComparable() {
    return trinoType.isComparable();
  }

  /** Returns true for parameterized types: DECIMAL, TIMESTAMP, ROW, ARRAY, MAP. */
  public boolean isParameterized() {
    return trinoType instanceof DecimalType
        || trinoType instanceof TimestampType
        || trinoType instanceof RowType
        || trinoType instanceof ArrayType
        || trinoType instanceof MapType;
  }

  /** Returns true if this is a ROW type. */
  public boolean isRowType() {
    return trinoType instanceof RowType;
  }

  /** Returns true if this is an ARRAY type. */
  public boolean isArrayType() {
    return trinoType instanceof ArrayType;
  }

  /** Returns true if this is a MAP type. */
  public boolean isMapType() {
    return trinoType instanceof MapType;
  }

  /**
   * Returns the element type for ARRAY types.
   *
   * @throws IllegalStateException if this is not an ARRAY type
   */
  public DqeType getElementType() {
    if (!(trinoType instanceof ArrayType)) {
      throw new IllegalStateException("getElementType() called on non-ARRAY type: " + displayName);
    }
    Type elementTrinoType = ((ArrayType) trinoType).getElementType();
    return DqeTypes.fromTrinoType(elementTrinoType);
  }

  /**
   * Returns the field descriptors for ROW types.
   *
   * @throws IllegalStateException if this is not a ROW type
   */
  public List<RowField> getRowFields() {
    if (!(trinoType instanceof RowType)) {
      throw new IllegalStateException("getRowFields() called on non-ROW type: " + displayName);
    }
    RowType rowType = (RowType) trinoType;
    return rowType.getFields().stream()
        .map(f -> new RowField(f.getName().orElse(""), DqeTypes.fromTrinoType(f.getType())))
        .toList();
  }

  /**
   * Returns the precision for DECIMAL or TIMESTAMP types.
   *
   * @throws IllegalStateException if this is not a DECIMAL or TIMESTAMP type
   */
  public int getPrecision() {
    if (trinoType instanceof DecimalType) {
      return ((DecimalType) trinoType).getPrecision();
    }
    if (trinoType instanceof TimestampType) {
      return ((TimestampType) trinoType).getPrecision();
    }
    throw new IllegalStateException(
        "getPrecision() called on non-DECIMAL/TIMESTAMP type: " + displayName);
  }

  /**
   * Returns the scale for DECIMAL types.
   *
   * @throws IllegalStateException if this is not a DECIMAL type
   */
  public int getScale() {
    if (!(trinoType instanceof DecimalType)) {
      throw new IllegalStateException("getScale() called on non-DECIMAL type: " + displayName);
    }
    return ((DecimalType) trinoType).getScale();
  }

  /**
   * Type identity is based solely on the underlying Trino type. Sortability is metadata carried by
   * {@link org.opensearch.dqe.types.mapping.ResolvedField}, not part of type identity.
   */
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof DqeType other)) {
      return false;
    }
    return trinoType.equals(other.trinoType);
  }

  @Override
  public int hashCode() {
    return trinoType.hashCode();
  }

  @Override
  public String toString() {
    return displayName;
  }

  /** Describes a field within a ROW type. */
  public static final class RowField {

    private final String name;
    private final DqeType type;

    public RowField(String name, DqeType type) {
      this.name = Objects.requireNonNull(name, "name must not be null");
      this.type = Objects.requireNonNull(type, "type must not be null");
    }

    public String getName() {
      return name;
    }

    public DqeType getType() {
      return type;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof RowField other)) {
        return false;
      }
      return name.equals(other.name) && type.equals(other.type);
    }

    @Override
    public int hashCode() {
      return Objects.hash(name, type);
    }

    @Override
    public String toString() {
      return name + " " + type;
    }
  }
}
