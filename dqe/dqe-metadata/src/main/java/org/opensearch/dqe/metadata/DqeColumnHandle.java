/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.metadata;

import java.io.IOException;
import java.util.Objects;
import javax.annotation.Nullable;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.dqe.types.DqeType;
import org.opensearch.dqe.types.DqeTypes;

/**
 * Immutable handle representing a resolved column within an OpenSearch index. Carries field name,
 * field path (for nested fields), DQE type, sortability, keyword sub-field info, and array flag.
 *
 * <p>Implements {@link Writeable} for transport serialization between coordinator and data nodes.
 */
public final class DqeColumnHandle implements Writeable {

  private final String fieldName;
  private final String fieldPath;
  private final DqeType type;
  private final boolean sortable;
  @Nullable private final String keywordSubField;
  private final boolean isArray;

  public DqeColumnHandle(
      String fieldName,
      String fieldPath,
      DqeType type,
      boolean sortable,
      @Nullable String keywordSubField,
      boolean isArray) {
    this.fieldName = Objects.requireNonNull(fieldName, "fieldName must not be null");
    this.fieldPath = Objects.requireNonNull(fieldPath, "fieldPath must not be null");
    this.type = Objects.requireNonNull(type, "type must not be null");
    this.sortable = sortable;
    this.keywordSubField = keywordSubField;
    this.isArray = isArray;
  }

  /** Deserializes from a stream. */
  public DqeColumnHandle(StreamInput in) throws IOException {
    this.fieldName = in.readString();
    this.fieldPath = in.readString();
    this.type = readDqeType(in);
    this.sortable = in.readBoolean();
    this.keywordSubField = in.readOptionalString();
    this.isArray = in.readBoolean();
  }

  @Override
  public void writeTo(StreamOutput out) throws IOException {
    out.writeString(fieldName);
    out.writeString(fieldPath);
    writeDqeType(out, type);
    out.writeBoolean(sortable);
    out.writeOptionalString(keywordSubField);
    out.writeBoolean(isArray);
  }

  public String getFieldName() {
    return fieldName;
  }

  public String getFieldPath() {
    return fieldPath;
  }

  public DqeType getType() {
    return type;
  }

  public boolean isSortable() {
    return sortable;
  }

  @Nullable
  public String getKeywordSubField() {
    return keywordSubField;
  }

  public boolean isArray() {
    return isArray;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof DqeColumnHandle other)) {
      return false;
    }
    return fieldPath.equals(other.fieldPath) && type.equals(other.type);
  }

  @Override
  public int hashCode() {
    return Objects.hash(fieldPath, type);
  }

  @Override
  public String toString() {
    return "DqeColumnHandle{" + fieldPath + " " + type + (isArray ? " ARRAY" : "") + "}";
  }

  /**
   * Serializes a DqeType to the stream. Writes the display name and the Trino type base name so it
   * can be reconstructed on the other side.
   */
  private static void writeDqeType(StreamOutput out, DqeType type) throws IOException {
    out.writeString(type.getTrinoType().getBaseName());
    out.writeString(type.getDisplayName());
    out.writeBoolean(type.isSortable());
    // For parameterized types, write extra fields
    if (type.getTrinoType() instanceof io.trino.spi.type.DecimalType dt) {
      out.writeInt(dt.getPrecision());
      out.writeInt(dt.getScale());
    } else if (type.getTrinoType() instanceof io.trino.spi.type.TimestampType ts) {
      out.writeInt(ts.getPrecision());
    }
  }

  /**
   * Deserializes a DqeType from the stream. Reconstructs canonical types for common base names and
   * uses DqeTypes factory methods for parameterized types.
   */
  private static DqeType readDqeType(StreamInput in) throws IOException {
    String baseName = in.readString();
    String displayName = in.readString();
    boolean sortable = in.readBoolean();
    return switch (baseName) {
      case "varchar" -> DqeTypes.VARCHAR;
      case "bigint" -> DqeTypes.BIGINT;
      case "integer" -> DqeTypes.INTEGER;
      case "smallint" -> DqeTypes.SMALLINT;
      case "tinyint" -> DqeTypes.TINYINT;
      case "double" -> DqeTypes.DOUBLE;
      case "real" -> DqeTypes.REAL;
      case "boolean" -> DqeTypes.BOOLEAN;
      case "varbinary" -> DqeTypes.VARBINARY;
      case "decimal" -> {
        int precision = in.readInt();
        int scale = in.readInt();
        yield DqeTypes.decimal(precision, scale);
      }
      case "timestamp" -> {
        int precision = in.readInt();
        yield DqeTypes.timestamp(precision);
      }
      default -> DqeTypes.VARCHAR; // Fallback for unknown types
    };
  }
}
