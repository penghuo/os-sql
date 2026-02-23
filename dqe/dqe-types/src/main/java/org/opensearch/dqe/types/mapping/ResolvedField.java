/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.types.mapping;

import java.util.Objects;
import org.opensearch.dqe.types.DqeType;

/**
 * Result of type resolution for a single OpenSearch field. Carries the resolved DQE type along with
 * multi-field metadata and sortability.
 *
 * <p>This is the intermediate data carrier between the type mapping layer (dqe-types) and the
 * metadata handle construction layer (dqe-metadata). The metadata layer constructs {@code
 * DqeColumnHandle} from {@code ResolvedField}.
 */
public final class ResolvedField {

  private final String fieldPath;
  private final DqeType type;
  private final boolean sortable;
  private final String keywordSubField;
  private final boolean hasFielddata;
  private final boolean isArray;

  /**
   * Constructs a resolved field.
   *
   * @param fieldPath dot-notation path (e.g. "address.city")
   * @param type resolved DQE type
   * @param sortable whether this field supports sorting
   * @param keywordSubField path to the keyword sub-field (e.g. "title.keyword"), or null
   * @param hasFielddata true if the text field has fielddata enabled
   * @param isArray true if the field was detected as multi-valued
   */
  public ResolvedField(
      String fieldPath,
      DqeType type,
      boolean sortable,
      String keywordSubField,
      boolean hasFielddata,
      boolean isArray) {
    this.fieldPath = Objects.requireNonNull(fieldPath, "fieldPath must not be null");
    this.type = Objects.requireNonNull(type, "type must not be null");
    this.sortable = sortable;
    this.keywordSubField = keywordSubField;
    this.hasFielddata = hasFielddata;
    this.isArray = isArray;
  }

  /** Returns the dot-notation field path (e.g. "address.city"). */
  public String getFieldPath() {
    return fieldPath;
  }

  /** Returns the resolved DQE type. */
  public DqeType getType() {
    return type;
  }

  /** Returns whether this field supports sorting in OpenSearch pushdown. */
  public boolean isSortable() {
    return sortable;
  }

  /**
   * Returns the keyword sub-field path (e.g. "title.keyword") for text fields with a keyword
   * sub-field, or null if not applicable.
   */
  public String getKeywordSubField() {
    return keywordSubField;
  }

  /** Returns true if the text field has fielddata enabled. */
  public boolean hasFielddata() {
    return hasFielddata;
  }

  /** Returns true if the field was detected as multi-valued. */
  public boolean isArray() {
    return isArray;
  }

  /**
   * Returns the field path to use for pushdown, based on the operation type. For text fields with a
   * keyword sub-field, equality, IN, and sorting use the .keyword sub-field; full-text operations
   * use the text field directly.
   *
   * @param usage the operation context
   * @return the field path to use in OpenSearch queries
   */
  public String getPushdownFieldPath(FieldUsage usage) {
    if (keywordSubField != null && usage.prefersKeyword()) {
      return keywordSubField;
    }
    return fieldPath;
  }

  /**
   * Returns the field path to use for sorting. Text fields with a keyword sub-field sort on the
   * .keyword sub-field. Text fields with fielddata sort on the field itself. Other types sort on
   * the field path directly.
   *
   * @return the field path for sort pushdown, or null if this field is not sortable
   */
  public String getSortFieldPath() {
    if (!sortable && keywordSubField == null) {
      return null;
    }
    if (keywordSubField != null) {
      return keywordSubField;
    }
    return fieldPath;
  }

  /**
   * Returns true if this field has a keyword sub-field (i.e., it is a text+keyword multi-field).
   */
  public boolean hasKeywordSubField() {
    return keywordSubField != null;
  }

  /**
   * Describes how a field is being used in a query, which determines whether the .keyword sub-field
   * should be used.
   */
  public enum FieldUsage {
    /** Equality comparison ({@code =}, {@code !=}). Uses .keyword for text fields. */
    EQUALITY,
    /** Set membership ({@code IN}). Uses .keyword for text fields. */
    IN,
    /** Sorting ({@code ORDER BY}). Uses .keyword for text fields. */
    SORT,
    /** Range comparison ({@code <}, {@code >}, {@code <=}, {@code >=}). Uses .keyword. */
    RANGE,
    /** Full-text search (e.g., LIKE, match). Uses the text field directly. */
    FULL_TEXT,
    /** Projection (SELECT). Uses the base field path. */
    PROJECTION;

    /** Returns true if this usage prefers the .keyword sub-field over the text field. */
    public boolean prefersKeyword() {
      return this == EQUALITY || this == IN || this == SORT || this == RANGE;
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ResolvedField other)) {
      return false;
    }
    return sortable == other.sortable
        && hasFielddata == other.hasFielddata
        && isArray == other.isArray
        && fieldPath.equals(other.fieldPath)
        && type.equals(other.type)
        && Objects.equals(keywordSubField, other.keywordSubField);
  }

  @Override
  public int hashCode() {
    return Objects.hash(fieldPath, type, sortable, keywordSubField, hasFielddata, isArray);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("ResolvedField{");
    sb.append("path=").append(fieldPath);
    sb.append(", type=").append(type);
    sb.append(", sortable=").append(sortable);
    if (keywordSubField != null) {
      sb.append(", keyword=").append(keywordSubField);
    }
    if (isArray) {
      sb.append(", array=true");
    }
    sb.append('}');
    return sb.toString();
  }
}
