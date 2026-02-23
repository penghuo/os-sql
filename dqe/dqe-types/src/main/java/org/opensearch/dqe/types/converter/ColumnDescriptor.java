/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.types.converter;

import java.util.Objects;
import org.opensearch.dqe.types.DqeType;
import org.opensearch.dqe.types.mapping.DateFormatResolver;

/**
 * Describes a column for the SearchHit to Page/Block conversion. Carries the field path, DQE type,
 * and optional date format info needed for date field parsing.
 */
public final class ColumnDescriptor {

  private final String fieldPath;
  private final DqeType type;
  private final DateFormatResolver.DateFormatInfo dateFormat;

  /**
   * Creates a column descriptor.
   *
   * @param fieldPath dot-notation field path in the OpenSearch source
   * @param type the DQE type for this column
   * @param dateFormat date format info for date fields, or null for non-date fields
   */
  public ColumnDescriptor(
      String fieldPath, DqeType type, DateFormatResolver.DateFormatInfo dateFormat) {
    this.fieldPath = Objects.requireNonNull(fieldPath, "fieldPath must not be null");
    this.type = Objects.requireNonNull(type, "type must not be null");
    this.dateFormat = dateFormat;
  }

  /** Creates a column descriptor for non-date fields. */
  public ColumnDescriptor(String fieldPath, DqeType type) {
    this(fieldPath, type, null);
  }

  /** Returns the dot-notation field path. */
  public String getFieldPath() {
    return fieldPath;
  }

  /** Returns the DQE type. */
  public DqeType getType() {
    return type;
  }

  /** Returns the date format info, or null for non-date fields. */
  public DateFormatResolver.DateFormatInfo getDateFormat() {
    return dateFormat;
  }

  @Override
  public String toString() {
    return fieldPath + " " + type + (dateFormat != null ? " [date]" : "");
  }
}
