/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.metadata;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Immutable snapshot of the schema (table handle + column handles) frozen at query analysis time.
 * Ensures consistent metadata for the lifetime of a query.
 */
public final class SchemaSnapshot {

  private final DqeTableHandle tableHandle;
  private final List<DqeColumnHandle> columns;
  private final long createdAtMillis;

  public SchemaSnapshot(
      DqeTableHandle tableHandle, List<DqeColumnHandle> columns, long createdAtMillis) {
    this.tableHandle = Objects.requireNonNull(tableHandle, "tableHandle must not be null");
    this.columns = List.copyOf(Objects.requireNonNull(columns, "columns must not be null"));
    this.createdAtMillis = createdAtMillis;
  }

  /** Returns the table handle. */
  public DqeTableHandle getTableHandle() {
    return tableHandle;
  }

  /** Returns all column handles. */
  public List<DqeColumnHandle> getColumns() {
    return columns;
  }

  /** Returns the timestamp (epoch millis) when this snapshot was created. */
  public long getCreatedAtMillis() {
    return createdAtMillis;
  }

  /** Looks up a column by field path. */
  public Optional<DqeColumnHandle> getColumn(String fieldPath) {
    for (DqeColumnHandle col : columns) {
      if (col.getFieldPath().equals(fieldPath)) {
        return Optional.of(col);
      }
    }
    return Optional.empty();
  }

  /** Returns the column at the given ordinal position. */
  public DqeColumnHandle getColumn(int ordinal) {
    return columns.get(ordinal);
  }

  /** Returns the number of columns. */
  public int getColumnCount() {
    return columns.size();
  }
}
