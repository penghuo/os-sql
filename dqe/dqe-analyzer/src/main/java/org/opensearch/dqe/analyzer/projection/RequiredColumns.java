/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.analyzer.projection;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import org.opensearch.dqe.metadata.DqeColumnHandle;

/**
 * The minimal set of columns needed from the table scan. Used by {@code ShardScanOperator} for
 * {@code fetchSource} field filtering.
 */
public class RequiredColumns {

  private final Set<DqeColumnHandle> columns;
  private final boolean allColumns;

  public RequiredColumns(Set<DqeColumnHandle> columns) {
    this.columns = Set.copyOf(Objects.requireNonNull(columns, "columns must not be null"));
    this.allColumns = false;
  }

  private RequiredColumns(Set<DqeColumnHandle> columns, boolean allColumns) {
    this.columns = Set.copyOf(columns);
    this.allColumns = allColumns;
  }

  /** The set of column handles needed from the scan. */
  public Set<DqeColumnHandle> getColumns() {
    return columns;
  }

  /** Field names for use in {@code SearchSourceBuilder.fetchSource()}. */
  public String[] getFieldNames() {
    return columns.stream().map(DqeColumnHandle::getFieldPath).toArray(String[]::new);
  }

  /** Number of required columns. */
  public int size() {
    return columns.size();
  }

  /** Returns true if all columns from the table are required (SELECT *). */
  public boolean isAllColumns() {
    return allColumns;
  }

  /** Creates a RequiredColumns representing all table columns (for SELECT *). */
  public static RequiredColumns allColumns(List<DqeColumnHandle> allTableColumns) {
    return new RequiredColumns(Set.copyOf(allTableColumns), true);
  }
}
