/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.analyzer.scope;

import java.util.Objects;
import org.opensearch.dqe.metadata.DqeColumnHandle;
import org.opensearch.dqe.metadata.DqeTableHandle;
import org.opensearch.dqe.types.DqeType;

/**
 * A fully resolved column reference with its source table, column handle, and type. Produced by
 * {@link ScopeResolver} during identifier binding.
 */
public class ResolvedField {

  private final DqeTableHandle table;
  private final DqeColumnHandle column;
  private final DqeType type;

  public ResolvedField(DqeTableHandle table, DqeColumnHandle column, DqeType type) {
    this.table = Objects.requireNonNull(table, "table must not be null");
    this.column = Objects.requireNonNull(column, "column must not be null");
    this.type = Objects.requireNonNull(type, "type must not be null");
  }

  public DqeTableHandle getTable() {
    return table;
  }

  public DqeColumnHandle getColumn() {
    return column;
  }

  public DqeType getType() {
    return type;
  }

  public String getFieldName() {
    return column.getFieldName();
  }

  @Override
  public String toString() {
    return "ResolvedField{" + column.getFieldPath() + " " + type + "}";
  }
}
