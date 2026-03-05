/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.analyzer.scope;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.opensearch.dqe.metadata.DqeColumnHandle;
import org.opensearch.dqe.metadata.DqeTableHandle;
import org.opensearch.dqe.types.DqeType;

/**
 * Represents a name-resolution scope (e.g., a FROM clause table). Holds the set of available
 * columns and optional table alias. Column aliases defined in SELECT can be added dynamically.
 */
public class Scope {

  private final DqeTableHandle table;
  private final List<DqeColumnHandle> columns;
  private final Optional<String> tableAlias;
  private final Map<String, DqeType> columnAliases;

  public Scope(DqeTableHandle table, List<DqeColumnHandle> columns, Optional<String> tableAlias) {
    this.table = Objects.requireNonNull(table, "table must not be null");
    this.columns = List.copyOf(Objects.requireNonNull(columns, "columns must not be null"));
    this.tableAlias = Objects.requireNonNull(tableAlias, "tableAlias must not be null");
    this.columnAliases = new HashMap<>();
  }

  public DqeTableHandle getTable() {
    return table;
  }

  public List<DqeColumnHandle> getColumns() {
    return columns;
  }

  public Optional<String> getTableAlias() {
    return tableAlias;
  }

  public String getTableName() {
    return table.getIndexName();
  }

  /**
   * Adds a column alias defined in SELECT (e.g., {@code SELECT price * qty AS total}).
   *
   * @param alias the alias name
   * @param type the resolved type of the aliased expression
   */
  public void addColumnAlias(String alias, DqeType type) {
    columnAliases.put(alias.toLowerCase(), type);
  }

  /** Returns the column aliases map (alias name -> type). Keys are lowercase. */
  public Map<String, DqeType> getColumnAliases() {
    return columnAliases;
  }
}
