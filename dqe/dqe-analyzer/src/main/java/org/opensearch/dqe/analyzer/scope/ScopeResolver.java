/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.analyzer.scope;

import java.util.ArrayList;
import java.util.List;
import org.opensearch.dqe.analyzer.DqeAnalysisException;
import org.opensearch.dqe.metadata.DqeColumnHandle;

/**
 * Resolves unqualified and qualified column references against available scopes. Handles ambiguity
 * detection when a column name exists in multiple scopes or matches multiple fields.
 */
public class ScopeResolver {

  public ScopeResolver() {}

  /**
   * Resolves an unqualified column name (e.g., "price") against the current scope.
   *
   * @param columnName the unqualified column name
   * @param scope the current scope
   * @return the resolved field
   * @throws DqeAnalysisException if column is not found or is ambiguous
   */
  public ResolvedField resolveColumn(String columnName, Scope scope) {
    String lowerName = columnName.toLowerCase();
    List<DqeColumnHandle> matches = new ArrayList<>();

    for (DqeColumnHandle col : scope.getColumns()) {
      if (col.getFieldName().equalsIgnoreCase(columnName)
          || col.getFieldPath().equalsIgnoreCase(columnName)) {
        matches.add(col);
      }
    }

    if (matches.size() > 1) {
      throw new DqeAnalysisException(
          "Ambiguous column reference '" + columnName + "': matches " + matches.size() + " fields");
    }

    if (matches.size() == 1) {
      DqeColumnHandle col = matches.get(0);
      return new ResolvedField(scope.getTable(), col, col.getType());
    }

    // Check column aliases
    if (scope.getColumnAliases().containsKey(lowerName)) {
      // Alias references don't have a column handle — return null column, the caller
      // should handle this as an alias reference. For simplicity, we create a synthetic handle.
      throw new DqeAnalysisException(
          "Column alias '"
              + columnName
              + "' cannot be used in this context. "
              + "Column aliases from SELECT can only be referenced in ORDER BY.");
    }

    throw new DqeAnalysisException("Column '" + columnName + "' not found in table " + scope.getTableName());
  }

  /**
   * Resolves an unqualified column name, also checking column aliases (for ORDER BY). Returns a
   * ResolvedField with a synthetic column handle for aliases.
   *
   * @param columnName the unqualified column name
   * @param scope the current scope
   * @return the resolved field
   * @throws DqeAnalysisException if column is not found
   */
  public ResolvedField resolveColumnOrAlias(String columnName, Scope scope) {
    String lowerName = columnName.toLowerCase();

    // Try table columns first
    List<DqeColumnHandle> matches = new ArrayList<>();
    for (DqeColumnHandle col : scope.getColumns()) {
      if (col.getFieldName().equalsIgnoreCase(columnName)
          || col.getFieldPath().equalsIgnoreCase(columnName)) {
        matches.add(col);
      }
    }

    if (matches.size() > 1) {
      throw new DqeAnalysisException(
          "Ambiguous column reference '" + columnName + "': matches " + matches.size() + " fields");
    }

    if (matches.size() == 1) {
      DqeColumnHandle col = matches.get(0);
      return new ResolvedField(scope.getTable(), col, col.getType());
    }

    // Check column aliases (for ORDER BY)
    if (scope.getColumnAliases().containsKey(lowerName)) {
      // For ORDER BY referencing SELECT aliases, we need the original column.
      // Throw to let the caller handle alias resolution at a higher level.
      throw new DqeAnalysisException("_ALIAS_:" + columnName);
    }

    throw new DqeAnalysisException(
        "Column '" + columnName + "' not found in table " + scope.getTableName());
  }

  /**
   * Resolves a qualified column reference (e.g., "orders.price" or "o.price") against the scope,
   * matching table name or alias.
   *
   * @param qualifier the table name or alias
   * @param columnName the column name
   * @param scope the current scope
   * @return the resolved field
   * @throws DqeAnalysisException if table/alias or column is not found
   */
  public ResolvedField resolveQualifiedColumn(String qualifier, String columnName, Scope scope) {
    // Check if the qualifier matches the table name or alias
    boolean matches =
        scope.getTableName().equalsIgnoreCase(qualifier)
            || (scope.getTableAlias().isPresent()
                && scope.getTableAlias().get().equalsIgnoreCase(qualifier));

    if (!matches) {
      throw new DqeAnalysisException(
          "Table or alias '" + qualifier + "' not found in the current scope");
    }

    // Now resolve the column within this scope
    for (DqeColumnHandle col : scope.getColumns()) {
      if (col.getFieldName().equalsIgnoreCase(columnName)) {
        return new ResolvedField(scope.getTable(), col, col.getType());
      }
    }

    throw new DqeAnalysisException(
        "Column '" + columnName + "' not found in table " + qualifier);
  }
}
