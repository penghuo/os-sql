/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.analyzer.sort;

import io.trino.sql.tree.DereferenceExpression;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.SortItem;
import java.util.ArrayList;
import java.util.List;
import org.opensearch.dqe.analyzer.DqeAnalysisException;
import org.opensearch.dqe.analyzer.scope.ResolvedField;
import org.opensearch.dqe.analyzer.scope.Scope;
import org.opensearch.dqe.analyzer.scope.ScopeResolver;
import org.opensearch.dqe.analyzer.type.ExpressionTypeChecker;
import org.opensearch.dqe.metadata.DqeColumnHandle;

/**
 * Validates and resolves ORDER BY clauses. Checks that referenced columns/expressions are sortable
 * per DQE type sortability from Section 9.1. Rejects sorting on non-sortable types (text without
 * fielddata, geo_point, nested).
 */
public class OrderByAnalyzer {

  private final ExpressionTypeChecker typeChecker;
  private final ScopeResolver scopeResolver;

  public OrderByAnalyzer(ExpressionTypeChecker typeChecker) {
    this.typeChecker = typeChecker;
    this.scopeResolver = new ScopeResolver();
  }

  /**
   * Analyzes ORDER BY clause items.
   *
   * @param sortItems the Trino SortItem list from ORDER BY
   * @param scope the current scope
   * @return resolved sort specifications
   * @throws DqeAnalysisException if a column is not sortable or not resolvable
   */
  public List<SortSpecification> analyze(List<SortItem> sortItems, Scope scope) {
    List<SortSpecification> specs = new ArrayList<>();

    for (SortItem item : sortItems) {
      DqeColumnHandle column = resolveOrderByColumn(item, scope);

      if (!column.isSortable()) {
        throw new DqeAnalysisException(
            "Column '"
                + column.getFieldName()
                + "' of type "
                + column.getType().getDisplayName()
                + " is not sortable. "
                + "Text fields require fielddata to be enabled for sorting.");
      }

      SortSpecification.SortDirection direction =
          item.getOrdering() == SortItem.Ordering.DESCENDING
              ? SortSpecification.SortDirection.DESC
              : SortSpecification.SortDirection.ASC;

      SortSpecification.NullOrdering nullOrdering = resolveNullOrdering(item, direction);

      specs.add(new SortSpecification(column, column.getType(), direction, nullOrdering));
    }

    return specs;
  }

  private DqeColumnHandle resolveOrderByColumn(SortItem item, Scope scope) {
    if (item.getSortKey() instanceof Identifier id) {
      // Try resolving as a column first, then as an alias
      try {
        ResolvedField resolved = scopeResolver.resolveColumn(id.getValue(), scope);
        return resolved.getColumn();
      } catch (DqeAnalysisException e) {
        // Check if it matches a SELECT alias
        String lowerName = id.getValue().toLowerCase();
        if (scope.getColumnAliases().containsKey(lowerName)) {
          throw new DqeAnalysisException(
              "ORDER BY alias '"
                  + id.getValue()
                  + "' refers to an expression, not a simple column. "
                  + "ORDER BY on aliased expressions is not supported in Phase 1.");
        }
        throw e;
      }
    }

    if (item.getSortKey() instanceof DereferenceExpression deref) {
      if (deref.getBase() instanceof Identifier base && deref.getField().isPresent()) {
        ResolvedField resolved =
            scopeResolver.resolveQualifiedColumn(
                base.getValue(), deref.getField().get().getValue(), scope);
        return resolved.getColumn();
      }
    }

    throw new DqeAnalysisException(
        "ORDER BY expression must be a column reference. Complex expressions in ORDER BY are not supported in Phase 1.");
  }

  private SortSpecification.NullOrdering resolveNullOrdering(
      SortItem item, SortSpecification.SortDirection direction) {
    if (item.getNullOrdering() == SortItem.NullOrdering.FIRST) {
      return SortSpecification.NullOrdering.NULLS_FIRST;
    }
    if (item.getNullOrdering() == SortItem.NullOrdering.LAST) {
      return SortSpecification.NullOrdering.NULLS_LAST;
    }
    // Default: NULLS LAST for ASC, NULLS FIRST for DESC (Trino convention)
    return direction == SortSpecification.SortDirection.ASC
        ? SortSpecification.NullOrdering.NULLS_LAST
        : SortSpecification.NullOrdering.NULLS_FIRST;
  }
}
