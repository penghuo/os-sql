/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.analyzer.projection;

import io.trino.sql.tree.DereferenceExpression;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.Node;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.opensearch.dqe.analyzer.scope.Scope;
import org.opensearch.dqe.analyzer.type.TypedExpression;
import org.opensearch.dqe.metadata.DqeColumnHandle;

/**
 * Analyzes SELECT, WHERE, and ORDER BY clauses to compute the minimal set of columns needed from
 * the source table scan. Implements {@code PruneTableScanColumns} from design doc Section 10.1.
 */
public class ProjectionAnalyzer {

  public ProjectionAnalyzer() {}

  /**
   * Computes the minimal set of columns required to evaluate SELECT expressions, WHERE predicates,
   * and ORDER BY columns.
   *
   * @param selectExpressions typed SELECT expressions
   * @param whereClause typed WHERE expression (null if no WHERE)
   * @param orderByExpressions typed ORDER BY expressions (may be empty)
   * @param scope current scope with available columns
   * @return the minimal required columns
   */
  public RequiredColumns computeRequiredColumns(
      List<TypedExpression> selectExpressions,
      TypedExpression whereClause,
      List<TypedExpression> orderByExpressions,
      Scope scope) {
    Set<String> referencedNames = new HashSet<>();

    // Collect from SELECT
    for (TypedExpression te : selectExpressions) {
      collectColumnReferences(te.getExpression(), referencedNames);
    }

    // Collect from WHERE
    if (whereClause != null) {
      collectColumnReferences(whereClause.getExpression(), referencedNames);
    }

    // Collect from ORDER BY
    for (TypedExpression te : orderByExpressions) {
      collectColumnReferences(te.getExpression(), referencedNames);
    }

    // Resolve names to column handles
    Set<DqeColumnHandle> requiredColumns = new HashSet<>();
    for (DqeColumnHandle col : scope.getColumns()) {
      if (referencedNames.contains(col.getFieldName().toLowerCase())
          || referencedNames.contains(col.getFieldPath().toLowerCase())) {
        requiredColumns.add(col);
      }
    }

    return new RequiredColumns(requiredColumns);
  }

  private void collectColumnReferences(Expression expr, Set<String> names) {
    if (expr instanceof Identifier id) {
      names.add(id.getValue().toLowerCase());
      return;
    }
    if (expr instanceof DereferenceExpression deref) {
      names.add(flattenDereference(deref).toLowerCase());
      return;
    }
    // Recursively process children
    for (Node child : expr.getChildren()) {
      if (child instanceof Expression childExpr) {
        collectColumnReferences(childExpr, names);
      }
    }
  }

  private String flattenDereference(DereferenceExpression deref) {
    if (deref.getBase() instanceof Identifier base && deref.getField().isPresent()) {
      return base.getValue() + "." + deref.getField().get().getValue();
    }
    if (deref.getBase() instanceof DereferenceExpression inner && deref.getField().isPresent()) {
      return flattenDereference(inner) + "." + deref.getField().get().getValue();
    }
    return deref.toString();
  }
}
