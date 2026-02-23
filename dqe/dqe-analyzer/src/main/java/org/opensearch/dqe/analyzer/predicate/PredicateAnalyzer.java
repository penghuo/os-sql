/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.analyzer.predicate;

import io.trino.sql.tree.Expression;
import io.trino.sql.tree.LogicalExpression;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.opensearch.dqe.analyzer.scope.Scope;
import org.opensearch.dqe.analyzer.type.TypedExpression;

/**
 * Analyzes WHERE clause expressions to classify which predicates can be pushed down to OpenSearch
 * Query DSL and which must remain as post-filter operators.
 */
public class PredicateAnalyzer {

  private final PushdownClassifier classifier;

  public PredicateAnalyzer() {
    this.classifier = new PushdownClassifier();
  }

  /**
   * Analyzes a WHERE clause expression and splits into pushdown-eligible and residual (post-filter)
   * predicates.
   *
   * @param whereClause the type-checked WHERE clause expression
   * @param scope the current scope
   * @return the analysis result with pushdown and residual predicates
   */
  public PredicateAnalysisResult analyze(TypedExpression whereClause, Scope scope) {
    List<PushdownPredicate> pushdown = new ArrayList<>();
    List<TypedExpression> residual = new ArrayList<>();

    Expression expr = whereClause.getExpression();

    // For top-level AND, split individual conjuncts
    if (expr instanceof LogicalExpression logical
        && logical.getOperator() == LogicalExpression.Operator.AND) {
      for (Expression term : logical.getTerms()) {
        TypedExpression typedTerm = new TypedExpression(term, whereClause.getType());
        Optional<PushdownPredicate> classified = classifier.classify(typedTerm, scope);
        if (classified.isPresent()) {
          pushdown.add(classified.get());
        } else {
          residual.add(typedTerm);
        }
      }
    } else {
      // Try to classify the entire expression
      Optional<PushdownPredicate> classified = classifier.classify(whereClause, scope);
      if (classified.isPresent()) {
        pushdown.add(classified.get());
      } else {
        residual.add(whereClause);
      }
    }

    return new PredicateAnalysisResult(pushdown, residual);
  }
}
