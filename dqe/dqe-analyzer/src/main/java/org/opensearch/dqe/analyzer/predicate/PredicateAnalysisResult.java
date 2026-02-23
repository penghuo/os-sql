/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.analyzer.predicate;

import java.util.List;
import org.opensearch.dqe.analyzer.type.TypedExpression;

/** Result of predicate analysis: separated into pushable and residual predicates. */
public class PredicateAnalysisResult {

  private final List<PushdownPredicate> pushdownPredicates;
  private final List<TypedExpression> residualPredicates;

  public PredicateAnalysisResult(
      List<PushdownPredicate> pushdownPredicates, List<TypedExpression> residualPredicates) {
    this.pushdownPredicates = List.copyOf(pushdownPredicates);
    this.residualPredicates = List.copyOf(residualPredicates);
  }

  /** Predicates that can be converted to Query DSL and pushed to OpenSearch. */
  public List<PushdownPredicate> getPushdownPredicates() {
    return pushdownPredicates;
  }

  /** Predicates that must be evaluated as post-filter operators. */
  public List<TypedExpression> getResidualPredicates() {
    return residualPredicates;
  }

  /** Returns true if there are no residual predicates (full pushdown). */
  public boolean isFullyPushedDown() {
    return residualPredicates.isEmpty();
  }
}
