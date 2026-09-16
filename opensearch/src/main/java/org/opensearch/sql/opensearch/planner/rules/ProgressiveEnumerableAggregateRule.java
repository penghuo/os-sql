/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.planner.rules;

import org.apache.calcite.adapter.enumerable.EnumerableAggregate;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.InvalidRelException;
import org.opensearch.sql.calcite.plan.ProgressivePlanningContext;
import org.opensearch.sql.opensearch.executor.ProgressiveQueryContext;
import org.opensearch.sql.opensearch.planner.physical.CalciteEnumerableIncrementalAggregate;

/** Replaces supported aggregates only for an active progressive async execution. */
@SuppressWarnings("deprecation")
public final class ProgressiveEnumerableAggregateRule extends RelOptRule {
  public static final ProgressiveEnumerableAggregateRule INSTANCE =
      new ProgressiveEnumerableAggregateRule();

  private ProgressiveEnumerableAggregateRule() {
    super(
        operand(
            EnumerableAggregate.class,
            null,
            aggregate ->
                !(aggregate instanceof CalciteEnumerableIncrementalAggregate)
                    && (ProgressivePlanningContext.isActive() || ProgressiveQueryContext.isActive())
                    && CalciteEnumerableIncrementalAggregate.supports(aggregate),
            any()),
        "ProgressiveEnumerableAggregateRule");
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    try {
      call.transformTo(new CalciteEnumerableIncrementalAggregate(call.rel(0)));
    } catch (InvalidRelException e) {
      throw new IllegalStateException("Failed to build incremental aggregate", e);
    }
  }
}
