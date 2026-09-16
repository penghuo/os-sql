/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.planner.rules;

import org.apache.calcite.adapter.enumerable.EnumerableWindow;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.opensearch.sql.calcite.plan.ProgressivePlanningContext;
import org.opensearch.sql.opensearch.executor.ProgressiveQueryContext;
import org.opensearch.sql.opensearch.planner.physical.CalciteEnumerableIncrementalWindow;

/** Replaces supported eventstats windows only for an active progressive async execution. */
@SuppressWarnings("deprecation")
public final class ProgressiveEnumerableWindowRule extends RelOptRule {
  public static final ProgressiveEnumerableWindowRule INSTANCE =
      new ProgressiveEnumerableWindowRule();

  private ProgressiveEnumerableWindowRule() {
    super(
        operand(
            EnumerableWindow.class,
            null,
            window ->
                (ProgressivePlanningContext.isActive() || ProgressiveQueryContext.isActive())
                    && CalciteEnumerableIncrementalWindow.supports(window),
            any()),
        "ProgressiveEnumerableWindowRule");
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    call.transformTo(new CalciteEnumerableIncrementalWindow(call.rel(0)));
  }
}
