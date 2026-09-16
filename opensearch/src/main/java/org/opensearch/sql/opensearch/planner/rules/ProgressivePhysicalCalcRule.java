/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.planner.rules;

import org.apache.calcite.adapter.enumerable.EnumerableCalc;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.opensearch.sql.calcite.plan.ProgressivePlanningContext;
import org.opensearch.sql.opensearch.executor.ProgressiveQueryContext;
import org.opensearch.sql.opensearch.planner.physical.CalciteEnumerableIncrementalCalc;

/** Replaces async EnumerableCalc nodes created by late Project/Filter fusion rules. */
@SuppressWarnings("deprecation")
public final class ProgressivePhysicalCalcRule extends RelOptRule {
  public static final ProgressivePhysicalCalcRule INSTANCE = new ProgressivePhysicalCalcRule();

  private ProgressivePhysicalCalcRule() {
    super(
        operand(
            EnumerableCalc.class,
            null,
            calc -> ProgressivePlanningContext.isActive() || ProgressiveQueryContext.isActive(),
            any()),
        "ProgressivePhysicalCalcRule");
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    call.transformTo(new CalciteEnumerableIncrementalCalc(call.rel(0)));
  }
}
