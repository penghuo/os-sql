/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.planner.rules;

import org.apache.calcite.adapter.enumerable.EnumerableCalc;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalCalc;
import org.opensearch.sql.calcite.plan.ProgressivePlanningContext;
import org.opensearch.sql.opensearch.executor.ProgressiveQueryContext;
import org.opensearch.sql.opensearch.planner.physical.CalciteEnumerableIncrementalCalc;

/**
 * Converts every async row-local Calc directly to its changelog-preserving Enumerable form.
 *
 * <p>Doing this at the logical-to-Enumerable boundary makes composition independent of Volcano rule
 * firing order. With an ordinary child the operator has exactly the normal Calc behavior; with an
 * incremental child it propagates keyed insert/update/delete changes.
 */
public final class ProgressiveEnumerableCalcRule extends ConverterRule {
  public static final Config DEFAULT_CONFIG =
      Config.INSTANCE
          .withConversion(
              LogicalCalc.class,
              Convention.NONE,
              EnumerableConvention.INSTANCE,
              "ProgressiveEnumerableCalcRule")
          .withRuleFactory(ProgressiveEnumerableCalcRule::new);

  public static final ProgressiveEnumerableCalcRule INSTANCE =
      new ProgressiveEnumerableCalcRule(DEFAULT_CONFIG);

  private ProgressiveEnumerableCalcRule(Config config) {
    super(config);
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    return ProgressivePlanningContext.isActive() || ProgressiveQueryContext.isActive();
  }

  @Override
  public RelNode convert(RelNode rel) {
    LogicalCalc calc = (LogicalCalc) rel;
    RelTraitSet inputTraits = calc.getInput().getTraitSet().replace(EnumerableConvention.INSTANCE);
    RelNode input = convert(calc.getInput(), inputTraits);
    if (input == null) {
      return null;
    }
    return new CalciteEnumerableIncrementalCalc(EnumerableCalc.create(input, calc.getProgram()));
  }
}
