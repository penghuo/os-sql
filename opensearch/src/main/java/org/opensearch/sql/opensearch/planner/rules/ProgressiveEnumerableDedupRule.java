/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.planner.rules;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.opensearch.sql.calcite.plan.ProgressivePlanningContext;
import org.opensearch.sql.calcite.plan.rel.LogicalDedup;
import org.opensearch.sql.opensearch.executor.ProgressiveQueryContext;
import org.opensearch.sql.opensearch.planner.physical.CalciteEnumerableIncrementalDedup;

/** Converts a supported logical dedup directly to one live incremental operator for async PPL. */
public class ProgressiveEnumerableDedupRule extends ConverterRule {
  public static final Config DEFAULT_CONFIG =
      Config.INSTANCE
          .withConversion(
              LogicalDedup.class,
              Convention.NONE,
              EnumerableConvention.INSTANCE,
              "ProgressiveEnumerableDedupRule")
          .withRuleFactory(ProgressiveEnumerableDedupRule::new);

  protected ProgressiveEnumerableDedupRule(Config config) {
    super(config);
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    LogicalDedup dedup = call.rel(0);
    return (ProgressivePlanningContext.isActive() || ProgressiveQueryContext.isActive())
        && CalciteEnumerableIncrementalDedup.supports(dedup);
  }

  @Override
  public RelNode convert(RelNode rel) {
    LogicalDedup dedup = (LogicalDedup) rel;
    RelTraitSet inputTraits = dedup.getInput().getTraitSet().replace(EnumerableConvention.INSTANCE);
    if (dedup.getInputCollation() != null) {
      inputTraits = inputTraits.replace(dedup.getInputCollation());
    }
    RelNode input = convert(dedup.getInput(), inputTraits);
    if (input == null) {
      return null;
    }
    RelTraitSet outputTraits = dedup.getTraitSet().replace(EnumerableConvention.INSTANCE);
    if (dedup.getInputCollation() != null) {
      outputTraits = outputTraits.replace(dedup.getInputCollation());
    }
    return new CalciteEnumerableIncrementalDedup(
        outputTraits,
        input,
        dedup.getDedupeFields(),
        dedup.getAllowedDuplication(),
        dedup.getKeepEmpty(),
        dedup.getConsecutive(),
        dedup.getInputCollation(),
        dedup.getInputCollationFieldNames());
  }
}
