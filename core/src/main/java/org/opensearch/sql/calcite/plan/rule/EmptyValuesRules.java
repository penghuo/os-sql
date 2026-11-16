/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.plan.rule;

import com.google.common.collect.ImmutableList;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.adapter.enumerable.EnumerableValues;
import org.apache.calcite.adapter.enumerable.EnumerableValuesRule;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.rules.PruneEmptyRules;
import org.apache.calcite.rel.type.RelDataType;
import org.opensearch.sql.calcite.plan.rel.EmptyValuesFactory;

/**
 * Volcano rules that handle an empty {@link Values} without deriving its collation trait.
 *
 * <p>Calcite derives the collation trait of an empty {@code Values} with {@code
 * RelMdCollation.values}, which builds n(n+1)/2 field collations for n columns. The default {@link
 * PruneEmptyRules} create a new empty {@code Values} for every operator they remove, and {@link
 * EnumerableValuesRule} derives the trait again while converting it, so on a wide index each step
 * costs O(n^2). This class swaps those rules for versions that skip the derivation.
 */
public final class EmptyValuesRules {

  /** Default {@link PruneEmptyRules} registered by Calcite, keyed to their replacements. */
  private static final Map<RelOptRule, RelOptRule> PRUNE_EMPTY_RULES =
      List.of(
              PruneEmptyRules.UNION_INSTANCE,
              PruneEmptyRules.INTERSECT_INSTANCE,
              PruneEmptyRules.MINUS_INSTANCE,
              PruneEmptyRules.PROJECT_INSTANCE,
              PruneEmptyRules.FILTER_INSTANCE,
              PruneEmptyRules.SORT_INSTANCE,
              PruneEmptyRules.AGGREGATE_INSTANCE,
              PruneEmptyRules.WINDOW_INSTANCE,
              PruneEmptyRules.JOIN_LEFT_INSTANCE,
              PruneEmptyRules.JOIN_RIGHT_INSTANCE,
              PruneEmptyRules.SORT_FETCH_ZERO_INSTANCE,
              PruneEmptyRules.EMPTY_TABLE_INSTANCE)
          .stream()
          .collect(
              Collectors.toUnmodifiableMap(
                  Function.identity(),
                  rule ->
                      ((RelRule<?>) rule)
                          .config
                          .withRelBuilderFactory(EmptyValuesFactory.LOGICAL_BUILDER)
                          .toRule()));

  /** Converts an empty {@code LogicalValues} to {@link EnumerableValues}. */
  public static final RelOptRule ENUMERABLE_VALUES_RULE =
      EnumerableValuesRule.DEFAULT_CONFIG
          .withRuleFactory(EnumerableEmptyValuesRule::new)
          .toRule(EnumerableEmptyValuesRule.class);

  private EmptyValuesRules() {}

  /** Replaces the planner's default rules that create or convert an empty {@code Values}. */
  public static void register(RelOptPlanner planner) {
    PRUNE_EMPTY_RULES.forEach(
        (original, replacement) -> {
          if (planner.removeRule(original)) {
            planner.addRule(replacement);
          }
        });
    if (planner.removeRule(EnumerableRules.ENUMERABLE_VALUES_RULE)) {
      planner.addRule(ENUMERABLE_VALUES_RULE);
    }
  }

  /**
   * {@link EnumerableValuesRule} that converts an empty {@code Values} by copying its traits
   * instead of deriving them with {@link EnumerableValues#create}. The default rule discards the
   * derived traits anyway and keeps the logical node's traits, so the converted node is the same.
   */
  public static class EnumerableEmptyValuesRule extends EnumerableValuesRule {

    /** The private {@link EnumerableValues} constructor, which takes the trait set as is. */
    private static final MethodHandle NEW_ENUMERABLE_VALUES = enumerableValuesConstructor();

    protected EnumerableEmptyValuesRule(Config config) {
      super(config);
    }

    @Override
    public RelNode convert(RelNode rel) {
      final Values values = (Values) rel;
      if (!values.getTuples().isEmpty()) {
        return super.convert(rel);
      }
      try {
        return (RelNode)
            NEW_ENUMERABLE_VALUES.invoke(
                values.getCluster(),
                values.getRowType(),
                values.getTuples(),
                values.getTraitSet().replace(EnumerableConvention.INSTANCE));
      } catch (Throwable e) {
        throw new IllegalStateException("Failed to convert an empty Values", e);
      }
    }

    private static MethodHandle enumerableValuesConstructor() {
      try {
        return MethodHandles.privateLookupIn(EnumerableValues.class, MethodHandles.lookup())
            .findConstructor(
                EnumerableValues.class,
                MethodType.methodType(
                    void.class,
                    RelOptCluster.class,
                    RelDataType.class,
                    ImmutableList.class,
                    RelTraitSet.class));
      } catch (ReflectiveOperationException e) {
        throw new ExceptionInInitializerError(e);
      }
    }
  }
}
