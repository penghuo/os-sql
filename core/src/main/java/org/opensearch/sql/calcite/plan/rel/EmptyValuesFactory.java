/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.plan.rel;

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.metadata.RelMdDistribution;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;

/**
 * {@link RelFactories.ValuesFactory} that creates an empty {@link LogicalValues} without deriving
 * its collation trait.
 *
 * <p>{@link LogicalValues#create} derives the collation trait of an empty {@code Values} with
 * {@code RelMdCollation.values}, which builds n(n+1)/2 field collations for n columns and keeps
 * them on the node. {@code RelBuilder.empty()} creates such a node over the full row type whenever
 * a filter folds to FALSE, so on a wide index each one costs O(n^2) time and memory. Non-empty
 * values are created by Calcite's default factory.
 */
public final class EmptyValuesFactory implements RelFactories.ValuesFactory {

  public static final EmptyValuesFactory INSTANCE = new EmptyValuesFactory();

  /** Plain {@link RelBuilder} factory that creates values through {@link #INSTANCE}. */
  public static final RelBuilderFactory LOGICAL_BUILDER = RelBuilder.proto(Contexts.of(INSTANCE));

  private EmptyValuesFactory() {}

  @Override
  public RelNode createValues(
      RelOptCluster cluster, RelDataType rowType, List<ImmutableList<RexLiteral>> tuples) {
    if (!tuples.isEmpty()) {
      return RelFactories.DEFAULT_VALUES_FACTORY.createValues(cluster, rowType, tuples);
    }
    ImmutableList<ImmutableList<RexLiteral>> noTuples = ImmutableList.of();
    return new LogicalValues(
        cluster,
        cluster
            .traitSetOf(Convention.NONE)
            .replaceIf(
                RelDistributionTraitDef.INSTANCE,
                () -> RelMdDistribution.values(rowType, noTuples)),
        rowType,
        noTuples);
  }
}
