/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.planner.physical;

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.adapter.enumerable.EnumUtils;
import org.apache.calcite.adapter.enumerable.EnumerableAggregate;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.adapter.enumerable.JavaRowFormat;
import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.adapter.enumerable.PhysTypeImpl;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.ImmutableBitSet;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Aggregate whose live state is authoritative for both partial and final output. */
public class CalciteEnumerableIncrementalAggregate extends EnumerableAggregate {
  public CalciteEnumerableIncrementalAggregate(EnumerableAggregate aggregate)
      throws InvalidRelException {
    this(
        aggregate.getTraitSet(),
        aggregate.getInput(),
        aggregate.getGroupSet(),
        aggregate.getGroupSets(),
        aggregate.getAggCallList());
  }

  private CalciteEnumerableIncrementalAggregate(
      RelTraitSet traitSet,
      RelNode input,
      ImmutableBitSet groupSet,
      @Nullable List<ImmutableBitSet> groupSets,
      List<AggregateCall> aggCalls)
      throws InvalidRelException {
    super(input.getCluster(), traitSet, input, groupSet, groupSets, aggCalls);
  }

  public static boolean supports(EnumerableAggregate aggregate) {
    return aggregate.getGroupType() == Aggregate.Group.SIMPLE
        && IncrementalEnumerableOperators.supports(
            aggregate.getAggCallList(), aggregate.getInput().getRowType().getFieldCount());
  }

  @Override
  public CalciteEnumerableIncrementalAggregate copy(
      RelTraitSet traitSet,
      RelNode input,
      ImmutableBitSet groupSet,
      @Nullable List<ImmutableBitSet> groupSets,
      List<AggregateCall> aggCalls) {
    try {
      return new CalciteEnumerableIncrementalAggregate(
          traitSet, input, groupSet, groupSets, aggCalls);
    } catch (InvalidRelException e) {
      throw new AssertionError(e);
    }
  }

  @Override
  public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
    EnumerableRel child = (EnumerableRel) getInput();
    Result childResult = implementor.visitChild(this, 0, child, Prefer.ARRAY);
    BlockBuilder builder = new BlockBuilder();
    Expression childExp = builder.append("child", childResult.block);

    ParameterExpression inputRow =
        Expressions.parameter(childResult.physType.getJavaRowType(), "inputRow");
    List<Expression> fields = new ArrayList<>(getInput().getRowType().getFieldCount());
    for (int i = 0; i < getInput().getRowType().getFieldCount(); i++) {
      fields.add(EnumUtils.convert(childResult.physType.fieldReference(inputRow, i), Object.class));
    }
    Expression arrayInput =
        builder.append(
            "arrayInput",
            Expressions.call(
                IncrementalEnumerableOperators.class,
                "projectRows",
                childExp,
                Expressions.lambda(
                    Function1.class, Expressions.newArrayInit(Object.class, fields), inputRow)));
    Expression operator = implementor.stash(this, CalciteEnumerableIncrementalAggregate.class);
    PhysType outputType =
        PhysTypeImpl.of(implementor.getTypeFactory(), getRowType(), JavaRowFormat.ARRAY, false);
    builder.add(Expressions.return_(null, Expressions.call(operator, "execute", arrayInput)));
    return implementor.result(outputType, builder.toBlock());
  }

  public Enumerable<Object[]> execute(Enumerable<Object[]> input) {
    return IncrementalEnumerableOperators.aggregate(
        this, input, getRowType(), getGroupSet().asList(), getAggCallList());
  }

  @Override
  public @Nullable RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    return IncrementalEnumerableOperators.progressiveCost(super.computeSelfCost(planner, mq));
  }
}
