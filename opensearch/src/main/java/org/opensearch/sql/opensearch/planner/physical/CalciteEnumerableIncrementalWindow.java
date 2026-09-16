/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.planner.physical;

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.adapter.enumerable.EnumUtils;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.adapter.enumerable.EnumerableWindow;
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
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Window whose live state is authoritative for partial and final output. */
public class CalciteEnumerableIncrementalWindow extends Window implements EnumerableRel {
  public CalciteEnumerableIncrementalWindow(EnumerableWindow window) {
    this(
        window.getTraitSet(),
        window.getInput(),
        window.getConstants(),
        window.getRowType(),
        window.groups);
  }

  private CalciteEnumerableIncrementalWindow(
      RelTraitSet traitSet,
      RelNode input,
      List<RexLiteral> constants,
      RelDataType rowType,
      List<Group> groups) {
    super(input.getCluster(), traitSet, input, constants, rowType, groups);
  }

  public static boolean supports(EnumerableWindow window) {
    if (window.groups.size() != 1) {
      return false;
    }
    Group group = window.groups.getFirst();
    List<AggregateCall> calls = group.getAggregateCalls(window);
    return group.orderKeys.getFieldCollations().isEmpty()
        && group.lowerBound.isUnboundedPreceding()
        && (group.upperBound.isUnboundedFollowing() || group.upperBound.isCurrentRow())
        && IncrementalEnumerableOperators.supports(
            calls, window.getInput().getRowType().getFieldCount());
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new CalciteEnumerableIncrementalWindow(
        traitSet, inputs.getFirst(), constants, getRowType(), groups);
  }

  @Override
  public Window copy(List<RexLiteral> constants) {
    return new CalciteEnumerableIncrementalWindow(
        getTraitSet(), getInput(), constants, getRowType(), groups);
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
    Expression operator = implementor.stash(this, CalciteEnumerableIncrementalWindow.class);
    PhysType outputType =
        PhysTypeImpl.of(implementor.getTypeFactory(), getRowType(), JavaRowFormat.ARRAY, false);
    builder.add(Expressions.return_(null, Expressions.call(operator, "execute", arrayInput)));
    return implementor.result(outputType, builder.toBlock());
  }

  public Enumerable<Object[]> execute(Enumerable<Object[]> input) {
    Group group = groups.getFirst();
    List<AggregateCall> calls = group.getAggregateCalls(this);
    return group.upperBound.isCurrentRow()
        ? IncrementalEnumerableOperators.runningWindow(
            this, input, getRowType(), group.keys.asList(), calls)
        : IncrementalEnumerableOperators.eventStats(
            this, input, getRowType(), group.keys.asList(), calls);
  }

  @Override
  public @Nullable RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    return IncrementalEnumerableOperators.progressiveCost(super.computeSelfCost(planner, mq));
  }
}
