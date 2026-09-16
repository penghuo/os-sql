/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.planner.physical;

import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.calcite.adapter.enumerable.EnumUtils;
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
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.opensearch.sql.calcite.plan.rel.Dedup;

/** Enumerable dedup that retains accepted rows as its live partial/final state. */
public class CalciteEnumerableIncrementalDedup extends Dedup implements EnumerableRel {

  public CalciteEnumerableIncrementalDedup(
      RelTraitSet traitSet,
      RelNode input,
      List<RexNode> dedupeFields,
      Integer allowedDuplication,
      Boolean keepEmpty,
      Boolean consecutive,
      @Nullable RelCollation inputCollation,
      @Nullable List<String> inputCollationFieldNames) {
    super(
        input.getCluster(),
        traitSet,
        input,
        dedupeFields,
        allowedDuplication,
        keepEmpty,
        consecutive,
        inputCollation,
        inputCollationFieldNames);
  }

  public static boolean supports(Dedup dedup) {
    return !dedup.getConsecutive()
        && dedup.getDedupeFields().stream().allMatch(RexInputRef.class::isInstance);
  }

  @Override
  public Dedup copy(
      RelTraitSet traitSet,
      RelNode input,
      List<RexNode> dedupeFields,
      Integer allowedDuplication,
      Boolean keepEmpty,
      Boolean consecutive,
      @Nullable RelCollation inputCollation,
      @Nullable List<String> inputCollationFieldNames) {
    return new CalciteEnumerableIncrementalDedup(
        traitSet,
        input,
        dedupeFields,
        allowedDuplication,
        keepEmpty,
        consecutive,
        inputCollation,
        inputCollationFieldNames);
  }

  @Override
  public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
    EnumerableRel child = (EnumerableRel) getInput();
    Result childResult = implementor.visitChild(this, 0, child, Prefer.ARRAY);
    BlockBuilder builder = new BlockBuilder();
    Expression childExp = builder.append("child", childResult.block);

    ParameterExpression inputRow =
        Expressions.parameter(childResult.physType.getJavaRowType(), "inputRow");
    List<Expression> fields = new ArrayList<>(getRowType().getFieldCount());
    for (int i = 0; i < getRowType().getFieldCount(); i++) {
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
    Expression operator = implementor.stash(this, CalciteEnumerableIncrementalDedup.class);
    PhysType outputType =
        PhysTypeImpl.of(implementor.getTypeFactory(), getRowType(), JavaRowFormat.ARRAY, false);
    builder.add(Expressions.return_(null, Expressions.call(operator, "execute", arrayInput)));
    return implementor.result(outputType, builder.toBlock());
  }

  public Enumerable<Object[]> execute(Enumerable<Object[]> input) {
    List<Integer> keyOrdinals =
        getDedupeFields().stream().map(field -> ((RexInputRef) field).getIndex()).toList();
    return IncrementalEnumerableOperators.dedup(
        this, input, getRowType(), keyOrdinals, getAllowedDuplication(), getKeepEmpty());
  }

  @Override
  public @Nullable RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    return IncrementalEnumerableOperators.progressiveCost(super.computeSelfCost(planner, mq));
  }
}
