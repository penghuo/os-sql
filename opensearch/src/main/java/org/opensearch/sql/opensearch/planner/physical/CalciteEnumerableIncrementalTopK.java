/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.planner.physical;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
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
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Pair;

/** TopK whose bounded heap is authoritative for both partial snapshots and final output. */
public class CalciteEnumerableIncrementalTopK extends CalciteEnumerableTopK {

  private CalciteEnumerableIncrementalTopK(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode input,
      RelCollation collation,
      RexNode offset,
      RexNode fetch) {
    super(cluster, traitSet, input, collation, offset, fetch);
  }

  public static CalciteEnumerableIncrementalTopK create(
      RelNode input, RelCollation collation, RexNode offset, RexNode fetch) {
    CalciteEnumerableTopK topK = CalciteEnumerableTopK.create(input, collation, offset, fetch);
    return new CalciteEnumerableIncrementalTopK(
        topK.getCluster(),
        topK.getTraitSet(),
        topK.getInput(),
        topK.getCollation(),
        topK.offset,
        topK.fetch);
  }

  @Override
  public CalciteEnumerableIncrementalTopK copy(
      RelTraitSet traitSet,
      RelNode newInput,
      RelCollation newCollation,
      RexNode offset,
      RexNode fetch) {
    return new CalciteEnumerableIncrementalTopK(
        getCluster(), traitSet, newInput, newCollation, offset, fetch);
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
    PhysType outputType =
        PhysTypeImpl.of(implementor.getTypeFactory(), getRowType(), JavaRowFormat.ARRAY, false);
    Pair<Expression, Expression> collationKey =
        outputType.generateCollationKey(collation.getFieldCollations());
    Expression keySelector = builder.append("keySelector", collationKey.left);
    Expression comparator =
        collationKey.right == null
            ? Expressions.constant(null, Comparator.class)
            : builder.append("comparator", collationKey.right);
    Expression operator = implementor.stash(this, CalciteEnumerableIncrementalTopK.class);
    builder.add(
        Expressions.return_(
            null,
            Expressions.call(
                operator,
                "execute",
                arrayInput,
                keySelector,
                comparator,
                Expressions.constant(literalInt(offset, 0)),
                Expressions.constant(literalInt(fetch, Integer.MAX_VALUE)))));
    return implementor.result(outputType, builder.toBlock());
  }

  public Enumerable<Object[]> execute(
      Enumerable<Object[]> input,
      Function1<Object[], Object> keySelector,
      Comparator<Object> comparator,
      int offset,
      int fetch) {
    return IncrementalEnumerableOperators.topK(
        this, input, getRowType(), keySelector, comparator, offset, fetch);
  }

  private static int literalInt(RexNode node, int defaultValue) {
    if (!(node instanceof RexLiteral literal)) {
      return defaultValue;
    }
    Integer value = literal.getValueAs(Integer.class);
    return value == null || value < 0 ? defaultValue : value;
  }
}
