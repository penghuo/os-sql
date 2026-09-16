/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.planner.physical;

import java.util.List;
import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.enumerable.EnumerableCalc;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.adapter.enumerable.JavaRowFormat;
import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.adapter.enumerable.PhysTypeImpl;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.function.Predicate1;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexSimplify;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Row-local Calc that propagates keyed before/after changes from an incremental child.
 *
 * <p>The generated predicate and projector are the same Rex expressions used by Calcite's normal
 * {@link EnumerableCalc}. The runtime applies them independently to the before and after images,
 * which naturally converts filter entry/exit into INSERT/DELETE changes while preserving row
 * identity.
 */
public class CalciteEnumerableIncrementalCalc extends Calc implements EnumerableRel {

  public CalciteEnumerableIncrementalCalc(EnumerableCalc calc) {
    this(calc.getTraitSet(), calc.getInput(), calc.getProgram());
  }

  public CalciteEnumerableIncrementalCalc(EnumerableCalc calc, RelNode input) {
    this(calc.getTraitSet(), input, calc.getProgram());
  }

  private CalciteEnumerableIncrementalCalc(
      RelTraitSet traitSet, RelNode input, RexProgram program) {
    super(input.getCluster(), traitSet, input, program);
  }

  @Override
  public CalciteEnumerableIncrementalCalc copy(
      RelTraitSet traitSet, RelNode child, RexProgram program) {
    return new CalciteEnumerableIncrementalCalc(traitSet, child, program);
  }

  @Override
  public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
    JavaTypeFactory typeFactory = implementor.getTypeFactory();
    EnumerableRel child = (EnumerableRel) getInput();
    Result childResult = implementor.visitChild(this, 0, child, Prefer.ARRAY);
    PhysType outputType = PhysTypeImpl.of(typeFactory, getRowType(), JavaRowFormat.ARRAY, false);
    BlockBuilder builder = new BlockBuilder();
    Expression inputEnumerable = builder.append("inputEnumerable", childResult.block, false);
    ParameterExpression inputRow =
        Expressions.parameter(childResult.physType.getJavaRowType(), "inputRow");

    RexBuilder rexBuilder = getCluster().getRexBuilder();
    RelOptPredicateList predicates = getCluster().getMetadataQuery().getPulledUpPredicates(child);
    RexProgram normalizedProgram =
        program.normalize(rexBuilder, new RexSimplify(rexBuilder, predicates, RexUtil.EXECUTOR));
    RexToLixTranslator.InputGetter inputGetter =
        new RexToLixTranslator.InputGetterImpl(inputRow, childResult.physType);
    Function1<String, RexToLixTranslator.InputGetter> correlateVariables =
        implementor::getCorrelVariableGetter;

    Expression predicate;
    if (normalizedProgram.getCondition() == null) {
      predicate = Expressions.lambda(Predicate1.class, Expressions.constant(true), inputRow);
    } else {
      BlockBuilder predicateBuilder = new BlockBuilder();
      Expression condition =
          RexToLixTranslator.translateCondition(
              normalizedProgram,
              typeFactory,
              predicateBuilder,
              inputGetter,
              correlateVariables,
              implementor.getConformance());
      predicateBuilder.add(Expressions.return_(null, condition));
      predicate = Expressions.lambda(Predicate1.class, predicateBuilder.toBlock(), inputRow);
    }

    BlockBuilder projectBuilder = new BlockBuilder();
    SqlConformance conformance =
        (SqlConformance) implementor.map.getOrDefault("_conformance", SqlConformanceEnum.DEFAULT);
    List<Expression> projects =
        RexToLixTranslator.translateProjects(
            normalizedProgram,
            typeFactory,
            conformance,
            projectBuilder,
            null,
            outputType,
            DataContext.ROOT,
            inputGetter,
            correlateVariables);
    projectBuilder.add(Expressions.return_(null, outputType.record(projects)));
    Expression projector = Expressions.lambda(Function1.class, projectBuilder.toBlock(), inputRow);

    Expression operator = implementor.stash(this, CalciteEnumerableIncrementalCalc.class);
    builder.add(
        Expressions.return_(
            null, Expressions.call(operator, "execute", inputEnumerable, predicate, projector)));
    return implementor.result(outputType, builder.toBlock());
  }

  public Enumerable<Object[]> execute(
      Enumerable<Object[]> input,
      Predicate1<Object[]> predicate,
      Function1<Object[], Object[]> projector) {
    return IncrementalEnumerableOperators.calc(this, input, getRowType(), predicate, projector);
  }

  @Override
  public @Nullable RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    return IncrementalEnumerableOperators.progressiveCost(super.computeSelfCost(planner, mq));
  }
}
