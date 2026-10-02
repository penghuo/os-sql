/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.progress;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableList;
import java.math.BigDecimal;
import java.util.OptionalLong;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableLimit;
import org.apache.calcite.adapter.enumerable.EnumerableValues;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.executor.progress.ProgressiveSourceProgress;

/**
 * Plan substitution: which nodes are replaced, and that nothing else about the plan moves.
 *
 * <p>The substitution runs after optimization, so it cannot change which plan was chosen. What it
 * must not do is change the chosen plan's shape, traits, or parameters — and it must not run at all
 * for a synchronous query.
 */
class ProgressLimitInstrumentationTest {

  private RelOptCluster cluster;
  private RexBuilder rexBuilder;
  private ProgressiveQueryContext context;

  @BeforeEach
  void setUp() {
    JavaTypeFactoryImpl typeFactory = new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    rexBuilder = new RexBuilder(typeFactory);
    VolcanoPlanner planner = new VolcanoPlanner();
    planner.addRelTraitDef(org.apache.calcite.plan.ConventionTraitDef.INSTANCE);
    cluster = RelOptCluster.create(planner, rexBuilder);
    context = ProgressiveQueryContext.create(new ProgressiveSourceProgress());
    assertNotNull(context);
  }

  @Test
  @DisplayName("a limit is replaced by its progress-reporting equivalent")
  void limitIsReplaced() {
    EnumerableLimit limit = limit(values(), 0, 2);

    RelNode instrumented = ProgressLimitInstrumentation.instrument(limit, context);

    assertInstanceOf(ProgressAwareEnumerableLimit.class, instrumented);
  }

  @Test
  @DisplayName("the replacement keeps the original's input, offset, fetch, and traits")
  void replacementPreservesEverything() {
    RelNode input = values();
    EnumerableLimit limit = limit(input, 5, 3);

    EnumerableLimit instrumented =
        (EnumerableLimit) ProgressLimitInstrumentation.instrument(limit, context);

    assertSame(input, instrumented.getInput());
    assertEquals(5L, literal(instrumented.offset));
    assertEquals(3L, literal(instrumented.fetch));
    assertEquals(limit.getTraitSet(), instrumented.getTraitSet());
    assertSame(limit.getCluster(), instrumented.getCluster());
    assertEquals(limit.getRowType(), instrumented.getRowType());
  }

  @Test
  @DisplayName("a synchronous query is not instrumented at all")
  void synchronousPlanUntouched() {
    EnumerableLimit limit = limit(values(), 0, 2);
    // No context means no observer, which is every synchronous query. Returning the plan unchanged
    // is what makes it
    // impossible for progress to perturb synchronous plans, costs, or explain output.
    assertSame(limit, ProgressLimitInstrumentation.instrument(limit, null));
  }

  @Test
  @DisplayName("a plan with no limit is returned unchanged")
  void planWithoutLimitUntouched() {
    RelNode plan = values();
    assertSame(plan, ProgressLimitInstrumentation.instrument(plan, context));
  }

  @Test
  @DisplayName("nested limits are both replaced")
  void nestedLimitsBothReplaced() {
    EnumerableLimit inner = limit(values(), 2, 10);
    EnumerableLimit outer = limit(inner, 5, 3);

    RelNode instrumented = ProgressLimitInstrumentation.instrument(outer, context);

    assertInstanceOf(ProgressAwareEnumerableLimit.class, instrumented);
    assertInstanceOf(
        ProgressAwareEnumerableLimit.class,
        instrumented.getInput(0),
        "the inner limit must be instrumented too: each reports the sources beneath itself");
    // Both keep their own parameters; the outer's demand does not rewrite the inner's fetch.
    assertEquals(3L, literal(((EnumerableLimit) instrumented).fetch));
    assertEquals(10L, literal(((EnumerableLimit) instrumented.getInput(0)).fetch));
  }

  @Test
  @DisplayName("an already-instrumented plan is left alone")
  void idempotent() {
    RelNode once = ProgressLimitInstrumentation.instrument(limit(values(), 0, 2), context);
    assertSame(once, ProgressLimitInstrumentation.instrument(once, context));
  }

  @Test
  @DisplayName("a limit below another operator is replaced in place")
  void limitBelowAnotherOperatorIsReplaced() {
    EnumerableLimit limit = limit(values(), 0, 2);
    RelNode above = limit(limit, 0, 1);

    RelNode instrumented = ProgressLimitInstrumentation.instrument(above, context);

    assertTrue(instrumented.getInput(0) instanceof ProgressAwareEnumerableLimit);
  }

  // ---------------------------------------------------------------- helpers

  private RelNode values() {
    RelDataType rowType =
        rexBuilder
            .getTypeFactory()
            .builder()
            .add("id", rexBuilder.getTypeFactory().createSqlType(SqlTypeName.INTEGER))
            .build();
    return EnumerableValues.create(
        cluster,
        rowType,
        ImmutableList.of(
            ImmutableList.of(
                rexBuilder.makeExactLiteral(
                    BigDecimal.ONE, rowType.getFieldList().get(0).getType()))));
  }

  private EnumerableLimit limit(RelNode input, int offset, int fetch) {
    return new EnumerableLimit(
        cluster,
        cluster.traitSetOf(EnumerableConvention.INSTANCE),
        input,
        rexBuilder.makeExactLiteral(BigDecimal.valueOf(offset)),
        rexBuilder.makeExactLiteral(BigDecimal.valueOf(fetch)));
  }

  private static long literal(org.apache.calcite.rex.RexNode node) {
    OptionalLong value =
        node instanceof RexLiteral literal
            ? OptionalLong.of(literal.getValueAs(Long.class))
            : OptionalLong.empty();
    assertTrue(value.isPresent(), "expected a literal, got " + node);
    return value.getAsLong();
  }
}
