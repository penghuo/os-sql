/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.plan;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.calcite.adapter.enumerable.EnumerableLimit;
import org.apache.calcite.adapter.java.ReflectiveSchema;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.calcite.utils.CalciteToolsHelper;

/**
 * The physical-plan callback, exercised through the real Calcite prepare and code-generation path.
 *
 * <h2>What this is guarding</h2>
 *
 * The plan the execution engine hands to the runner is logical: a {@code head} is a {@code
 * LogicalSort}, and only the planner inside statement preparation turns it into an {@code
 * EnumerableLimit}. An earlier integration attempt transformed the plan before handing it over and
 * therefore matched nothing at all, while every wrapper-level test and every "terminal value is
 * 1.0" test still passed. So the assertions here are specifically:
 *
 * <ul>
 *   <li>the callback is invoked with a plan that already contains {@code EnumerableLimit} — proving
 *       it runs after physical conversion rather than before it;
 *   <li>a replacement the callback returns is the node that actually executes — proving the
 *       transformed tree is the one code generation and {@code PLAN_BEFORE_IMPLEMENTATION} see;
 *   <li>the replacement observes the limit's rows at the moment the limit emits them, not at end of
 *       execution.
 * </ul>
 */
class PhysicalPlanHookTest {

  /** Row type for the in-memory table the plans below read. */
  public static final class Row {
    public final int id;

    public Row(int id) {
      this.id = id;
    }
  }

  /** Schema exposing a single ten-row table. */
  public static final class TestSchema {
    public final Row[] rows = {
      new Row(1), new Row(2), new Row(3), new Row(4), new Row(5),
      new Row(6), new Row(7), new Row(8), new Row(9), new Row(10)
    };
  }

  @Test
  @DisplayName("nothing is installed by default, so a plan passes through untouched")
  void withoutInstallationPlanIsUntouched() {
    RelNode plan = planWithLimit(2);
    assertSame(plan, PhysicalPlanHook.apply(plan));
  }

  @Test
  @DisplayName("a scope restores the previous transform and leaves the thread clean")
  void scopesRestore() {
    RelNode plan = planWithLimit(2);
    AtomicInteger outer = new AtomicInteger();
    try (PhysicalPlanHook.Scope first =
        PhysicalPlanHook.install(
            p -> {
              outer.incrementAndGet();
              return p;
            })) {
      AtomicInteger inner = new AtomicInteger();
      try (PhysicalPlanHook.Scope second =
          PhysicalPlanHook.install(
              p -> {
                inner.incrementAndGet();
                return p;
              })) {
        PhysicalPlanHook.apply(plan);
        assertEquals(1, inner.get());
        assertEquals(0, outer.get());
      }
      PhysicalPlanHook.apply(plan);
      assertEquals(1, outer.get());
    }
    assertSame(plan, PhysicalPlanHook.apply(plan));
  }

  @Test
  @DisplayName("a transform returning null is ignored rather than erasing the plan")
  void nullTransformResultIgnored() {
    RelNode plan = planWithLimit(2);
    try (PhysicalPlanHook.Scope scope = PhysicalPlanHook.install(p -> null)) {
      assertSame(plan, PhysicalPlanHook.apply(plan));
    }
  }

  // ------------------------------------------------------------------ the production gate

  @Test
  @DisplayName("the callback sees a physical plan containing EnumerableLimit, not the logical plan")
  void callbackRunsAfterPhysicalConversion() throws Exception {
    // The logical plan the engine would hand over has no EnumerableLimit anywhere. This is what a
    // pre-conversion
    // rewrite saw, and why it matched nothing.
    RelNode logical = planWithLimit(2);
    assertFalse(
        containsEnumerableLimit(logical),
        "the plan handed to the runner is logical: " + RelOptUtil.toString(logical));

    AtomicReference<String> observedPlan = new AtomicReference<>();
    AtomicInteger limitsSeen = new AtomicInteger();
    try (PhysicalPlanHook.Scope scope =
        PhysicalPlanHook.install(
            physical -> {
              observedPlan.set(RelOptUtil.toString(physical));
              limitsSeen.addAndGet(countEnumerableLimits(physical));
              return physical;
            })) {
      drain(logical);
    }

    assertNotNull(observedPlan.get(), "the callback must be invoked during statement preparation");
    assertTrue(
        limitsSeen.get() >= 1,
        "the callback must see a physical limit; it saw:\n" + observedPlan.get());
  }

  @Test
  @DisplayName(
      "a replacement returned by the callback is the node that executes and sees rows as they are"
          + " emitted")
  void replacementExecutesAndObservesRowsAsEmitted() throws Exception {
    RelNode logical = planWithLimit(2);
    List<String> events = new ArrayList<>();

    try (PhysicalPlanHook.Scope scope =
        PhysicalPlanHook.install(physical -> RecordingLimit.wrap(physical, events))) {
      List<Integer> rows = drain(logical);

      // The limit's rows are unchanged, and the replacement saw each one at the moment it was
      // emitted — not after
      // execution finished. That ordering is the whole point: a source capped by a limit is
      // finished with its work
      // then, while the rest of the plan is still running.
      assertEquals(List.of(1, 2), rows);
      assertEquals(List.of("row", "row", "quota-reached"), events);
    }
  }

  // ------------------------------------------------------------------ helpers

  /**
   * One config, shared by the plan builder and the connection: the prepared statement resolves the
   * plan's tables against the connection's root schema, so they have to be the same schema.
   */
  private final FrameworkConfig config = newConfig();

  private static FrameworkConfig newConfig() {
    SchemaPlus root = Frameworks.createRootSchema(true);
    root.add("test", new ReflectiveSchema(new TestSchema()));
    return Frameworks.newConfigBuilder().defaultSchema(root).build();
  }

  /** Builds {@code SELECT id FROM rows LIMIT n} as the engine would hand it over: logical. */
  private RelNode planWithLimit(int fetch) {
    RelBuilder builder = RelBuilder.create(config);
    return builder.scan("test", "rows").project(builder.field("id")).limit(0, fetch).build();
  }

  /** Prepares and executes {@code plan} through the repo's own prepare path, returning its rows. */
  private List<Integer> drain(RelNode plan) throws Exception {
    List<Integer> rows = new ArrayList<>();
    try (java.sql.Connection connection =
        CalciteToolsHelper.connect(
            config,
            (org.apache.calcite.adapter.java.JavaTypeFactory) plan.getCluster().getTypeFactory())) {
      // Generated code resolves the scanned table by name against the *connection's* root schema at
      // run time, and the
      // connection builds its own root. Registering the schema there is what the OpenSearch path
      // avoids needing by
      // stashing its scan instead of looking one up.
      connection
          .unwrap(org.apache.calcite.jdbc.CalciteConnection.class)
          .getRootSchema()
          .add("test", new ReflectiveSchema(new TestSchema()));
      try (PreparedStatement statement =
              connection.unwrap(org.apache.calcite.tools.RelRunner.class).prepareStatement(plan);
          ResultSet results = statement.executeQuery()) {
        while (results.next()) {
          rows.add(results.getInt(1));
        }
      }
    }
    return rows;
  }

  private static boolean containsEnumerableLimit(RelNode node) {
    return countEnumerableLimits(node) > 0;
  }

  private static int countEnumerableLimits(RelNode node) {
    int count = node instanceof EnumerableLimit ? 1 : 0;
    for (RelNode input : node.getInputs()) {
      count += countEnumerableLimits(input);
    }
    return count;
  }
}
