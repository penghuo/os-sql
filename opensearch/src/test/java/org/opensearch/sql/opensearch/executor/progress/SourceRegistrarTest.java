/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.progress;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.opensearch.storage.scan.AbstractCalciteIndexScan;

/**
 * Plan traversal for source registration.
 *
 * <p>The case that matters is the subquery one: an uncorrelated {@code in} subquery survives into
 * the physical plan as a {@code RexSubQuery} held inside a filter's condition, and no node lists
 * its plan among its inputs. A walker that only follows {@code getInputs()} therefore never sees
 * that scan, the source is never registered, and the subquery reports no progress for the whole
 * query.
 */
class SourceRegistrarTest {

  @Test
  @DisplayName("finds a scan reachable through inputs")
  void findsScanThroughInputs() {
    AbstractCalciteIndexScan scan = mock(AbstractCalciteIndexScan.class);
    RelNode project = node(List.of(scan));
    RelNode filter = node(List.of(project));

    assertEquals(List.of(scan), SourceRegistrar.collectScans(filter));
  }

  @Test
  @DisplayName("finds a scan hidden inside a subquery expression")
  void findsScanInsideSubQuery() {
    AbstractCalciteIndexScan outer = mock(AbstractCalciteIndexScan.class);
    AbstractCalciteIndexScan inner = mock(AbstractCalciteIndexScan.class);
    // A filter whose condition holds the subquery plan: visible to a RexShuttle, absent from
    // getInputs().
    RelNode filter = nodeWithSubQueryPlans(List.of(outer), List.of(inner));

    List<AbstractCalciteIndexScan> scans = SourceRegistrar.collectScans(filter);

    assertTrue(scans.contains(outer), "outer scan must be registered");
    assertTrue(scans.contains(inner), "subquery scan must be registered");
    assertEquals(2, scans.size());
  }

  @Test
  @DisplayName("registers both sides of a join over the same index as two occurrences")
  void findsBothJoinSides() {
    AbstractCalciteIndexScan build = mock(AbstractCalciteIndexScan.class);
    AbstractCalciteIndexScan probe = mock(AbstractCalciteIndexScan.class);
    RelNode join = node(List.of(build, probe));

    List<AbstractCalciteIndexScan> scans = SourceRegistrar.collectScans(join);

    assertEquals(2, scans.size());
    assertSame(build, scans.get(0));
    assertSame(probe, scans.get(1));
  }

  @Test
  @DisplayName("a node Calcite canonicalized onto two positions counts twice")
  void canonicalizedNodeCountsPerPosition() {
    AbstractCalciteIndexScan shared = mock(AbstractCalciteIndexScan.class);
    // What a same-index equijoin actually looks like after planning: both scan positions are the
    // same object.
    RelNode join = node(List.of(shared, shared));

    List<AbstractCalciteIndexScan> scans = SourceRegistrar.collectScans(join);

    // Two positions, two independent searches at execution time, two sources. Counting the object
    // once would
    // register one source for two reads: the first to finish would report the plan's whole source
    // work as done
    // and the second would report nothing at all.
    assertEquals(2, scans.size());
    assertSame(shared, scans.get(0));
    assertSame(shared, scans.get(1));
  }

  @Test
  @DisplayName("a shared subtree counts once per reference, matching how often it is read")
  void sharedSubtreeCountsPerReference() {
    AbstractCalciteIndexScan shared = mock(AbstractCalciteIndexScan.class);
    RelNode left = node(List.of(shared));
    RelNode right = node(List.of(shared));
    RelNode union = node(List.of(left, right));

    assertEquals(2, SourceRegistrar.collectScans(union).size());
  }

  @Test
  @DisplayName("a self-referential plan terminates instead of recursing forever")
  void cyclicPlanTerminates() {
    RelNode[] holder = new RelNode[1];
    RelNode self = mock(RelNode.class);
    when(self.getInputs()).thenAnswer(invocation -> new ArrayList<>(List.of(holder[0])));
    holder[0] = self;

    // A path-scoped guard is what keeps position counting from looping on a cycle.
    assertEquals(List.of(), SourceRegistrar.collectScans(self));
  }

  @Test
  @DisplayName("a plan with no scan yields an empty source set")
  void sourcelessPlanYieldsNothing() {
    assertEquals(List.of(), SourceRegistrar.collectScans(node(List.of())));
  }

  // ---------------------------------------------------------------- helpers

  private static RelNode node(List<RelNode> inputs) {
    RelNode node = mock(RelNode.class);
    when(node.getInputs()).thenReturn(new ArrayList<>(inputs));
    return node;
  }

  /**
   * A node whose {@code accept(RexShuttle)} hands the shuttle a subquery for each plan in {@code
   * subQueryPlans}, mirroring how a filter exposes the plans held in its condition.
   */
  private static RelNode nodeWithSubQueryPlans(List<RelNode> inputs, List<RelNode> subQueryPlans) {
    RelNode node = mock(RelNode.class);
    when(node.getInputs()).thenReturn(new ArrayList<>(inputs));
    when(node.accept(org.mockito.ArgumentMatchers.any(RexShuttle.class)))
        .thenAnswer(
            invocation -> {
              RexShuttle shuttle = invocation.getArgument(0);
              for (RelNode plan : subQueryPlans) {
                shuttle.visitSubQuery(subQueryOf(plan));
              }
              return node;
            });
    return node;
  }

  /**
   * A real {@code RexSubQuery} carrying {@code plan}, built the way Calcite builds an {@code
   * exists}.
   */
  private static RexSubQuery subQueryOf(RelNode plan) {
    RelOptCluster cluster = mock(RelOptCluster.class);
    when(cluster.getTypeFactory()).thenReturn(new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT));
    when(plan.getCluster()).thenReturn(cluster);
    return RexSubQuery.exists(plan);
  }
}
