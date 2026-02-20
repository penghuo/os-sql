/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.planner;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.util.ImmutableBitSet;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.distributed.planner.plan.AddExchanges;
import org.opensearch.sql.distributed.planner.plan.PlanFragmenter;
import org.opensearch.sql.distributed.planner.plan.StageFragment;

/**
 * Tests the full multi-stage execution pipeline: PlanNode tree -> AddExchanges ->
 * ensureGatherExchange -> PlanFragmenter -> StageFragments.
 *
 * <p>Verifies that the planner correctly fragments plans into leaf and root stages, with
 * RemoteSourceNodes wiring stages together.
 */
class MultiStageExecutionTest extends ConverterTestBase {

  private final AddExchanges addExchanges = new AddExchanges();
  private final PlanFragmenter fragmenter = new PlanFragmenter();

  @Test
  @DisplayName("Aggregation produces exactly 2 fragments with correct node types")
  void aggregationProducesCorrectFragments() {
    var scan = createTableScan("test_index", "name", "age", "status");
    AggregateCall countCall = countCall();
    var agg =
        LogicalAggregate.create(scan, List.of(), ImmutableBitSet.of(2), null, List.of(countCall));

    PlanNode planNode = converter.convert(agg);
    PlanNode withExchange = addExchanges.addExchanges(planNode);
    withExchange = addExchanges.ensureGatherExchange(withExchange);
    List<StageFragment> fragments = fragmenter.fragment(withExchange);

    // Exactly 2 fragments
    assertEquals(2, fragments.size(), "Expected exactly 2 fragments (leaf + root)");

    // Leaf fragment contains LuceneTableScanNode (via AggregationNode -> source chain)
    StageFragment leafStage = fragments.get(0);
    assertTrue(leafStage.isLeafStage(), "First fragment should be a leaf stage");
    assertTrue(
        containsNodeType(leafStage.getRoot(), LuceneTableScanNode.class),
        "Leaf fragment must contain a LuceneTableScanNode");

    // Root fragment contains RemoteSourceNode
    StageFragment rootStage = fragments.get(1);
    assertFalse(rootStage.isLeafStage(), "Second fragment should be the root stage");
    assertEquals(
        1,
        rootStage.getRemoteSourceNodes().size(),
        "Root stage should have exactly one RemoteSourceNode");

    // RemoteSourceNode references the leaf stage
    RemoteSourceNode remoteSource = rootStage.getRemoteSourceNodes().get(0);
    assertTrue(
        remoteSource.getSourceStageIds().contains(leafStage.getStageId()),
        "RemoteSourceNode.getSourceStageIds() must reference the leaf stage ID ("
            + leafStage.getStageId()
            + "), but got "
            + remoteSource.getSourceStageIds());
  }

  @Test
  @DisplayName("Filter-only plan with ensureGatherExchange produces 2 fragments")
  void filterOnlyProducesCorrectFragments() {
    var scan = createTableScan("my_index", "col_a", "col_b");
    var filter = LogicalFilter.create(scan, greaterThan(fieldRef(1), literal(10)));

    PlanNode planNode = converter.convert(filter);
    PlanNode withExchange = addExchanges.addExchanges(planNode);
    withExchange = addExchanges.ensureGatherExchange(withExchange);
    List<StageFragment> fragments = fragmenter.fragment(withExchange);

    assertEquals(2, fragments.size(), "Expected exactly 2 fragments (leaf + root)");

    StageFragment leafStage = fragments.get(0);
    assertTrue(leafStage.isLeafStage());
    assertTrue(containsNodeType(leafStage.getRoot(), LuceneTableScanNode.class));

    StageFragment rootStage = fragments.get(1);
    assertFalse(rootStage.isLeafStage());
    RemoteSourceNode remoteSource = rootStage.getRemoteSourceNodes().get(0);
    assertTrue(remoteSource.getSourceStageIds().contains(leafStage.getStageId()));
  }

  @Test
  @DisplayName("RemoteSourceNode exchange type matches the exchange it replaced")
  void remoteSourceNodeExchangeTypeMatchesExchange() {
    var scan = createTableScan("test_index", "name", "age", "status");
    AggregateCall countCall = countCall();
    var agg =
        LogicalAggregate.create(scan, List.of(), ImmutableBitSet.of(0), null, List.of(countCall));

    PlanNode planNode = converter.convert(agg);
    PlanNode withExchange = addExchanges.addExchanges(planNode);
    List<StageFragment> fragments = fragmenter.fragment(withExchange);

    StageFragment rootStage = fragments.get(fragments.size() - 1);
    RemoteSourceNode remoteSource = rootStage.getRemoteSourceNodes().get(0);
    assertEquals(
        ExchangeNode.ExchangeType.GATHER,
        remoteSource.getExchangeType(),
        "Scatter-gather aggregation should produce GATHER exchange type");
  }

  @Test
  @DisplayName("Leaf stage fragment has no remote source nodes")
  void leafStageHasNoRemoteSources() {
    var scan = createTableScan("test_index", "x", "y");
    var filter = LogicalFilter.create(scan, equals(fieldRef(0), literalString("foo")));

    PlanNode planNode = converter.convert(filter);
    PlanNode withExchange = addExchanges.addExchanges(planNode);
    withExchange = addExchanges.ensureGatherExchange(withExchange);
    List<StageFragment> fragments = fragmenter.fragment(withExchange);

    StageFragment leafStage = fragments.get(0);
    assertTrue(leafStage.isLeafStage());
    assertTrue(
        leafStage.getRemoteSourceNodes().isEmpty(),
        "Leaf stage should have no remote source nodes");
  }

  /** Recursively checks if the plan tree rooted at {@code node} contains a node of the given type. */
  private boolean containsNodeType(PlanNode node, Class<? extends PlanNode> type) {
    if (type.isInstance(node)) {
      return true;
    }
    for (PlanNode child : node.getSources()) {
      if (containsNodeType(child, type)) {
        return true;
      }
    }
    return false;
  }
}
