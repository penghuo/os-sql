/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.planner;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.util.ImmutableBitSet;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.distributed.planner.plan.AddExchanges;
import org.opensearch.sql.distributed.planner.plan.PartitioningScheme;
import org.opensearch.sql.distributed.planner.plan.PlanFragmenter;
import org.opensearch.sql.distributed.planner.plan.StageFragment;

/** Tests that PlanFragmenter correctly splits plans into leaf + root stages. */
class TwoStageFragmentTest extends ConverterTestBase {

  private final AddExchanges addExchanges = new AddExchanges();
  private final PlanFragmenter fragmenter = new PlanFragmenter();

  @Test
  @DisplayName("Aggregation plan produces 2 fragments: leaf (partial) + root (final)")
  void aggregationProducesTwoFragments() {
    var scan = createTableScan("test", "name", "age", "status");
    AggregateCall countCall = countCall();
    var agg =
        LogicalAggregate.create(scan, List.of(), ImmutableBitSet.of(2), null, List.of(countCall));

    PlanNode planNode = converter.convert(agg);
    PlanNode withExchange = addExchanges.addExchanges(planNode);
    List<StageFragment> fragments = fragmenter.fragment(withExchange);

    // Should produce exactly 2 fragments
    assertEquals(2, fragments.size());

    // First fragment should be the leaf stage
    StageFragment leafStage = fragments.get(0);
    assertTrue(leafStage.isLeafStage());
    assertInstanceOf(AggregationNode.class, leafStage.getRoot());
    AggregationNode partialAgg = (AggregationNode) leafStage.getRoot();
    assertEquals(AggregationNode.AggregationMode.PARTIAL, partialAgg.getMode());

    // Second fragment should be the root stage
    StageFragment rootStage = fragments.get(1);
    assertFalse(rootStage.isLeafStage());
    assertInstanceOf(AggregationNode.class, rootStage.getRoot());
    AggregationNode finalAgg = (AggregationNode) rootStage.getRoot();
    assertEquals(AggregationNode.AggregationMode.FINAL, finalAgg.getMode());
  }

  @Test
  @DisplayName("Root stage has RemoteSourceNode referencing leaf stage")
  void rootStageHasRemoteSource() {
    var scan = createTableScan("test", "name", "age", "status");
    AggregateCall countCall = countCall();
    var agg =
        LogicalAggregate.create(scan, List.of(), ImmutableBitSet.of(2), null, List.of(countCall));

    PlanNode planNode = converter.convert(agg);
    PlanNode withExchange = addExchanges.addExchanges(planNode);
    List<StageFragment> fragments = fragmenter.fragment(withExchange);

    StageFragment leafStage = fragments.get(0);
    StageFragment rootStage = fragments.get(1);

    // Root stage should have exactly one RemoteSourceNode
    assertEquals(1, rootStage.getRemoteSourceNodes().size());
    RemoteSourceNode remoteSource = rootStage.getRemoteSourceNodes().get(0);
    assertTrue(remoteSource.getSourceStageIds().contains(leafStage.getStageId()));
    assertEquals(ExchangeNode.ExchangeType.GATHER, remoteSource.getExchangeType());
  }

  @Test
  @DisplayName("Sort plan produces 2 fragments with correct structure")
  void sortProducesTwoFragments() {
    var scan = createTableScan("test", "name", "age");
    var collation = RelCollations.of(new RelFieldCollation(1));
    var sort = LogicalSort.create(scan, collation, null, null);

    PlanNode planNode = converter.convert(sort);
    PlanNode withExchange = addExchanges.addExchanges(planNode);
    List<StageFragment> fragments = fragmenter.fragment(withExchange);

    assertEquals(2, fragments.size());

    // Leaf stage should have local sort
    StageFragment leafStage = fragments.get(0);
    assertTrue(leafStage.isLeafStage());
    assertInstanceOf(SortNode.class, leafStage.getRoot());

    // Root stage should have coordinator sort with RemoteSourceNode
    StageFragment rootStage = fragments.get(1);
    assertFalse(rootStage.isLeafStage());
    assertInstanceOf(SortNode.class, rootStage.getRoot());
  }

  @Test
  @DisplayName("Leaf stage has COORDINATOR_ONLY output partitioning (gather)")
  void leafStageHasGatherPartitioning() {
    var scan = createTableScan("test", "name", "age");
    var collation = RelCollations.of(new RelFieldCollation(1));
    var sort = LogicalSort.create(scan, collation, null, null);

    PlanNode planNode = converter.convert(sort);
    PlanNode withExchange = addExchanges.addExchanges(planNode);
    List<StageFragment> fragments = fragmenter.fragment(withExchange);

    StageFragment leafStage = fragments.get(0);
    assertEquals(
        PartitioningScheme.Partitioning.COORDINATOR_ONLY,
        leafStage.getOutputPartitioning().getPartitioning());
  }

  @Test
  @DisplayName("Filter-only plan with ensureGatherExchange produces 2 fragments")
  void filterOnlyWithEnsuredExchange() {
    var scan = createTableScan("test", "name", "age");
    var filter = LogicalFilter.create(scan, greaterThan(fieldRef(1), literal(30)));

    PlanNode planNode = converter.convert(filter);
    PlanNode withExchange = addExchanges.addExchanges(planNode);
    withExchange = addExchanges.ensureGatherExchange(withExchange);
    List<StageFragment> fragments = fragmenter.fragment(withExchange);

    assertEquals(2, fragments.size());
    assertTrue(fragments.get(0).isLeafStage());
    assertFalse(fragments.get(1).isLeafStage());
  }
}
