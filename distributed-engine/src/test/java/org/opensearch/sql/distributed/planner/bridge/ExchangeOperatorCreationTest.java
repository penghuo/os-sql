/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.planner.bridge;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.distributed.operator.FilterAndProjectOperator;
import org.opensearch.sql.distributed.operator.HashAggregationOperator;
import org.opensearch.sql.distributed.operator.OperatorFactory;
import org.opensearch.sql.distributed.planner.AggregationNode;
import org.opensearch.sql.distributed.planner.ExchangeNode;
import org.opensearch.sql.distributed.planner.LuceneTableScanNode;
import org.opensearch.sql.distributed.planner.PlanNode;
import org.opensearch.sql.distributed.planner.PlanNodeId;
import org.opensearch.sql.distributed.planner.ProjectNode;
import org.opensearch.sql.distributed.planner.RemoteSourceNode;
import org.opensearch.sql.distributed.planner.plan.AddExchanges;
import org.opensearch.sql.distributed.planner.plan.PartitioningScheme;
import org.opensearch.sql.distributed.planner.plan.PlanFragmenter;
import org.opensearch.sql.distributed.planner.plan.StageFragment;

/**
 * Tests that PlanNodeToOperatorConverter correctly handles exchange-related nodes. Specifically
 * verifies that:
 *
 * <ul>
 *   <li>RemoteSourceNode calls createRemoteSourceFactory (not a pass-through)
 *   <li>ExchangeNode in pre-fragmentation context passes through to child
 *   <li>The converter produces correct factory types for fragmented plans
 * </ul>
 */
class ExchangeOperatorCreationTest {

  private static org.apache.calcite.rel.type.RelDataTypeFactory typeFactory;
  private static RexBuilder rexBuilder;
  private static RelDataType bigintType;
  private static RelDataType rowType;

  /** Tracking source provider that records which methods were called. */
  private static class TrackingSourceProvider
      implements PlanNodeToOperatorConverter.SourceOperatorFactoryProvider {

    int scanCalls = 0;
    int remoteCalls = 0;

    @Override
    public OperatorFactory createSourceFactory(LuceneTableScanNode node) {
      scanCalls++;
      return new StubFactory("scan:" + node.getIndexName());
    }

    @Override
    public OperatorFactory createRemoteSourceFactory(RemoteSourceNode node) {
      remoteCalls++;
      return new StubFactory("remote:stages=" + node.getSourceStageIds());
    }
  }

  @BeforeAll
  static void setup() {
    typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    rexBuilder = new RexBuilder(typeFactory);
    bigintType = typeFactory.createSqlType(SqlTypeName.BIGINT);
    rowType =
        typeFactory.createStructType(List.of(bigintType, bigintType), List.of("col0", "col1"));
  }

  @Test
  @DisplayName("RemoteSourceNode invokes createRemoteSourceFactory, not pass-through")
  void remoteSourceNodeCreatesFactory() {
    RemoteSourceNode remoteSource =
        new RemoteSourceNode(
            PlanNodeId.next("remote"), List.of(0), ExchangeNode.ExchangeType.GATHER);

    TrackingSourceProvider provider = new TrackingSourceProvider();
    List<OperatorFactory> factories =
        PlanNodeToOperatorConverter.convert(remoteSource, provider);

    assertEquals(1, factories.size(), "RemoteSourceNode should produce exactly 1 factory");
    assertEquals(0, provider.scanCalls, "Should not call createSourceFactory for RemoteSourceNode");
    assertEquals(
        1, provider.remoteCalls, "Should call createRemoteSourceFactory for RemoteSourceNode");
  }

  @Test
  @DisplayName("ExchangeNode in pre-fragmentation context passes through to child")
  void exchangeNodePassesThroughToChild() {
    LuceneTableScanNode scan =
        new LuceneTableScanNode(
            PlanNodeId.next("scan"), "test_index", rowType, List.of("col0", "col1"));
    ExchangeNode exchange =
        new ExchangeNode(
            PlanNodeId.next("exchange"),
            scan,
            ExchangeNode.ExchangeType.GATHER,
            PartitioningScheme.gatherPartitioning());

    TrackingSourceProvider provider = new TrackingSourceProvider();
    List<OperatorFactory> factories =
        PlanNodeToOperatorConverter.convert(exchange, provider);

    // Exchange is transparent — the scan should still be processed
    assertEquals(1, factories.size(), "Exchange pass-through should produce only the child factory");
    assertEquals(1, provider.scanCalls, "Child LuceneTableScanNode should still be processed");
    assertEquals(0, provider.remoteCalls, "No RemoteSourceNode in this plan");
  }

  @Test
  @DisplayName("Fragmented root stage with RemoteSource + Aggregation produces correct factories")
  void fragmentedRootStageProducesCorrectFactories() {
    // Build a plan: Aggregation(LuceneTableScan)
    LuceneTableScanNode scan =
        new LuceneTableScanNode(
            PlanNodeId.next("scan"), "test_index", rowType, List.of("col0", "col1"));
    AggregateCall countCall =
        AggregateCall.create(
            SqlStdOperatorTable.COUNT,
            false,
            false,
            false,
            List.of(),
            List.of(),
            -1,
            null,
            org.apache.calcite.rel.RelCollations.EMPTY,
            bigintType,
            "cnt");
    AggregationNode agg =
        new AggregationNode(
            PlanNodeId.next("agg"),
            scan,
            ImmutableBitSet.of(0),
            List.of(countCall),
            AggregationNode.AggregationMode.SINGLE);

    // Add exchanges + fragment
    AddExchanges addExchanges = new AddExchanges();
    PlanNode withExchange = addExchanges.addExchanges(agg);
    PlanFragmenter fragmenter = new PlanFragmenter();
    List<StageFragment> fragments = fragmenter.fragment(withExchange);

    // Get the root stage
    StageFragment rootStage = fragments.get(fragments.size() - 1);
    assertFalse(rootStage.isLeafStage());

    // Convert root stage to operators
    TrackingSourceProvider provider = new TrackingSourceProvider();
    List<OperatorFactory> factories =
        PlanNodeToOperatorConverter.convert(rootStage.getRoot(), provider);

    // Root stage should have: RemoteSourceFactory + HashAggregationFactory (for final agg)
    assertEquals(2, factories.size(), "Root stage should have 2 factories: remote source + agg");
    assertEquals(
        1,
        provider.remoteCalls,
        "Root stage should invoke createRemoteSourceFactory for the RemoteSourceNode");
    assertInstanceOf(
        HashAggregationOperator.HashAggregationOperatorFactory.class,
        factories.get(1),
        "Second factory should be HashAggregation for the final aggregation");
  }

  @Test
  @DisplayName("Leaf stage contains LuceneTableScanNode, not RemoteSourceNode")
  void leafStageContainsTableScan() {
    LuceneTableScanNode scan =
        new LuceneTableScanNode(
            PlanNodeId.next("scan"), "test_index", rowType, List.of("col0", "col1"));
    AggregateCall countCall =
        AggregateCall.create(
            SqlStdOperatorTable.COUNT,
            false,
            false,
            false,
            List.of(),
            List.of(),
            -1,
            null,
            org.apache.calcite.rel.RelCollations.EMPTY,
            bigintType,
            "cnt");
    AggregationNode agg =
        new AggregationNode(
            PlanNodeId.next("agg"),
            scan,
            ImmutableBitSet.of(0),
            List.of(countCall),
            AggregationNode.AggregationMode.SINGLE);

    AddExchanges addExchanges = new AddExchanges();
    PlanNode withExchange = addExchanges.addExchanges(agg);
    PlanFragmenter fragmenter = new PlanFragmenter();
    List<StageFragment> fragments = fragmenter.fragment(withExchange);

    StageFragment leafStage = fragments.get(0);
    assertTrue(leafStage.isLeafStage());

    TrackingSourceProvider provider = new TrackingSourceProvider();
    List<OperatorFactory> factories =
        PlanNodeToOperatorConverter.convert(leafStage.getRoot(), provider);

    assertEquals(
        1,
        provider.scanCalls,
        "Leaf stage should invoke createSourceFactory for LuceneTableScanNode");
    assertEquals(0, provider.remoteCalls, "Leaf stage should have no RemoteSourceNode calls");
  }

  /** Stub OperatorFactory for test assertions. */
  private static class StubFactory implements OperatorFactory {
    private final String label;

    StubFactory(String label) {
      this.label = label;
    }

    @Override
    public org.opensearch.sql.distributed.operator.Operator createOperator(
        org.opensearch.sql.distributed.context.OperatorContext ctx) {
      throw new UnsupportedOperationException("StubFactory.createOperator");
    }

    @Override
    public void noMoreOperators() {}

    @Override
    public String toString() {
      return "StubFactory[" + label + "]";
    }
  }
}
