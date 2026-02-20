/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.transport;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.distributed.context.OperatorContext;
import org.opensearch.sql.distributed.data.Page;
import org.opensearch.sql.distributed.data.PagesSerde;
import org.opensearch.sql.distributed.operator.Operator;
import org.opensearch.sql.distributed.operator.OperatorFactory;
import org.opensearch.sql.distributed.planner.AggregationNode;
import org.opensearch.sql.distributed.planner.LuceneTableScanNode;
import org.opensearch.sql.distributed.planner.PlanNode;
import org.opensearch.sql.distributed.planner.PlanNodeId;
import org.opensearch.sql.distributed.planner.RemoteSourceNode;
import org.opensearch.sql.distributed.planner.bridge.PlanNodeToOperatorConverter;
import org.opensearch.sql.distributed.planner.plan.AddExchanges;
import org.opensearch.sql.distributed.planner.plan.PlanFragmenter;
import org.opensearch.sql.distributed.planner.plan.StageFragment;

/**
 * Tests the TransportShardQueryAction pipeline building components. Since the full transport action
 * requires OpenSearch cluster infrastructure (IndicesService, ClusterService, TransportService),
 * these tests verify the individual components it relies on: FragmentRegistry, ShardQueryRequest
 * validation, and the pipeline building via PlanNodeToOperatorConverter.
 */
class TransportShardQueryActionTest {

  private static org.apache.calcite.rel.type.RelDataTypeFactory typeFactory;
  private static RexBuilder rexBuilder;
  private static RelDataType bigintType;
  private static RelDataType rowType;

  @BeforeAll
  static void setup() {
    typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    rexBuilder = new RexBuilder(typeFactory);
    bigintType = typeFactory.createSqlType(SqlTypeName.BIGINT);
    rowType =
        typeFactory.createStructType(List.of(bigintType, bigintType), List.of("col0", "col1"));
  }

  @AfterEach
  void cleanup() {
    FragmentRegistry.clear();
  }

  @Test
  @DisplayName("FragmentRegistry stores and retrieves StageFragment correctly")
  void fragmentRegistryStoresAndRetrieves() {
    LuceneTableScanNode scan =
        new LuceneTableScanNode(
            PlanNodeId.next("scan"), "test_index", rowType, List.of("col0", "col1"));
    StageFragment fragment =
        new StageFragment(
            0,
            scan,
            org.opensearch.sql.distributed.planner.plan.PartitioningScheme
                .sourceDistributed(),
            List.of());

    String queryId = "test-query-123";
    FragmentRegistry.put(queryId, 0, fragment);

    StageFragment retrieved = FragmentRegistry.get(queryId, 0);
    assertNotNull(retrieved, "Fragment should be retrievable from the registry");
    assertEquals(0, retrieved.getStageId());
    assertInstanceOf(LuceneTableScanNode.class, retrieved.getRoot());
  }

  @Test
  @DisplayName("FragmentRegistry returns null for unregistered fragment")
  void fragmentRegistryReturnsNullForMissing() {
    StageFragment result = FragmentRegistry.get("nonexistent-query", 0);
    assertNull(result, "Should return null for unregistered fragment");
  }

  @Test
  @DisplayName("ShardQueryRequest validates missing queryId")
  void shardQueryRequestValidatesMissingQueryId() {
    ShardQueryRequest request =
        new ShardQueryRequest("", 0, new byte[] {1}, List.of(0), "test_index");

    assertNotNull(request.validate(), "Request with empty queryId should fail validation");
  }

  @Test
  @DisplayName("ShardQueryRequest validates missing serializedFragment")
  void shardQueryRequestValidatesMissingFragment() {
    ShardQueryRequest request =
        new ShardQueryRequest("query-1", 0, new byte[] {}, List.of(0), "test_index");

    assertNotNull(request.validate(), "Request with empty fragment should fail validation");
  }

  @Test
  @DisplayName("ShardQueryRequest validates missing shardIds")
  void shardQueryRequestValidatesMissingShards() {
    ShardQueryRequest request =
        new ShardQueryRequest("query-1", 0, new byte[] {1}, List.of(), "test_index");

    assertNotNull(request.validate(), "Request with empty shardIds should fail validation");
  }

  @Test
  @DisplayName("Valid ShardQueryRequest passes validation")
  void validShardQueryRequestPassesValidation() {
    ShardQueryRequest request =
        new ShardQueryRequest("query-1", 0, new byte[] {1, 2, 3}, List.of(0, 1), "test_index");

    assertNull(request.validate(), "Valid request should pass validation");
    assertEquals("query-1", request.getQueryId());
    assertEquals(0, request.getStageId());
    assertEquals(List.of(0, 1), request.getShardIds());
    assertEquals("test_index", request.getIndexName());
  }

  @Test
  @DisplayName("PlanNodeToOperatorConverter builds pipeline from leaf fragment's PlanNode")
  void converterBuildsPipelineFromLeafFragment() {
    // Build a simple aggregation plan, fragment it, and verify the leaf fragment
    // can produce a correct operator pipeline
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
            RelCollations.EMPTY,
            bigintType,
            "cnt");
    AggregationNode agg =
        new AggregationNode(
            PlanNodeId.next("agg"),
            scan,
            ImmutableBitSet.of(0),
            List.of(countCall),
            AggregationNode.AggregationMode.SINGLE);

    // Fragment the plan
    AddExchanges addExchanges = new AddExchanges();
    PlanNode withExchange = addExchanges.addExchanges(agg);
    PlanFragmenter fragmenter = new PlanFragmenter();
    List<StageFragment> fragments = fragmenter.fragment(withExchange);

    // Register the leaf fragment (simulates what the coordinator does)
    StageFragment leafStage = fragments.get(0);
    assertTrue(leafStage.isLeafStage());
    FragmentRegistry.put("test-query", leafStage.getStageId(), leafStage);

    // Verify the registered fragment can produce operator factories
    StageFragment retrieved = FragmentRegistry.get("test-query", leafStage.getStageId());
    assertNotNull(retrieved);

    // Use a mock source provider to verify the converter processes the leaf correctly
    MockSourceProvider provider = new MockSourceProvider();
    List<OperatorFactory> factories =
        PlanNodeToOperatorConverter.convert(retrieved.getRoot(), provider);

    // Leaf fragment should have: scan source + partial aggregation
    assertTrue(factories.size() >= 2, "Leaf fragment should produce at least 2 factories");
    assertTrue(provider.scanCalled, "Source factory should be called for LuceneTableScanNode");
    assertFalse(provider.remoteCalled, "Leaf fragment should not have RemoteSourceNode");
  }

  @Test
  @DisplayName("ShardQueryResponse serialization round-trip preserves pages")
  void shardQueryResponseRoundTrip() {
    PagesSerde serde = new PagesSerde();

    // Create a response with empty pages (no actual data, but valid)
    List<Page> inputPages = List.of();
    ShardQueryResponse response = new ShardQueryResponse(inputPages, false, serde);

    assertEquals(0, response.getPageCount());
    assertFalse(response.hasMore());
    List<Page> outputPages = response.getPages(serde);
    assertEquals(0, outputPages.size());
  }

  @Test
  @DisplayName("FragmentRegistry key format is queryId:stageId")
  void fragmentRegistryKeyFormat() {
    assertEquals("q1:0", FragmentRegistry.key("q1", 0));
    assertEquals("my-query:42", FragmentRegistry.key("my-query", 42));
  }

  /** Mock source provider for testing the pipeline building path. */
  private static class MockSourceProvider
      implements PlanNodeToOperatorConverter.SourceOperatorFactoryProvider {

    boolean scanCalled = false;
    boolean remoteCalled = false;

    @Override
    public OperatorFactory createSourceFactory(LuceneTableScanNode node) {
      scanCalled = true;
      return new OperatorFactory() {
        @Override
        public Operator createOperator(OperatorContext ctx) {
          throw new UnsupportedOperationException("mock");
        }

        @Override
        public void noMoreOperators() {}
      };
    }

    @Override
    public OperatorFactory createRemoteSourceFactory(RemoteSourceNode node) {
      remoteCalled = true;
      return new OperatorFactory() {
        @Override
        public Operator createOperator(OperatorContext ctx) {
          throw new UnsupportedOperationException("mock");
        }

        @Override
        public void noMoreOperators() {}
      };
    }
  }
}
