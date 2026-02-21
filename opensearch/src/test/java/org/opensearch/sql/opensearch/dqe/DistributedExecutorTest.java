/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.dqe;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.core.action.ActionListener;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.executor.ExecutionEngine.QueryResponse;
import org.opensearch.sql.opensearch.dqe.exchange.ConcatExchange;
import org.opensearch.sql.opensearch.dqe.exchange.Exchange;
import org.opensearch.sql.opensearch.dqe.exchange.MergeAggregateExchange;
import org.opensearch.sql.opensearch.dqe.exchange.MergeSortExchange;
import org.opensearch.sql.opensearch.dqe.serde.RelNodeSerializer;
import org.opensearch.sql.opensearch.storage.scan.AbstractCalciteIndexScan;

@ExtendWith(MockitoExtension.class)
class DistributedExecutorTest {

  @Mock private ShardRoutingResolver resolver;

  @Mock private ShardQueryDispatcher dispatcher;

  private DistributedExecutor executor;
  private RelDataTypeFactory typeFactory;
  private RelDataType rowType;
  private RelOptCluster cluster;

  @BeforeEach
  void setUp() {
    executor = new DistributedExecutor(resolver, dispatcher);
    typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    rowType =
        typeFactory.createStructType(
            List.of(
                typeFactory.createSqlType(SqlTypeName.VARCHAR, 100),
                typeFactory.createSqlType(SqlTypeName.INTEGER)),
            List.of("name", "age"));
    RexBuilder rexBuilder = new RexBuilder(typeFactory);
    VolcanoPlanner planner = new VolcanoPlanner();
    planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
    cluster = RelOptCluster.create(planner, rexBuilder);
  }

  // ============= Helper methods =============

  private AbstractCalciteIndexScan mockScanNode() {
    AbstractCalciteIndexScan scan = mock(AbstractCalciteIndexScan.class, RETURNS_DEEP_STUBS);
    RelOptTable table = mock(RelOptTable.class);
    when(scan.getTable()).thenReturn(table);
    when(table.getQualifiedName()).thenReturn(List.of("test_index"));
    when(scan.getRowType()).thenReturn(rowType);
    when(scan.getInputs()).thenReturn(Collections.emptyList());
    return scan;
  }

  private List<Integer> createShardIds(int count) {
    List<Integer> shardIds = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      shardIds.add(i);
    }
    return shardIds;
  }

  /** Creates an ExchangeLeaf coordinator node wrapping the given exchange. */
  private PlanSplitter.ExchangeLeaf exchangeLeaf(Exchange exchange) {
    return new PlanSplitter.ExchangeLeaf(cluster, cluster.traitSet(), exchange);
  }

  /**
   * Configures the mock dispatcher to immediately call onResponse with the given rows for each
   * shard.
   */
  private void stubDispatcherWithRows(List<List<Object[]>> perShardRows) {
    doAnswer(
            invocation -> {
              int shardId = invocation.getArgument(2);
              ActionListener<ShardQueryDispatcher.ShardResponse> listener =
                  invocation.getArgument(3);
              List<Object[]> rows =
                  shardId < perShardRows.size() ? perShardRows.get(shardId) : List.of();
              listener.onResponse(new ShardQueryDispatcher.ShardResponse(rows));
              return null;
            })
        .when(dispatcher)
        .dispatch(anyString(), anyString(), anyInt(), any());
  }

  /**
   * Configures the mock dispatcher to call onResponse with an error for a specific shard and
   * success for others.
   */
  private void stubDispatcherWithErrorOnShard(
      int errorShardId, Exception error, List<Object[]> successRows) {
    doAnswer(
            invocation -> {
              int shardId = invocation.getArgument(2);
              ActionListener<ShardQueryDispatcher.ShardResponse> listener =
                  invocation.getArgument(3);
              if (shardId == errorShardId) {
                listener.onResponse(new ShardQueryDispatcher.ShardResponse(error));
              } else {
                listener.onResponse(new ShardQueryDispatcher.ShardResponse(successRows));
              }
              return null;
            })
        .when(dispatcher)
        .dispatch(anyString(), anyString(), anyInt(), any());
  }

  /** Configures the mock dispatcher to call onFailure (transport error) for a specific shard. */
  private void stubDispatcherWithTransportError(
      int errorShardId, Exception transportError, List<Object[]> successRows) {
    doAnswer(
            invocation -> {
              int shardId = invocation.getArgument(2);
              ActionListener<ShardQueryDispatcher.ShardResponse> listener =
                  invocation.getArgument(3);
              if (shardId == errorShardId) {
                listener.onFailure(transportError);
              } else {
                listener.onResponse(new ShardQueryDispatcher.ShardResponse(successRows));
              }
              return null;
            })
        .when(dispatcher)
        .dispatch(anyString(), anyString(), anyInt(), any());
  }

  // ============= Dispatch tests =============

  @Test
  @DisplayName("DistributedExecutor dispatches to N shards and collects N responses")
  void dispatchToNShards() {
    AbstractCalciteIndexScan scan = mockScanNode();
    ConcatExchange exchange = new ConcatExchange(scan, rowType);
    PlanSplitter.ExchangeLeaf coordinator = exchangeLeaf(exchange);
    DistributedPlan plan =
        new DistributedPlan(coordinator, Collections.singletonList(exchange));

    when(resolver.resolve("test_index")).thenReturn(createShardIds(3));

    List<Object[]> shard0Rows = rows(new Object[] {"Alice", 30});
    List<Object[]> shard1Rows = rows(new Object[] {"Bob", 25});
    List<Object[]> shard2Rows = rows(new Object[] {"Carol", 35});
    stubDispatcherWithRows(List.of(shard0Rows, shard1Rows, shard2Rows));

    AtomicReference<QueryResponse> responseRef = new AtomicReference<>();
    executor.execute(
        plan,
        null,
        new ResponseListener<>() {
          @Override
          public void onResponse(QueryResponse response) {
            responseRef.set(response);
          }

          @Override
          public void onFailure(Exception e) {
            throw new AssertionError("Expected success but got failure", e);
          }
        });

    assertNotNull(responseRef.get());
    assertEquals(3, responseRef.get().getResults().size());
    verify(dispatcher, times(3)).dispatch(anyString(), eq("test_index"), anyInt(), any());
  }

  // ============= ConcatExchange integration tests =============

  @Test
  @DisplayName("ConcatExchange integration: rows from all shards are concatenated")
  void concatExchangeIntegration() {
    AbstractCalciteIndexScan scan = mockScanNode();
    ConcatExchange exchange = new ConcatExchange(scan, rowType);
    PlanSplitter.ExchangeLeaf coordinator = exchangeLeaf(exchange);
    DistributedPlan plan =
        new DistributedPlan(coordinator, Collections.singletonList(exchange));

    when(resolver.resolve("test_index")).thenReturn(createShardIds(2));

    stubDispatcherWithRows(
        List.of(
            rows(new Object[] {"Alice", 30}, new Object[] {"Bob", 25}),
            rows(new Object[] {"Carol", 35})));

    AtomicReference<QueryResponse> responseRef = new AtomicReference<>();
    executor.execute(
        plan,
        null,
        new ResponseListener<>() {
          @Override
          public void onResponse(QueryResponse response) {
            responseRef.set(response);
          }

          @Override
          public void onFailure(Exception e) {
            throw new AssertionError("Expected success but got failure", e);
          }
        });

    QueryResponse response = responseRef.get();
    assertNotNull(response);
    assertEquals(3, response.getResults().size());
    assertEquals(2, response.getSchema().getColumns().size());
    assertEquals("name", response.getSchema().getColumns().get(0).getName());
    assertEquals("age", response.getSchema().getColumns().get(1).getName());
  }

  // ============= MergeAggregateExchange integration tests =============

  @Test
  @DisplayName("MergeAggregateExchange integration: partial aggregates are merged")
  void mergeAggregateExchangeIntegration() {
    AbstractCalciteIndexScan scan = mockScanNode();
    RelDataType aggRowType =
        typeFactory.createStructType(
            List.of(
                typeFactory.createSqlType(SqlTypeName.VARCHAR, 100),
                typeFactory.createSqlType(SqlTypeName.BIGINT)),
            List.of("department", "cnt"));

    // SUM_COUNTS merge function for COUNT
    MergeAggregateExchange exchange =
        new MergeAggregateExchange(
            scan,
            aggRowType,
            List.of(
                org.opensearch.sql.opensearch.dqe.exchange.MergeFunction.SUM_COUNTS),
            1);

    PlanSplitter.ExchangeLeaf coordinator = exchangeLeaf(exchange);
    DistributedPlan plan =
        new DistributedPlan(coordinator, Collections.singletonList(exchange));

    when(resolver.resolve("test_index")).thenReturn(createShardIds(2));

    // Shard 0: Engineering=3, Sales=2
    // Shard 1: Engineering=5, Sales=1
    stubDispatcherWithRows(
        List.of(
            rows(new Object[] {"Engineering", 3L}, new Object[] {"Sales", 2L}),
            rows(new Object[] {"Engineering", 5L}, new Object[] {"Sales", 1L})));

    AtomicReference<QueryResponse> responseRef = new AtomicReference<>();
    executor.execute(
        plan,
        null,
        new ResponseListener<>() {
          @Override
          public void onResponse(QueryResponse response) {
            responseRef.set(response);
          }

          @Override
          public void onFailure(Exception e) {
            throw new AssertionError("Expected success but got failure", e);
          }
        });

    QueryResponse response = responseRef.get();
    assertNotNull(response);
    assertEquals(2, response.getResults().size());
  }

  // ============= MergeSortExchange integration tests =============

  @Test
  @DisplayName("MergeSortExchange integration: sorted and limited results")
  void mergeSortExchangeIntegration() {
    AbstractCalciteIndexScan scan = mockScanNode();
    RelDataType sortRowType =
        typeFactory.createStructType(
            List.of(
                typeFactory.createSqlType(SqlTypeName.VARCHAR, 100),
                typeFactory.createSqlType(SqlTypeName.INTEGER)),
            List.of("name", "age"));

    org.apache.calcite.rel.RelFieldCollation collation =
        new org.apache.calcite.rel.RelFieldCollation(
            1,
            org.apache.calcite.rel.RelFieldCollation.Direction.ASCENDING,
            org.apache.calcite.rel.RelFieldCollation.NullDirection.LAST);

    MergeSortExchange exchange =
        new MergeSortExchange(scan, sortRowType, List.of(collation), 3);

    PlanSplitter.ExchangeLeaf coordinator = exchangeLeaf(exchange);
    DistributedPlan plan =
        new DistributedPlan(coordinator, Collections.singletonList(exchange));

    when(resolver.resolve("test_index")).thenReturn(createShardIds(2));

    // Each shard returns sorted by age ASC
    stubDispatcherWithRows(
        List.of(
            rows(
                new Object[] {"Alice", 20},
                new Object[] {"Bob", 30},
                new Object[] {"Carol", 40}),
            rows(
                new Object[] {"Dave", 25},
                new Object[] {"Eve", 35},
                new Object[] {"Frank", 45})));

    AtomicReference<QueryResponse> responseRef = new AtomicReference<>();
    executor.execute(
        plan,
        null,
        new ResponseListener<>() {
          @Override
          public void onResponse(QueryResponse response) {
            responseRef.set(response);
          }

          @Override
          public void onFailure(Exception e) {
            throw new AssertionError("Expected success but got failure", e);
          }
        });

    QueryResponse response = responseRef.get();
    assertNotNull(response);
    // Limit 3: should return Alice(20), Dave(25), Bob(30)
    assertEquals(3, response.getResults().size());
  }

  // ============= Empty result tests =============

  @Test
  @DisplayName("All shards return empty results")
  void allShardsReturnEmpty() {
    AbstractCalciteIndexScan scan = mockScanNode();
    ConcatExchange exchange = new ConcatExchange(scan, rowType);
    PlanSplitter.ExchangeLeaf coordinator = exchangeLeaf(exchange);
    DistributedPlan plan =
        new DistributedPlan(coordinator, Collections.singletonList(exchange));

    when(resolver.resolve("test_index")).thenReturn(createShardIds(3));

    stubDispatcherWithRows(List.of(List.of(), List.of(), List.of()));

    AtomicReference<QueryResponse> responseRef = new AtomicReference<>();
    executor.execute(
        plan,
        null,
        new ResponseListener<>() {
          @Override
          public void onResponse(QueryResponse response) {
            responseRef.set(response);
          }

          @Override
          public void onFailure(Exception e) {
            throw new AssertionError("Expected success but got failure", e);
          }
        });

    QueryResponse response = responseRef.get();
    assertNotNull(response);
    assertEquals(0, response.getResults().size());
    assertEquals(2, response.getSchema().getColumns().size());
  }

  // ============= Fail-fast tests (SC-1.11) =============

  @Test
  @DisplayName("One shard returns error -> entire query fails with error propagated")
  void oneShardError_failFast() {
    AbstractCalciteIndexScan scan = mockScanNode();
    ConcatExchange exchange = new ConcatExchange(scan, rowType);
    RelNode mockCoordinator = mock(RelNode.class, RETURNS_DEEP_STUBS);
    DistributedPlan plan =
        new DistributedPlan(mockCoordinator, Collections.singletonList(exchange));

    when(resolver.resolve("test_index")).thenReturn(createShardIds(3));

    Exception shardError = new RuntimeException("Shard execution failed: out of memory");
    stubDispatcherWithErrorOnShard(1, shardError, rows(new Object[] {"Alice", 30}));

    AtomicReference<Exception> failureRef = new AtomicReference<>();
    executor.execute(
        plan,
        null,
        new ResponseListener<>() {
          @Override
          public void onResponse(QueryResponse response) {
            throw new AssertionError("Expected failure but got success");
          }

          @Override
          public void onFailure(Exception e) {
            failureRef.set(e);
          }
        });

    assertNotNull(failureRef.get());
    assertTrue(failureRef.get().getMessage().contains("Shard [1]"));
    assertTrue(failureRef.get().getMessage().contains("test_index"));
    assertTrue(failureRef.get().getMessage().contains("out of memory"));
  }

  @Test
  @DisplayName("Serialization failure -> query fails with clear message")
  void serializationFailure_clearMessage() {
    // Create a scan that will cause serialization to fail
    RelNode badPlan = mock(RelNode.class, RETURNS_DEEP_STUBS);
    // No AbstractCalciteIndexScan means extractIndexName will fail
    when(badPlan.getInputs()).thenReturn(Collections.emptyList());

    ConcatExchange exchange = new ConcatExchange(badPlan, rowType);
    RelNode mockCoordinator = mock(RelNode.class, RETURNS_DEEP_STUBS);
    DistributedPlan plan =
        new DistributedPlan(mockCoordinator, Collections.singletonList(exchange));

    AtomicReference<Exception> failureRef = new AtomicReference<>();
    executor.execute(
        plan,
        null,
        new ResponseListener<>() {
          @Override
          public void onResponse(QueryResponse response) {
            throw new AssertionError("Expected failure but got success");
          }

          @Override
          public void onFailure(Exception e) {
            failureRef.set(e);
          }
        });

    assertNotNull(failureRef.get());
    // Should fail at extractIndexName step
    assertTrue(
        failureRef.get().getMessage().contains("No AbstractCalciteIndexScan found")
            || failureRef.get().getMessage().contains("Failed to serialize"));
  }

  @Test
  @DisplayName("Transport error (shard unreachable) -> query fails, no silent fallback")
  void transportError_noSilentFallback() {
    AbstractCalciteIndexScan scan = mockScanNode();
    ConcatExchange exchange = new ConcatExchange(scan, rowType);
    RelNode mockCoordinator = mock(RelNode.class, RETURNS_DEEP_STUBS);
    DistributedPlan plan =
        new DistributedPlan(mockCoordinator, Collections.singletonList(exchange));

    when(resolver.resolve("test_index")).thenReturn(createShardIds(2));

    Exception transportError = new RuntimeException("Connection refused: node_1");
    stubDispatcherWithTransportError(
        1, transportError, rows(new Object[] {"Alice", 30}));

    AtomicReference<Exception> failureRef = new AtomicReference<>();
    executor.execute(
        plan,
        null,
        new ResponseListener<>() {
          @Override
          public void onResponse(QueryResponse response) {
            throw new AssertionError("Expected failure but got success");
          }

          @Override
          public void onFailure(Exception e) {
            failureRef.set(e);
          }
        });

    assertNotNull(failureRef.get());
    assertTrue(failureRef.get().getMessage().contains("Transport error"));
    assertTrue(failureRef.get().getMessage().contains("shard [1]"));
    assertTrue(failureRef.get().getMessage().contains("Connection refused"));
  }

  @Test
  @DisplayName("Error message includes shard ID and root cause")
  void errorMessageIncludesShardIdAndRootCause() {
    AbstractCalciteIndexScan scan = mockScanNode();
    ConcatExchange exchange = new ConcatExchange(scan, rowType);
    RelNode mockCoordinator = mock(RelNode.class, RETURNS_DEEP_STUBS);
    DistributedPlan plan =
        new DistributedPlan(mockCoordinator, Collections.singletonList(exchange));

    when(resolver.resolve("test_index")).thenReturn(createShardIds(3));

    Exception rootCause = new IllegalStateException("Index corrupted");
    stubDispatcherWithErrorOnShard(2, rootCause, rows(new Object[] {"Alice", 30}));

    AtomicReference<Exception> failureRef = new AtomicReference<>();
    executor.execute(
        plan,
        null,
        new ResponseListener<>() {
          @Override
          public void onResponse(QueryResponse response) {
            throw new AssertionError("Expected failure but got success");
          }

          @Override
          public void onFailure(Exception e) {
            failureRef.set(e);
          }
        });

    Exception failure = failureRef.get();
    assertNotNull(failure);
    // Verify shard ID is in the message
    assertTrue(failure.getMessage().contains("Shard [2]"));
    // Verify index name is in the message
    assertTrue(failure.getMessage().contains("test_index"));
    // Verify root cause message is included
    assertTrue(failure.getMessage().contains("Index corrupted"));
    // Verify root cause is chained
    assertNotNull(failure.getCause());
  }

  // ============= Helper to avoid varargs type confusion =============

  /** Helper to create a typed List<Object[]> without varargs confusion. */
  private static List<Object[]> rows(Object[]... rows) {
    return Arrays.asList(rows);
  }

  // ============= extractIndexName tests =============

  @Test
  @DisplayName("extractIndexName finds scan in simple plan")
  void extractIndexNameSimplePlan() {
    AbstractCalciteIndexScan scan = mockScanNode();
    String indexName = DistributedExecutor.extractIndexName(scan);
    assertEquals("test_index", indexName);
  }

  @Test
  @DisplayName("extractIndexName finds scan in nested plan")
  void extractIndexNameNestedPlan() {
    AbstractCalciteIndexScan scan = mockScanNode();
    RelNode parent = mock(RelNode.class);
    when(parent.getInputs()).thenReturn(Collections.singletonList(scan));
    String indexName = DistributedExecutor.extractIndexName(parent);
    assertEquals("test_index", indexName);
  }
}
