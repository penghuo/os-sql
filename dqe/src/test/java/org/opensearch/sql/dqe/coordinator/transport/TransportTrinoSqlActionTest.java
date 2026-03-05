/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.coordinator.transport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.airlift.slice.Slices;
import io.trino.spi.Page;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.sql.dqe.common.config.DqeSettings;
import org.opensearch.sql.dqe.planner.plan.AggregationNode;
import org.opensearch.sql.dqe.planner.plan.ProjectNode;
import org.opensearch.sql.dqe.planner.plan.TableScanNode;
import org.opensearch.sql.dqe.shard.transport.ShardExecuteAction;
import org.opensearch.sql.dqe.shard.transport.ShardExecuteRequest;
import org.opensearch.sql.dqe.shard.transport.ShardExecuteResponse;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.TransportService;

@DisplayName("TransportTrinoSqlAction coordinator orchestration")
class TransportTrinoSqlActionTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Test
  @DisplayName(
      "Full pipeline: simple SELECT dispatches to shards via transport and returns response")
  void fullPipelineSimpleSelect() throws Exception {
    ClusterService clusterService = mockClusterService("logs", 2, Map.of("status", "long"));
    TransportService transportService = mock(TransportService.class);

    // Mock transport: when sendRequest is called, respond with test pages
    doAnswer(
            invocation -> {
              TransportResponseHandler<ShardExecuteResponse> handler = invocation.getArgument(3);
              // Return a page with 3 rows of BIGINT values (0, 1, 2)
              BlockBuilder builder = BigintType.BIGINT.createBlockBuilder(null, 3);
              for (int i = 0; i < 3; i++) {
                BigintType.BIGINT.writeLong(builder, i);
              }
              Page page = new Page(builder.build());
              handler.handleResponse(
                  new ShardExecuteResponse(List.of(page), List.of(BigintType.BIGINT)));
              return null;
            })
        .when(transportService)
        .sendRequest(any(DiscoveryNode.class), eq(ShardExecuteAction.NAME), any(), any());

    TransportTrinoSqlAction action =
        new TransportTrinoSqlAction(
            transportService, new ActionFilters(Collections.emptySet()), clusterService);

    TrinoSqlRequest request = new TrinoSqlRequest("SELECT status FROM logs", false);
    TrinoSqlResponse response = executeSync(action, request);

    assertNotNull(response);
    assertNotNull(response.getResult());
    assertEquals("application/json; charset=UTF-8", response.getContentType());

    Map<String, Object> parsed = MAPPER.readValue(response.getResult(), new TypeReference<>() {});
    assertTrue(parsed.containsKey("schema"), "Response should contain schema");
    assertTrue(parsed.containsKey("datarows"), "Response should contain datarows");
    assertEquals(200, ((Number) parsed.get("status")).intValue());

    // 2 shards x 3 rows = 6 total rows (passthrough merge)
    List<?> datarows = (List<?>) parsed.get("datarows");
    assertEquals(6, datarows.size());

    // Verify transport was called for each shard
    verify(transportService, times(2))
        .sendRequest(any(DiscoveryNode.class), eq(ShardExecuteAction.NAME), any(), any());
  }

  @Test
  @DisplayName("Transport dispatch sends request to correct target nodes")
  void transportDispatchTargetsCorrectNodes() throws Exception {
    ClusterService clusterService = mockClusterService("logs", 2, Map.of("status", "long"));
    TransportService transportService = mock(TransportService.class);

    doAnswer(
            invocation -> {
              TransportResponseHandler<ShardExecuteResponse> handler = invocation.getArgument(3);
              BlockBuilder builder = BigintType.BIGINT.createBlockBuilder(null, 1);
              BigintType.BIGINT.writeLong(builder, 1L);
              Page page = new Page(builder.build());
              handler.handleResponse(
                  new ShardExecuteResponse(List.of(page), List.of(BigintType.BIGINT)));
              return null;
            })
        .when(transportService)
        .sendRequest(any(DiscoveryNode.class), eq(ShardExecuteAction.NAME), any(), any());

    TransportTrinoSqlAction action =
        new TransportTrinoSqlAction(
            transportService, new ActionFilters(Collections.emptySet()), clusterService);

    TrinoSqlRequest request = new TrinoSqlRequest("SELECT status FROM logs", false);
    executeSync(action, request);

    // Capture the DiscoveryNode arguments
    ArgumentCaptor<DiscoveryNode> nodeCaptor = ArgumentCaptor.forClass(DiscoveryNode.class);
    verify(transportService, times(2))
        .sendRequest(nodeCaptor.capture(), eq(ShardExecuteAction.NAME), any(), any());

    List<DiscoveryNode> targetNodes = nodeCaptor.getAllValues();
    assertEquals(2, targetNodes.size());
  }

  @Test
  @DisplayName("Explain mode returns plan description without dispatching to shards")
  void explainMode() throws Exception {
    ClusterService clusterService = mockClusterService("logs", 2, Map.of("status", "long"));
    TransportService transportService = mock(TransportService.class);

    TransportTrinoSqlAction action =
        new TransportTrinoSqlAction(
            transportService, new ActionFilters(Collections.emptySet()), clusterService);

    TrinoSqlRequest request = new TrinoSqlRequest("SELECT status FROM logs", true);
    TrinoSqlResponse response = executeSync(action, request);

    assertNotNull(response);
    String result = response.getResult();
    assertNotNull(result);
    assertTrue(result.contains("plan"), "Explain result should contain 'plan'");
    assertTrue(result.contains("TableScanNode"), "Explain result should describe TableScanNode");
    assertTrue(result.contains("ProjectNode"), "Explain result should describe ProjectNode");
    assertTrue(result.contains("logs"), "Explain result should mention index name");

    // Verify no transport dispatch happened in explain mode
    verify(transportService, times(0)).sendRequest(any(), any(String.class), any(), any());
  }

  @Test
  @DisplayName("Invalid SQL triggers onFailure")
  void invalidSqlTriggersFailure() throws Exception {
    ClusterService clusterService = mockClusterService("logs", 2, Map.of("status", "long"));
    TransportService transportService = mock(TransportService.class);

    TransportTrinoSqlAction action =
        new TransportTrinoSqlAction(
            transportService, new ActionFilters(Collections.emptySet()), clusterService);

    TrinoSqlRequest request = new TrinoSqlRequest("NOT VALID SQL AT ALL", false);

    AtomicReference<TrinoSqlResponse> responseRef = new AtomicReference<>();
    AtomicReference<Exception> errorRef = new AtomicReference<>();
    CountDownLatch latch = new CountDownLatch(1);

    action.doExecute(
        null,
        request,
        new ActionListener<>() {
          @Override
          public void onResponse(TrinoSqlResponse response) {
            responseRef.set(response);
            latch.countDown();
          }

          @Override
          public void onFailure(Exception e) {
            errorRef.set(e);
            latch.countDown();
          }
        });

    assertTrue(latch.await(5, TimeUnit.SECONDS));
    assertNotNull(errorRef.get(), "Invalid SQL should trigger onFailure");
  }

  @Test
  @DisplayName("formatResponse produces valid JSON with schema and datarows from Pages")
  void formatResponseProducesValidJson() throws Exception {
    BlockBuilder nameBuilder = VarcharType.VARCHAR.createBlockBuilder(null, 2);
    VarcharType.VARCHAR.writeSlice(nameBuilder, Slices.utf8Slice("Alice"));
    VarcharType.VARCHAR.writeSlice(nameBuilder, Slices.utf8Slice("Bob"));

    BlockBuilder ageBuilder = BigintType.BIGINT.createBlockBuilder(null, 2);
    BigintType.BIGINT.writeLong(ageBuilder, 30L);
    BigintType.BIGINT.writeLong(ageBuilder, 25L);

    Page page = new Page(nameBuilder.build(), ageBuilder.build());
    List<String> columnNames = List.of("name", "age");
    List<Type> columnTypes = List.of(VarcharType.VARCHAR, BigintType.BIGINT);

    String json = TransportTrinoSqlAction.formatResponse(List.of(page), columnNames, columnTypes);
    Map<String, Object> parsed = MAPPER.readValue(json, new TypeReference<>() {});

    List<?> schema = (List<?>) parsed.get("schema");
    assertEquals(2, schema.size());

    List<?> datarows = (List<?>) parsed.get("datarows");
    assertEquals(2, datarows.size());

    assertEquals(2, ((Number) parsed.get("total")).intValue());
    assertEquals(200, ((Number) parsed.get("status")).intValue());
  }

  @Test
  @DisplayName("formatExplain produces JSON with plan key")
  void formatExplainProducesJson() throws Exception {
    TableScanNode scan = new TableScanNode("logs", List.of("a", "b"));
    ProjectNode project = new ProjectNode(scan, List.of("a"));

    String explain = TransportTrinoSqlAction.formatExplain(project);
    Map<String, Object> parsed = MAPPER.readValue(explain, new TypeReference<>() {});

    assertTrue(parsed.containsKey("plan"));
    String planStr = (String) parsed.get("plan");
    assertTrue(planStr.contains("ProjectNode"));
    assertTrue(planStr.contains("TableScanNode"));
  }

  @Test
  @DisplayName("resolveColumnNames returns columns from ProjectNode")
  void resolveColumnNamesFromProject() {
    TableScanNode scan = new TableScanNode("logs", List.of("a", "b", "c"));
    ProjectNode project = new ProjectNode(scan, List.of("a", "c"));

    assertEquals(List.of("a", "c"), TransportTrinoSqlAction.resolveColumnNames(project));
  }

  @Test
  @DisplayName("resolveColumnNames returns columns from AggregationNode")
  void resolveColumnNamesFromAggregation() {
    TableScanNode scan = new TableScanNode("logs", List.of("category", "value"));
    AggregationNode agg =
        new AggregationNode(
            scan, List.of("category"), List.of("COUNT(*)"), AggregationNode.Step.PARTIAL);

    assertEquals(List.of("category", "COUNT(*)"), TransportTrinoSqlAction.resolveColumnNames(agg));
  }

  @Test
  @DisplayName("doExecute fails with IllegalAccessException when DQE is disabled")
  void dqeDisabledRejectsQuery() throws Exception {
    Settings disabledSettings =
        Settings.builder().put("plugins.dqe.enabled", false).build();
    ClusterService clusterService =
        mockClusterService("logs", 2, Map.of("status", "long"), disabledSettings);
    TransportService transportService = mock(TransportService.class);

    TransportTrinoSqlAction action =
        new TransportTrinoSqlAction(
            transportService, new ActionFilters(Collections.emptySet()), clusterService);

    TrinoSqlRequest request = new TrinoSqlRequest("SELECT status FROM logs", false);

    AtomicReference<Exception> errorRef = new AtomicReference<>();
    CountDownLatch latch = new CountDownLatch(1);

    action.doExecute(
        null,
        request,
        new ActionListener<>() {
          @Override
          public void onResponse(TrinoSqlResponse response) {
            latch.countDown();
          }

          @Override
          public void onFailure(Exception e) {
            errorRef.set(e);
            latch.countDown();
          }
        });

    assertTrue(latch.await(5, TimeUnit.SECONDS));
    assertNotNull(errorRef.get(), "Should fail when DQE is disabled");
    assertInstanceOf(IllegalAccessException.class, errorRef.get());
    assertTrue(errorRef.get().getMessage().contains("DQE is disabled"));

    // Verify no transport dispatch happened
    verify(transportService, times(0)).sendRequest(any(), any(String.class), any(), any());
  }

  @Test
  @DisplayName("doExecute reads query timeout from cluster settings")
  void queryTimeoutReadFromSettings() throws Exception {
    // Configure a custom timeout of 10 seconds
    Settings customSettings =
        Settings.builder()
            .put("plugins.dqe.enabled", true)
            .put("plugins.dqe.query.timeout", "10s")
            .build();
    ClusterService clusterService =
        mockClusterService("logs", 2, Map.of("status", "long"), customSettings);
    TransportService transportService = mock(TransportService.class);

    // Capture the ShardExecuteRequest to verify the timeout value
    ArgumentCaptor<ShardExecuteRequest> requestCaptor =
        ArgumentCaptor.forClass(ShardExecuteRequest.class);

    doAnswer(
            invocation -> {
              TransportResponseHandler<ShardExecuteResponse> handler = invocation.getArgument(3);
              BlockBuilder builder = BigintType.BIGINT.createBlockBuilder(null, 1);
              BigintType.BIGINT.writeLong(builder, 1L);
              Page page = new Page(builder.build());
              handler.handleResponse(
                  new ShardExecuteResponse(List.of(page), List.of(BigintType.BIGINT)));
              return null;
            })
        .when(transportService)
        .sendRequest(any(DiscoveryNode.class), eq(ShardExecuteAction.NAME), any(), any());

    TransportTrinoSqlAction action =
        new TransportTrinoSqlAction(
            transportService, new ActionFilters(Collections.emptySet()), clusterService);

    TrinoSqlRequest request = new TrinoSqlRequest("SELECT status FROM logs", false);
    executeSync(action, request);

    // Capture the request sent to transport
    verify(transportService, times(2))
        .sendRequest(
            any(DiscoveryNode.class),
            eq(ShardExecuteAction.NAME),
            requestCaptor.capture(),
            any());

    // Verify the timeout in the shard request is 10000ms (10 seconds)
    ShardExecuteRequest capturedReq = requestCaptor.getValue();
    assertEquals(10000L, capturedReq.getTimeoutMillis());
  }

  // -- Helper methods --

  /** Execute a TransportTrinoSqlAction synchronously and return the response. */
  private TrinoSqlResponse executeSync(TransportTrinoSqlAction action, TrinoSqlRequest request)
      throws Exception {
    AtomicReference<TrinoSqlResponse> responseRef = new AtomicReference<>();
    AtomicReference<Exception> errorRef = new AtomicReference<>();
    CountDownLatch latch = new CountDownLatch(1);

    action.doExecute(
        null,
        request,
        new ActionListener<>() {
          @Override
          public void onResponse(TrinoSqlResponse response) {
            responseRef.set(response);
            latch.countDown();
          }

          @Override
          public void onFailure(Exception e) {
            errorRef.set(e);
            latch.countDown();
          }
        });

    assertTrue(latch.await(10, TimeUnit.SECONDS), "Action should complete within timeout");
    if (errorRef.get() != null) {
      throw errorRef.get();
    }
    return responseRef.get();
  }

  /**
   * Create a mock ClusterService that provides metadata, routing, DiscoveryNodes, and DQE-enabled
   * settings for the given index.
   */
  @SuppressWarnings("unchecked")
  private ClusterService mockClusterService(
      String indexName, int numShards, Map<String, String> fieldTypes) {
    return mockClusterService(
        indexName,
        numShards,
        fieldTypes,
        Settings.builder().put("plugins.dqe.enabled", true).build());
  }

  /**
   * Create a mock ClusterService with custom settings for testing settings enforcement.
   */
  @SuppressWarnings("unchecked")
  private ClusterService mockClusterService(
      String indexName, int numShards, Map<String, String> fieldTypes, Settings settings) {
    ClusterService clusterService = mock(ClusterService.class);
    ClusterState clusterState = mock(ClusterState.class);
    when(clusterService.state()).thenReturn(clusterState);
    when(clusterService.getSettings()).thenReturn(settings);

    // Mock ClusterSettings for dynamic settings update consumer registration
    Set<org.opensearch.common.settings.Setting<?>> settingSet = new HashSet<>(DqeSettings.settings());
    ClusterSettings clusterSettings = new ClusterSettings(settings, settingSet);
    when(clusterService.getClusterSettings()).thenReturn(clusterSettings);

    // Mock metadata for OpenSearchMetadata
    Metadata metadata = mock(Metadata.class);
    IndexMetadata indexMetadata = mock(IndexMetadata.class);
    MappingMetadata mappingMetadata = mock(MappingMetadata.class);

    when(clusterState.metadata()).thenReturn(metadata);
    when(metadata.index(indexName)).thenReturn(indexMetadata);
    when(indexMetadata.mapping()).thenReturn(mappingMetadata);

    Map<String, Object> properties = new HashMap<>();
    for (Map.Entry<String, String> entry : fieldTypes.entrySet()) {
      properties.put(entry.getKey(), Map.of("type", entry.getValue()));
    }
    when(mappingMetadata.sourceAsMap()).thenReturn(Map.of("properties", properties));

    // Mock routing for PlanFragmenter
    RoutingTable routingTable = mock(RoutingTable.class);
    IndexRoutingTable indexRoutingTable = mock(IndexRoutingTable.class);

    when(clusterState.routingTable()).thenReturn(routingTable);
    when(routingTable.index(indexName)).thenReturn(indexRoutingTable);

    // Mock DiscoveryNodes
    DiscoveryNodes discoveryNodes = mock(DiscoveryNodes.class);
    when(clusterState.nodes()).thenReturn(discoveryNodes);

    Map<Integer, IndexShardRoutingTable> shardMap = new HashMap<>();
    for (int i = 0; i < numShards; i++) {
      IndexShardRoutingTable shardRoutingTable = mock(IndexShardRoutingTable.class);
      ShardRouting primaryShard = mock(ShardRouting.class);
      String nodeId = "node-" + i;
      when(primaryShard.currentNodeId()).thenReturn(nodeId);
      when(shardRoutingTable.primaryShard()).thenReturn(primaryShard);
      shardMap.put(i, shardRoutingTable);

      // Mock DiscoveryNode for each node
      DiscoveryNode discoveryNode = mock(DiscoveryNode.class);
      when(discoveryNodes.get(nodeId)).thenReturn(discoveryNode);
    }
    when(indexRoutingTable.shards()).thenReturn(shardMap);

    return clusterService;
  }
}
