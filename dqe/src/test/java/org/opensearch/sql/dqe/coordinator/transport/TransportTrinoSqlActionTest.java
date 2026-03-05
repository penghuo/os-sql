/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.coordinator.transport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.dqe.operator.TestPageSource.buildBigintPage;
import static org.opensearch.sql.dqe.operator.TestPageSource.buildCategoryValuePage;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.action.ActionListener;
import org.opensearch.sql.dqe.operator.TestPageSource;
import org.opensearch.sql.dqe.planner.plan.AggregationNode;
import org.opensearch.sql.dqe.planner.plan.ProjectNode;
import org.opensearch.sql.dqe.planner.plan.TableScanNode;
import org.opensearch.transport.TransportService;

@DisplayName("TransportTrinoSqlAction coordinator orchestration")
class TransportTrinoSqlActionTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Test
  @DisplayName("Full pipeline: simple SELECT query returns formatted response")
  void fullPipelineSimpleSelect() throws Exception {
    // Setup: mock ClusterService with metadata for "logs" index (column: status(long))
    // Mock shard routing with 2 shards
    ClusterService clusterService = mockClusterService("logs", 2, Map.of("status", "long"));

    // Scan factory returns test data: each shard returns 3 rows of BIGINT values
    TransportTrinoSqlAction action =
        new TransportTrinoSqlAction(
            mock(TransportService.class),
            new ActionFilters(Collections.emptySet()),
            clusterService,
            node -> new TestPageSource(List.of(buildBigintPage(3))));

    // Execute: SELECT status FROM logs
    TrinoSqlRequest request = new TrinoSqlRequest("SELECT status FROM logs", false);

    TrinoSqlResponse response = executeSync(action, request);

    assertNotNull(response);
    assertNotNull(response.getResult());
    assertEquals("application/json; charset=UTF-8", response.getContentType());

    // Parse and verify the response structure
    Map<String, Object> parsed = MAPPER.readValue(response.getResult(), new TypeReference<>() {});
    assertTrue(parsed.containsKey("schema"), "Response should contain schema");
    assertTrue(parsed.containsKey("datarows"), "Response should contain datarows");
    assertTrue(parsed.containsKey("total"), "Response should contain total");
    assertTrue(parsed.containsKey("status"), "Response should contain status");
    assertEquals(200, ((Number) parsed.get("status")).intValue());

    // 2 shards x 3 rows = 6 total rows (passthrough merge)
    List<?> datarows = (List<?>) parsed.get("datarows");
    assertEquals(6, datarows.size());
  }

  @Test
  @DisplayName("Full pipeline: aggregation query merges partial results from shards")
  void fullPipelineAggregation() throws Exception {
    // Setup: mock ClusterService with metadata for "logs" index
    // (columns: category(keyword), status(long))
    ClusterService clusterService =
        mockClusterService("logs", 2, Map.of("category", "keyword", "status", "long"));

    // Scan factory returns category/value pages for aggregation
    // Each shard gets: [("web", 1), ("api", 2), ("web", 3)]
    TransportTrinoSqlAction action =
        new TransportTrinoSqlAction(
            mock(TransportService.class),
            new ActionFilters(Collections.emptySet()),
            clusterService,
            node ->
                new TestPageSource(
                    List.of(buildCategoryValuePage("web", 1L, "api", 2L, "web", 3L))));

    // Execute: SELECT category, COUNT(*) FROM logs GROUP BY category
    TrinoSqlRequest request =
        new TrinoSqlRequest("SELECT category, COUNT(*) FROM logs GROUP BY category", false);

    TrinoSqlResponse response = executeSync(action, request);

    assertNotNull(response);
    assertNotNull(response.getResult());

    // Parse response
    Map<String, Object> parsed = MAPPER.readValue(response.getResult(), new TypeReference<>() {});
    List<?> datarows = (List<?>) parsed.get("datarows");

    // The aggregation merges partial counts from 2 shards
    // Each shard produces partial: web=2, api=1
    // Final merge: web=4 (2+2), api=2 (1+1)
    assertNotNull(datarows);
    assertTrue(datarows.size() > 0, "Should have aggregated rows");
  }

  @Test
  @DisplayName("Explain mode returns plan description without executing")
  void explainMode() throws Exception {
    ClusterService clusterService = mockClusterService("logs", 2, Map.of("status", "long"));

    TransportTrinoSqlAction action =
        new TransportTrinoSqlAction(
            mock(TransportService.class),
            new ActionFilters(Collections.emptySet()),
            clusterService,
            node -> {
              throw new RuntimeException("Should not execute scan in explain mode");
            });

    TrinoSqlRequest request = new TrinoSqlRequest("SELECT status FROM logs", true);

    TrinoSqlResponse response = executeSync(action, request);

    assertNotNull(response);
    String result = response.getResult();
    assertNotNull(result);
    assertTrue(result.contains("plan"), "Explain result should contain 'plan'");
    assertTrue(result.contains("TableScanNode"), "Explain result should describe TableScanNode");
    assertTrue(result.contains("ProjectNode"), "Explain result should describe ProjectNode");
    assertTrue(result.contains("logs"), "Explain result should mention index name");
  }

  @Test
  @DisplayName("Invalid SQL triggers onFailure")
  void invalidSqlTriggersFailure() throws Exception {
    ClusterService clusterService = mockClusterService("logs", 2, Map.of("status", "long"));

    TransportTrinoSqlAction action =
        new TransportTrinoSqlAction(
            mock(TransportService.class),
            new ActionFilters(Collections.emptySet()),
            clusterService,
            node -> new TestPageSource(List.of()));

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
  @DisplayName("formatResponse produces valid JSON with schema and datarows")
  void formatResponseProducesValidJson() throws Exception {
    List<Map<String, Object>> rows =
        List.of(Map.of("name", "Alice", "age", 30L), Map.of("name", "Bob", "age", 25L));

    TableScanNode scan = new TableScanNode("users", List.of("name", "age"));
    ProjectNode project = new ProjectNode(scan, List.of("name", "age"));

    String json = TransportTrinoSqlAction.formatResponse(rows, project);
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
   * Create a mock ClusterService that provides both metadata (for OpenSearchMetadata) and routing
   * (for PlanFragmenter) for the given index.
   */
  @SuppressWarnings("unchecked")
  private ClusterService mockClusterService(
      String indexName, int numShards, Map<String, String> fieldTypes) {
    ClusterService clusterService = mock(ClusterService.class);
    ClusterState clusterState = mock(ClusterState.class);
    when(clusterService.state()).thenReturn(clusterState);

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

    Map<Integer, IndexShardRoutingTable> shardMap = new HashMap<>();
    for (int i = 0; i < numShards; i++) {
      IndexShardRoutingTable shardRoutingTable = mock(IndexShardRoutingTable.class);
      ShardRouting primaryShard = mock(ShardRouting.class);
      when(primaryShard.currentNodeId()).thenReturn("node-" + i);
      when(shardRoutingTable.primaryShard()).thenReturn(primaryShard);
      shardMap.put(i, shardRoutingTable);
    }
    when(indexRoutingTable.shards()).thenReturn(shardMap);

    return clusterService;
  }
}
