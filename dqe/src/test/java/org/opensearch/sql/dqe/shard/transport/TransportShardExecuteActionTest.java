/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.shard.transport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.opensearch.sql.dqe.operator.TestPageSource.buildBigintPage;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.action.ActionListener;
import org.opensearch.sql.dqe.operator.TestPageSource;
import org.opensearch.sql.dqe.planner.plan.DqePlanNode;
import org.opensearch.sql.dqe.planner.plan.LimitNode;
import org.opensearch.sql.dqe.planner.plan.TableScanNode;
import org.opensearch.transport.TransportService;

@DisplayName("TransportShardExecuteAction doExecute")
class TransportShardExecuteActionTest {

  @Test
  @DisplayName("doExecute with TableScan returns all rows as JSON")
  void doExecuteTableScan() throws Exception {
    // 1. Create a plan: TableScan with 3 BIGINT rows
    TableScanNode scan = new TableScanNode("logs", List.of("value"));
    byte[] fragmentBytes = serializePlan(scan);

    // 2. Create request
    ShardExecuteRequest request = new ShardExecuteRequest(fragmentBytes, "logs", 0, 30000L);

    // 3. Create action with a mock scan factory that returns test data
    TransportShardExecuteAction action =
        new TransportShardExecuteAction(
            mock(TransportService.class),
            new ActionFilters(Collections.emptySet()),
            node -> new TestPageSource(List.of(buildBigintPage(3))),
            Collections.emptyMap());

    // 4. Execute and capture response
    AtomicReference<ShardExecuteResponse> responseRef = new AtomicReference<>();
    AtomicReference<Exception> errorRef = new AtomicReference<>();
    CountDownLatch latch = new CountDownLatch(1);

    action.doExecute(
        null,
        request,
        new ActionListener<>() {
          @Override
          public void onResponse(ShardExecuteResponse response) {
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
    if (errorRef.get() != null) {
      throw errorRef.get();
    }

    ShardExecuteResponse response = responseRef.get();
    assertNotNull(response);
    String json = response.getResultJson();
    assertNotNull(json);
    // Should contain 3 rows with values 0, 1, 2
    assertTrue(json.contains("\"value\":0"), "JSON should contain value 0: " + json);
    assertTrue(json.contains("\"value\":1"), "JSON should contain value 1: " + json);
    assertTrue(json.contains("\"value\":2"), "JSON should contain value 2: " + json);
  }

  @Test
  @DisplayName("doExecute with Limit(TableScan) limits output rows")
  void doExecuteLimitedScan() throws Exception {
    // Plan: Limit(2) -> TableScan with 5 rows
    TableScanNode scan = new TableScanNode("logs", List.of("value"));
    LimitNode limit = new LimitNode(scan, 2);
    byte[] fragmentBytes = serializePlan(limit);

    ShardExecuteRequest request = new ShardExecuteRequest(fragmentBytes, "logs", 0, 30000L);

    TransportShardExecuteAction action =
        new TransportShardExecuteAction(
            mock(TransportService.class),
            new ActionFilters(Collections.emptySet()),
            node -> new TestPageSource(List.of(buildBigintPage(5))),
            Collections.emptyMap());

    AtomicReference<ShardExecuteResponse> responseRef = new AtomicReference<>();
    AtomicReference<Exception> errorRef = new AtomicReference<>();
    CountDownLatch latch = new CountDownLatch(1);

    action.doExecute(
        null,
        request,
        new ActionListener<>() {
          @Override
          public void onResponse(ShardExecuteResponse response) {
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
    if (errorRef.get() != null) {
      throw errorRef.get();
    }

    ShardExecuteResponse response = responseRef.get();
    assertNotNull(response);
    String json = response.getResultJson();
    // Should contain exactly 2 rows (limited from 5)
    assertTrue(json.contains("\"value\":0"), "JSON should contain value 0: " + json);
    assertTrue(json.contains("\"value\":1"), "JSON should contain value 1: " + json);
    // Should NOT contain rows 2-4
    assertTrue(!json.contains("\"value\":2"), "JSON should not contain value 2: " + json);
  }

  @Test
  @DisplayName("doExecute with empty result returns empty JSON array")
  void doExecuteEmptyResult() throws Exception {
    TableScanNode scan = new TableScanNode("logs", List.of("value"));
    byte[] fragmentBytes = serializePlan(scan);

    ShardExecuteRequest request = new ShardExecuteRequest(fragmentBytes, "logs", 0, 30000L);

    // Scan factory returns no pages
    TransportShardExecuteAction action =
        new TransportShardExecuteAction(
            mock(TransportService.class),
            new ActionFilters(Collections.emptySet()),
            node -> new TestPageSource(List.of()),
            Collections.emptyMap());

    AtomicReference<ShardExecuteResponse> responseRef = new AtomicReference<>();
    AtomicReference<Exception> errorRef = new AtomicReference<>();
    CountDownLatch latch = new CountDownLatch(1);

    action.doExecute(
        null,
        request,
        new ActionListener<>() {
          @Override
          public void onResponse(ShardExecuteResponse response) {
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
    if (errorRef.get() != null) {
      throw errorRef.get();
    }

    ShardExecuteResponse response = responseRef.get();
    assertNotNull(response);
    assertEquals("[]", response.getResultJson());
  }

  @Test
  @DisplayName("doExecute calls listener.onFailure when plan deserialization fails")
  void doExecuteFailure() throws Exception {
    // Invalid fragment bytes
    byte[] badFragment = new byte[] {0, 0, 0};
    ShardExecuteRequest request = new ShardExecuteRequest(badFragment, "logs", 0, 30000L);

    TransportShardExecuteAction action =
        new TransportShardExecuteAction(
            mock(TransportService.class),
            new ActionFilters(Collections.emptySet()),
            node -> new TestPageSource(List.of()),
            Collections.emptyMap());

    AtomicReference<ShardExecuteResponse> responseRef = new AtomicReference<>();
    AtomicReference<Exception> errorRef = new AtomicReference<>();
    CountDownLatch latch = new CountDownLatch(1);

    action.doExecute(
        null,
        request,
        new ActionListener<>() {
          @Override
          public void onResponse(ShardExecuteResponse response) {
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
    assertNotNull(errorRef.get(), "Should have called onFailure for invalid fragment");
  }

  @Test
  @DisplayName("toJson produces correct JSON for row maps")
  void toJsonProducesValidJson() {
    Map<String, Object> row1 = Map.of("a", 1L, "b", "hello");
    Map<String, Object> row2 = Map.of("a", 2L, "b", "world");
    String json = TransportShardExecuteAction.toJson(List.of(row1, row2));

    assertNotNull(json);
    assertTrue(json.startsWith("["));
    assertTrue(json.endsWith("]"));
    assertTrue(json.contains("\"a\":1"));
    assertTrue(json.contains("\"a\":2"));
    assertTrue(json.contains("\"hello\""));
    assertTrue(json.contains("\"world\""));
  }

  @Test
  @DisplayName("toJson handles null values")
  void toJsonHandlesNulls() {
    Map<String, Object> row = new java.util.LinkedHashMap<>();
    row.put("a", 1L);
    row.put("b", null);
    String json = TransportShardExecuteAction.toJson(List.of(row));

    assertEquals("[{\"a\":1,\"b\":null}]", json);
  }

  @Test
  @DisplayName("toJson handles empty list")
  void toJsonEmptyList() {
    String json = TransportShardExecuteAction.toJson(List.of());
    assertEquals("[]", json);
  }

  /** Serialize a DqePlanNode to bytes using the standard writePlanNode mechanism. */
  private byte[] serializePlan(DqePlanNode plan) throws IOException {
    BytesStreamOutput out = new BytesStreamOutput();
    DqePlanNode.writePlanNode(out, plan);
    return out.bytes().toBytesRef().bytes;
  }
}
