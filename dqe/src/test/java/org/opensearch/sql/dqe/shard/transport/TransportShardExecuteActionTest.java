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

import io.trino.spi.Page;
import io.trino.spi.type.BigintType;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.sql.dqe.common.config.DqeSettings;
import org.opensearch.sql.dqe.operator.TestPageSource;
import org.opensearch.sql.dqe.planner.plan.DqePlanNode;
import org.opensearch.sql.dqe.planner.plan.LimitNode;
import org.opensearch.sql.dqe.planner.plan.TableScanNode;
import org.opensearch.transport.TransportService;

@DisplayName("TransportShardExecuteAction doExecute")
class TransportShardExecuteActionTest {

  @Test
  @DisplayName("doExecute with TableScan returns all rows as Pages")
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
    assertNotNull(response.getPages());
    // Should contain pages with 3 rows total
    int totalRows = response.getPages().stream().mapToInt(Page::getPositionCount).sum();
    assertEquals(3, totalRows);
    // Verify values 0, 1, 2 in the first page
    Page page = response.getPages().get(0);
    assertEquals(0L, BigintType.BIGINT.getLong(page.getBlock(0), 0));
    assertEquals(1L, BigintType.BIGINT.getLong(page.getBlock(0), 1));
    assertEquals(2L, BigintType.BIGINT.getLong(page.getBlock(0), 2));
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
    // Should contain exactly 2 rows (limited from 5)
    int totalRows = response.getPages().stream().mapToInt(Page::getPositionCount).sum();
    assertEquals(2, totalRows);
    // Verify values 0 and 1
    Page page = response.getPages().get(0);
    assertEquals(0L, BigintType.BIGINT.getLong(page.getBlock(0), 0));
    assertEquals(1L, BigintType.BIGINT.getLong(page.getBlock(0), 1));
  }

  @Test
  @DisplayName("doExecute with empty result returns empty page list")
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
    assertTrue(response.getPages().isEmpty());
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
  @DisplayName("resolveColumnNames returns columns from TableScanNode")
  void resolveColumnNamesFromTableScan() {
    TableScanNode scan = new TableScanNode("logs", List.of("a", "b", "c"));
    assertEquals(List.of("a", "b", "c"), TransportShardExecuteAction.resolveColumnNames(scan));
  }

  @Test
  @DisplayName("DQE thread pool name constant matches registered pool name")
  void dqeThreadPoolNameMatchesRegisteredPool() {
    assertEquals(
        "dqe-shard-executor",
        TransportShardExecuteAction.DQE_THREAD_POOL_NAME,
        "Thread pool name must match the pool registered in SQLPlugin");
  }

  @Test
  @DisplayName("PAGE_BATCH_SIZE setting default matches expected value")
  void pageBatchSizeSettingDefault() {
    assertEquals(
        1024,
        (int) DqeSettings.PAGE_BATCH_SIZE.getDefault(Settings.EMPTY),
        "Default PAGE_BATCH_SIZE should be 1024");
  }

  /** Serialize a DqePlanNode to bytes using the standard writePlanNode mechanism. */
  private byte[] serializePlan(DqePlanNode plan) throws IOException {
    BytesStreamOutput out = new BytesStreamOutput();
    DqePlanNode.writePlanNode(out, plan);
    return out.bytes().toBytesRef().bytes;
  }
}
