/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.trino.memory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;
import io.trino.execution.StageId;
import io.trino.execution.TaskId;
import io.trino.spi.QueryId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CircuitBreakerMemoryPoolTest {

  private static final DataSize POOL_SIZE = DataSize.ofBytes(1000);
  private static final String TEST_TAG = "test";

  private CircuitBreakerMemoryPool pool;
  private TaskId taskId;

  @BeforeEach
  void setUp() {
    pool = CircuitBreakerMemoryPool.create(POOL_SIZE);
    QueryId queryId = new QueryId("test_query_0");
    StageId stageId = new StageId(queryId, 0);
    taskId = new TaskId(stageId, 0, 0);
  }

  @Test
  void reserveMemoryWithinLimit() {
    ListenableFuture<Void> future = pool.reserve(taskId, TEST_TAG, 500);
    assertTrue(future.isDone(), "Reserve within limit should return completed future");
    assertEquals(500, pool.getReservedBytes());
    assertEquals(500, pool.getFreeBytes());
  }

  @Test
  void freeReleasesMemory() {
    pool.reserve(taskId, TEST_TAG, 400);
    assertEquals(400, pool.getReservedBytes());

    pool.free(taskId, TEST_TAG, 400);
    assertEquals(0, pool.getReservedBytes());
    assertEquals(1000, pool.getFreeBytes());
  }

  @Test
  void tracksReservedBytesCorrectly() {
    pool.reserve(taskId, TEST_TAG, 300);
    pool.reserve(taskId, TEST_TAG, 200);
    assertEquals(500, pool.getReservedBytes());
    assertEquals(500, pool.getFreeBytes());
    assertEquals(1000, pool.getMaxBytes());
  }

  @Test
  void blocksWhenExhaustedAndUnblocksWhenFreed() {
    // Exhaust the pool
    pool.reserve(taskId, TEST_TAG, 1000);
    assertEquals(0, pool.getFreeBytes());

    // Next reservation should return a non-done future (blocked)
    ListenableFuture<Void> blocked = pool.reserve(taskId, TEST_TAG, 1);
    assertFalse(blocked.isDone(), "Reserve beyond limit should block");

    // Free memory to unblock
    pool.free(taskId, TEST_TAG, 500);
    assertTrue(blocked.isDone(), "Future should complete after freeing memory");
  }

  @Test
  void tryReserveSucceedsWithinLimit() {
    assertTrue(pool.tryReserve(taskId, TEST_TAG, 500));
    assertEquals(500, pool.getReservedBytes());
  }

  @Test
  void tryReserveFailsWhenExhausted() {
    pool.reserve(taskId, TEST_TAG, 1000);
    assertFalse(pool.tryReserve(taskId, TEST_TAG, 1));
  }

  @Test
  void createSetsCorrectMaxBytes() {
    assertEquals(1000, pool.getMaxBytes());
  }
}
