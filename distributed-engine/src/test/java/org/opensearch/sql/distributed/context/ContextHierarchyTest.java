/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.context;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.distributed.memory.MemoryPool;

class ContextHierarchyTest {

  @Test
  @DisplayName("QueryContext: create and verify memory pool binding")
  void testQueryContext() {
    MemoryPool pool = new MemoryPool("test-pool", 1024 * 1024);
    QueryContext ctx = new QueryContext("q1", pool, 30000);

    assertEquals("q1", ctx.getQueryId());
    assertSame(pool, ctx.getMemoryPool());
    assertEquals(30000, ctx.getQueryTimeoutMillis());
    assertFalse(ctx.isClosed());
  }

  @Test
  @DisplayName("TaskContext: create under QueryContext, verify parent linkage")
  void testTaskContext() {
    MemoryPool pool = new MemoryPool("test-pool", 1024 * 1024);
    QueryContext queryCtx = new QueryContext("q1", pool, 30000);
    TaskContext taskCtx = queryCtx.addTaskContext(0);

    assertSame(queryCtx, taskCtx.getQueryContext());
    assertEquals(0, taskCtx.getTaskId());
    assertSame(pool, taskCtx.getMemoryPool());
    assertEquals(1, queryCtx.getTaskContexts().size());
  }

  @Test
  @DisplayName("PipelineContext: create under TaskContext")
  void testPipelineContext() {
    MemoryPool pool = new MemoryPool("test-pool", 1024 * 1024);
    QueryContext queryCtx = new QueryContext("q1", pool, 30000);
    TaskContext taskCtx = queryCtx.addTaskContext(0);
    PipelineContext pipelineCtx = taskCtx.addPipelineContext(0);

    assertSame(taskCtx, pipelineCtx.getTaskContext());
    assertEquals(0, pipelineCtx.getPipelineId());
    assertSame(pool, pipelineCtx.getMemoryPool());
  }

  @Test
  @DisplayName("DriverContext: create under PipelineContext")
  void testDriverContext() {
    MemoryPool pool = new MemoryPool("test-pool", 1024 * 1024);
    QueryContext queryCtx = new QueryContext("q1", pool, 30000);
    TaskContext taskCtx = queryCtx.addTaskContext(0);
    PipelineContext pipelineCtx = taskCtx.addPipelineContext(0);
    DriverContext driverCtx = pipelineCtx.addDriverContext();

    assertSame(pipelineCtx, driverCtx.getPipelineContext());
    assertEquals(0, driverCtx.getDriverId());
    assertSame(pool, driverCtx.getMemoryPool());
  }

  @Test
  @DisplayName("OperatorContext: create under DriverContext, allocate memory")
  void testOperatorContext() {
    MemoryPool pool = new MemoryPool("test-pool", 1024 * 1024);
    QueryContext queryCtx = new QueryContext("q1", pool, 30000);
    TaskContext taskCtx = queryCtx.addTaskContext(0);
    PipelineContext pipelineCtx = taskCtx.addPipelineContext(0);
    DriverContext driverCtx = pipelineCtx.addDriverContext();
    OperatorContext opCtx = driverCtx.addOperatorContext(0, "TestOperator");

    assertSame(driverCtx, opCtx.getDriverContext());
    assertEquals(0, opCtx.getOperatorId());
    assertEquals("TestOperator", opCtx.getOperatorType());
    assertSame(pool, opCtx.getMemoryPool());

    // Reserve memory and verify tracking
    opCtx.reserveMemory(1000);
    assertEquals(1000, opCtx.getMemoryReservation());
    assertEquals(1000, pool.getReservedBytes());

    opCtx.freeMemory(500);
    assertEquals(500, opCtx.getMemoryReservation());
    assertEquals(500, pool.getReservedBytes());
  }

  @Test
  @DisplayName("Close QueryContext cascades to all children")
  void testCascadeCleanup() {
    MemoryPool pool = new MemoryPool("test-pool", 1024 * 1024);
    QueryContext queryCtx = new QueryContext("q1", pool, 30000);
    TaskContext taskCtx = queryCtx.addTaskContext(0);
    PipelineContext pipelineCtx = taskCtx.addPipelineContext(0);
    DriverContext driverCtx = pipelineCtx.addDriverContext();
    OperatorContext opCtx = driverCtx.addOperatorContext(0, "TestOp");

    // Reserve some memory
    opCtx.reserveMemory(1000);
    assertEquals(1000, pool.getReservedBytes());

    // Close the top-level context
    queryCtx.close();

    assertTrue(queryCtx.isClosed());
    assertTrue(taskCtx.isClosed());
    assertTrue(pipelineCtx.isClosed());
    assertTrue(driverCtx.isClosed());
    assertTrue(opCtx.isClosed());

    // Memory should be freed by OperatorContext.close()
    assertEquals(0, pool.getReservedBytes());
  }

  @Test
  @DisplayName("DriverContext thread binding")
  void testDriverThreadBinding() {
    MemoryPool pool = new MemoryPool("test-pool", 1024 * 1024);
    QueryContext queryCtx = new QueryContext("q1", pool, 30000);
    TaskContext taskCtx = queryCtx.addTaskContext(0);
    PipelineContext pipelineCtx = taskCtx.addPipelineContext(0);
    DriverContext driverCtx = pipelineCtx.addDriverContext();

    assertNull(driverCtx.getThread());
    driverCtx.setThread(Thread.currentThread());
    assertSame(Thread.currentThread(), driverCtx.getThread());
    driverCtx.close();
    assertNull(driverCtx.getThread());
  }

  @Test
  @DisplayName("PipelineContext stats tracking")
  void testPipelineStats() {
    MemoryPool pool = new MemoryPool("test-pool", 1024 * 1024);
    QueryContext queryCtx = new QueryContext("q1", pool, 30000);
    TaskContext taskCtx = queryCtx.addTaskContext(0);
    PipelineContext pipelineCtx = taskCtx.addPipelineContext(0);

    pipelineCtx.recordInputPositions(100);
    pipelineCtx.recordOutputPositions(50);
    assertEquals(100, pipelineCtx.getInputPositions());
    assertEquals(50, pipelineCtx.getOutputPositions());

    pipelineCtx.recordInputPositions(200);
    assertEquals(300, pipelineCtx.getInputPositions());
  }

  @Test
  @DisplayName("Cannot add child to closed context")
  void testCannotAddToClosedContext() {
    MemoryPool pool = new MemoryPool("test-pool", 1024 * 1024);
    QueryContext queryCtx = new QueryContext("q1", pool, 30000);
    queryCtx.close();

    assertThrows(IllegalStateException.class, () -> queryCtx.addTaskContext(0));
  }

  @Test
  @DisplayName("Standalone OperatorContext (no driver) works without memory pool")
  void testStandaloneOperatorContext() {
    OperatorContext opCtx = new OperatorContext(0, "Standalone");
    assertNull(opCtx.getDriverContext());
    assertNull(opCtx.getMemoryPool());

    // Memory operations should be no-ops when no pool
    opCtx.reserveMemory(100);
    assertEquals(100, opCtx.getMemoryReservation());
    opCtx.close();
  }
}
