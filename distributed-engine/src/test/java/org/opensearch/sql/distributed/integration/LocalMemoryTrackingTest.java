/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.integration;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.distributed.context.DriverContext;
import org.opensearch.sql.distributed.context.PipelineContext;
import org.opensearch.sql.distributed.context.QueryContext;
import org.opensearch.sql.distributed.data.Page;
import org.opensearch.sql.distributed.driver.Driver;
import org.opensearch.sql.distributed.driver.Pipeline;
import org.opensearch.sql.distributed.memory.MemoryPool;
import org.opensearch.sql.distributed.planner.LuceneTableScanNode;
import org.opensearch.sql.distributed.planner.PlanNodeId;
import org.opensearch.sql.distributed.planner.TopNNode;

/**
 * IC-1 Test 4: Memory pool accounting during local execution. Validates that MemoryPool
 * reservations are made and freed correctly during query execution.
 */
class LocalMemoryTrackingTest extends LocalExecutionTestBase {

  private final RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();

  @Test
  @DisplayName("Memory pool is created with correct capacity")
  void memoryPoolCapacity() {
    MemoryPool pool = new MemoryPool("test-pool", MEMORY_POOL_SIZE);
    assertEquals(MEMORY_POOL_SIZE, pool.getMaxBytes());
    assertEquals(0, pool.getReservedBytes());
    assertEquals(MEMORY_POOL_SIZE, pool.getFreeBytes());
  }

  @Test
  @DisplayName("Memory is freed after driver completes execution")
  void memoryFreedAfterExecution() {
    List<Page> sourcePages =
        List.of(
            nameAgeStatusPage(
                new String[] {"Alice", "Bob", "Charlie", "Dave", "Eve"},
                new long[] {25, 35, 45, 20, 50},
                new String[] {"a", "b", "c", "d", "e"}));

    RelDataType rowType =
        typeFactory
            .builder()
            .add("name", SqlTypeName.VARCHAR)
            .add("age", SqlTypeName.BIGINT)
            .add("status", SqlTypeName.VARCHAR)
            .build();

    LuceneTableScanNode scan =
        new LuceneTableScanNode(
            PlanNodeId.next("scan"), "test", rowType, List.of("name", "age", "status"));

    var collation =
        RelCollations.of(new RelFieldCollation(1, RelFieldCollation.Direction.ASCENDING));
    TopNNode topN = new TopNNode(PlanNodeId.next("topn"), scan, collation, 3);

    QueryContext queryContext = createQueryContext();
    MemoryPool pool = queryContext.getMemoryPool();
    PipelineContext pipelineContext = createPipelineContext(queryContext);
    Pipeline pipeline = buildPipeline(topN, sourcePages, pipelineContext);
    DriverContext driverContext = pipelineContext.addDriverContext();
    Driver driver = pipeline.createDriver(driverContext);

    // Execute
    runToCompletion(driver);

    // Close the driver to release resources
    driver.close();

    // Memory should be freed after close
    assertEquals(0, pool.getReservedBytes(), "All memory should be freed after driver close");
  }

  @Test
  @DisplayName("Query context hierarchy is properly constructed")
  void contextHierarchyCorrect() {
    QueryContext queryContext = createQueryContext();
    assertNotNull(queryContext.getMemoryPool());
    assertEquals("ic1-test", queryContext.getQueryId());

    PipelineContext pipelineContext = createPipelineContext(queryContext);
    assertNotNull(pipelineContext.getMemoryPool());
    assertSame(
        queryContext.getMemoryPool(),
        pipelineContext.getMemoryPool(),
        "Pipeline should share query's memory pool");

    DriverContext driverContext = pipelineContext.addDriverContext();
    assertNotNull(driverContext.getMemoryPool());
    assertSame(
        queryContext.getMemoryPool(),
        driverContext.getMemoryPool(),
        "Driver should share query's memory pool");
  }

  @Test
  @DisplayName("Context hierarchy closes cleanly")
  void contextHierarchyCloses() {
    QueryContext queryContext = createQueryContext();
    PipelineContext pipelineContext = createPipelineContext(queryContext);
    DriverContext driverContext = pipelineContext.addDriverContext();

    assertFalse(queryContext.isClosed());
    assertFalse(pipelineContext.isClosed());
    assertFalse(driverContext.isClosed());

    queryContext.close();

    assertTrue(queryContext.isClosed());
    // Child contexts should be closed by parent
    assertTrue(pipelineContext.isClosed());
    assertTrue(driverContext.isClosed());
  }
}
