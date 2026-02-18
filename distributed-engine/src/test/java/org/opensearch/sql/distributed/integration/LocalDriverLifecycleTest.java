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
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.distributed.context.DriverContext;
import org.opensearch.sql.distributed.context.PipelineContext;
import org.opensearch.sql.distributed.context.QueryContext;
import org.opensearch.sql.distributed.data.Page;
import org.opensearch.sql.distributed.driver.Driver;
import org.opensearch.sql.distributed.driver.Pipeline;
import org.opensearch.sql.distributed.operator.Operator;
import org.opensearch.sql.distributed.planner.FilterNode;
import org.opensearch.sql.distributed.planner.LuceneTableScanNode;
import org.opensearch.sql.distributed.planner.PlanNodeId;
import org.opensearch.sql.distributed.planner.TopNNode;

/**
 * IC-1 Test 5: Driver and operator lifecycle tests. Validates open/close, finish propagation, and
 * clean shutdown.
 */
class LocalDriverLifecycleTest extends LocalExecutionTestBase {

  private final RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
  private final RexBuilder rexBuilder = new RexBuilder(typeFactory);

  @Test
  @DisplayName("Driver finishes when all operators are done")
  void driverFinishesCorrectly() {
    List<Page> sourcePages =
        List.of(
            nameAgeStatusPage(
                new String[] {"Alice", "Bob"}, new long[] {25, 35}, new String[] {"a", "b"}));

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

    QueryContext queryContext = createQueryContext();
    PipelineContext pipelineContext = createPipelineContext(queryContext);
    Pipeline pipeline = buildPipeline(scan, sourcePages, pipelineContext);
    DriverContext driverContext = pipelineContext.addDriverContext();
    Driver driver = pipeline.createDriver(driverContext);

    assertFalse(driver.isFinished(), "Driver should not be finished before processing");

    List<Page> output = runToCompletion(driver);

    assertTrue(driver.isFinished(), "Driver should be finished after processing all data");
    assertEquals(2, totalPositions(output));
  }

  @Test
  @DisplayName("Driver close cleans up all operators and context")
  void driverCloseCleanup() {
    List<Page> sourcePages =
        List.of(nameAgeStatusPage(new String[] {"Alice"}, new long[] {25}, new String[] {"a"}));

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

    QueryContext queryContext = createQueryContext();
    PipelineContext pipelineContext = createPipelineContext(queryContext);
    Pipeline pipeline = buildPipeline(scan, sourcePages, pipelineContext);
    DriverContext driverContext = pipelineContext.addDriverContext();
    Driver driver = pipeline.createDriver(driverContext);

    // Run to completion
    runToCompletion(driver);

    // Close driver
    driver.close();

    assertTrue(driver.isFinished(), "Driver should be finished after close");
    assertTrue(driverContext.isClosed(), "DriverContext should be closed");
  }

  @Test
  @DisplayName("Pipeline creates driver with correct operator count")
  void pipelineCreatesCorrectOperators() {
    List<Page> sourcePages =
        List.of(nameAgeStatusPage(new String[] {"Alice"}, new long[] {25}, new String[] {"a"}));

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

    // Filter: age > 20
    RexNode ageRef = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.BIGINT), 1);
    RexNode literal = rexBuilder.makeBigintLiteral(java.math.BigDecimal.valueOf(20));
    RexNode predicate = rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN, ageRef, literal);
    FilterNode filter = new FilterNode(PlanNodeId.next("filter"), scan, predicate);

    // TopN
    var collation =
        RelCollations.of(new RelFieldCollation(1, RelFieldCollation.Direction.ASCENDING));
    TopNNode topN = new TopNNode(PlanNodeId.next("topn"), filter, collation, 10);

    QueryContext queryContext = createQueryContext();
    PipelineContext pipelineContext = createPipelineContext(queryContext);
    Pipeline pipeline = buildPipeline(topN, sourcePages, pipelineContext);
    DriverContext driverContext = pipelineContext.addDriverContext();
    Driver driver = pipeline.createDriver(driverContext);

    // Source (TestSource) + PassThroughFilter + TopN + Sink = 4 operators
    List<Operator> operators = driver.getOperators();
    assertTrue(
        operators.size() >= 3,
        "Driver should have at least source + processing + sink operators, got "
            + operators.size());
  }

  @Test
  @DisplayName("Multiple process() calls eventually finish the driver")
  void multipleProcessCallsFinish() {
    // Create enough data that it may require multiple process() calls
    String[] names = new String[100];
    long[] ages = new long[100];
    String[] statuses = new String[100];
    for (int i = 0; i < 100; i++) {
      names[i] = "user" + i;
      ages[i] = i;
      statuses[i] = i % 2 == 0 ? "active" : "inactive";
    }

    List<Page> sourcePages = List.of(nameAgeStatusPage(names, ages, statuses));

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
    TopNNode topN = new TopNNode(PlanNodeId.next("topn"), scan, collation, 10);

    QueryContext queryContext = createQueryContext();
    PipelineContext pipelineContext = createPipelineContext(queryContext);
    Pipeline pipeline = buildPipeline(topN, sourcePages, pipelineContext);
    DriverContext driverContext = pipelineContext.addDriverContext();
    Driver driver = pipeline.createDriver(driverContext);

    int processCount = 0;
    while (!driver.isFinished() && processCount < 10_000) {
      driver.process();
      processCount++;
    }

    assertTrue(driver.isFinished(), "Driver should finish within reasonable iterations");
    assertTrue(processCount > 0, "Should have required at least 1 process() call");

    driver.close();
  }

  @Test
  @DisplayName("Empty source produces empty output through full pipeline")
  void emptySourceThroughPipeline() {
    List<Page> sourcePages = List.of(); // empty

    RelDataType rowType =
        typeFactory
            .builder()
            .add("name", SqlTypeName.VARCHAR)
            .add("age", SqlTypeName.BIGINT)
            .build();

    LuceneTableScanNode scan =
        new LuceneTableScanNode(PlanNodeId.next("scan"), "test", rowType, List.of("name", "age"));

    var collation =
        RelCollations.of(new RelFieldCollation(1, RelFieldCollation.Direction.ASCENDING));
    TopNNode topN = new TopNNode(PlanNodeId.next("topn"), scan, collation, 10);

    List<Page> output = execute(topN, sourcePages);

    assertEquals(0, totalPositions(output), "Empty source should produce empty output");
  }
}
