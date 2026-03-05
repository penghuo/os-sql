/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.trino.spi.Page;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import io.trino.sql.tree.Statement;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.dqe.coordinator.metadata.TableInfo;
import org.opensearch.sql.dqe.coordinator.metadata.TableInfo.ColumnInfo;
import org.opensearch.sql.dqe.operator.Operator;
import org.opensearch.sql.dqe.operator.TestPageSource;
import org.opensearch.sql.dqe.planner.LogicalPlanner;
import org.opensearch.sql.dqe.planner.plan.DqePlanNode;
import org.opensearch.sql.dqe.planner.plan.TableScanNode;
import org.opensearch.sql.dqe.shard.executor.LocalExecutionPlanner;
import org.opensearch.sql.dqe.shard.source.ColumnHandle;
import org.opensearch.sql.dqe.shard.source.PageBuilder;
import org.opensearch.sql.dqe.trino.parser.DqeSqlParser;

@DisplayName("DQE end-to-end pipeline")
class DqeEndToEndTest {

  @Test
  @DisplayName("Simple SELECT with LIMIT through full pipeline")
  void simpleSelectWithLimit() {
    // Parse
    DqeSqlParser parser = new DqeSqlParser();
    Statement stmt = parser.parse("SELECT a FROM t LIMIT 2");

    // Plan
    DqePlanNode plan = LogicalPlanner.plan(stmt, this::mockTableInfo);
    assertNotNull(plan);

    // Create scan factory returning test data
    Function<TableScanNode, Operator> scanFactory =
        node -> {
          List<ColumnHandle> cols = List.of(new ColumnHandle("a", VarcharType.VARCHAR));
          List<Map<String, Object>> rows =
              List.of(Map.of("a", "x"), Map.of("a", "y"), Map.of("a", "z"));
          return new TestPageSource(List.of(PageBuilder.build(cols, rows)));
        };

    // Execute through LocalExecutionPlanner
    Map<String, Type> typeMap = Map.of("a", VarcharType.VARCHAR);
    LocalExecutionPlanner execPlanner = new LocalExecutionPlanner(scanFactory, typeMap);
    Operator pipeline = plan.accept(execPlanner, null);

    // Drain
    List<Page> results = TestPageSource.drainOperator(pipeline);
    int totalRows = results.stream().mapToInt(Page::getPositionCount).sum();
    assertEquals(2, totalRows); // LIMIT 2 applied
  }

  @Test
  @DisplayName("SELECT with GROUP BY and COUNT through full pipeline")
  void aggregationPipeline() {
    DqeSqlParser parser = new DqeSqlParser();
    Statement stmt = parser.parse("SELECT category, COUNT(*) FROM logs GROUP BY category");

    DqePlanNode plan = LogicalPlanner.plan(stmt, this::mockTableInfo2);
    assertNotNull(plan);

    // Scan factory returns test data with categories
    Function<TableScanNode, Operator> scanFactory =
        node -> {
          List<ColumnHandle> cols =
              List.of(
                  new ColumnHandle("category", VarcharType.VARCHAR),
                  new ColumnHandle("status", BigintType.BIGINT));
          List<Map<String, Object>> rows =
              List.of(
                  Map.of("category", "a", "status", 200),
                  Map.of("category", "b", "status", 404),
                  Map.of("category", "a", "status", 200));
          return new TestPageSource(List.of(PageBuilder.build(cols, rows)));
        };

    Map<String, Type> typeMap =
        Map.of(
            "category", VarcharType.VARCHAR,
            "status", BigintType.BIGINT);
    LocalExecutionPlanner execPlanner = new LocalExecutionPlanner(scanFactory, typeMap);
    Operator pipeline = plan.accept(execPlanner, null);

    List<Page> results = TestPageSource.drainOperator(pipeline);
    int totalRows = results.stream().mapToInt(Page::getPositionCount).sum();
    assertTrue(totalRows > 0);
    // Should have 2 groups: "a" (count 2) and "b" (count 1)
    assertEquals(2, totalRows);
  }

  @Test
  @DisplayName("SELECT with complex WHERE predicate through full pipeline")
  void selectWithComplexWhere() {
    DqeSqlParser parser = new DqeSqlParser();
    Statement stmt = parser.parse("SELECT category, status FROM logs WHERE status > 200");

    DqePlanNode plan = LogicalPlanner.plan(stmt, this::mockTableInfo2);
    assertNotNull(plan);

    Function<TableScanNode, Operator> scanFactory =
        node -> {
          List<ColumnHandle> cols =
              List.of(
                  new ColumnHandle("category", VarcharType.VARCHAR),
                  new ColumnHandle("status", BigintType.BIGINT));
          List<Map<String, Object>> rows =
              List.of(
                  Map.of("category", "error", "status", 500),
                  Map.of("category", "info", "status", 200),
                  Map.of("category", "warn", "status", 400));
          return new TestPageSource(List.of(PageBuilder.build(cols, rows)));
        };

    Map<String, Type> typeMap =
        Map.of(
            "category", VarcharType.VARCHAR,
            "status", BigintType.BIGINT);
    LocalExecutionPlanner execPlanner = new LocalExecutionPlanner(scanFactory, typeMap);
    Operator pipeline = plan.accept(execPlanner, null);

    List<Page> results = TestPageSource.drainOperator(pipeline);
    int totalRows = results.stream().mapToInt(Page::getPositionCount).sum();
    // status > 200: error/500, warn/400 = 2 rows
    assertEquals(2, totalRows);
  }

  private TableInfo mockTableInfo(String indexName) {
    return new TableInfo(indexName, List.of(new ColumnInfo("a", "keyword", VarcharType.VARCHAR)));
  }

  private TableInfo mockTableInfo2(String indexName) {
    return new TableInfo(
        indexName,
        List.of(
            new ColumnInfo("category", "keyword", VarcharType.VARCHAR),
            new ColumnInfo("status", "long", BigintType.BIGINT)));
  }
}
