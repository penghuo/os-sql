/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.shard.executor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.opensearch.sql.dqe.operator.TestPageSource.buildBigintPage;
import static org.opensearch.sql.dqe.operator.TestPageSource.buildCategoryValuePage;
import static org.opensearch.sql.dqe.operator.TestPageSource.buildMultiColumnPage;

import io.trino.spi.Page;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.dqe.operator.Operator;
import org.opensearch.sql.dqe.operator.TestPageSource;
import org.opensearch.sql.dqe.planner.plan.AggregationNode;
import org.opensearch.sql.dqe.planner.plan.FilterNode;
import org.opensearch.sql.dqe.planner.plan.LimitNode;
import org.opensearch.sql.dqe.planner.plan.ProjectNode;
import org.opensearch.sql.dqe.planner.plan.SortNode;
import org.opensearch.sql.dqe.planner.plan.TableScanNode;

@DisplayName("LocalExecutionPlanner: plan nodes -> operator pipeline")
class LocalExecutionPlannerTest {

  @Test
  @DisplayName("TableScanNode produces operator from scan factory")
  void tableScanProducesOperator() {
    TableScanNode scan = new TableScanNode("logs", List.of("a"));
    Page testPage = buildBigintPage(3);
    LocalExecutionPlanner planner =
        new LocalExecutionPlanner(node -> new TestPageSource(List.of(testPage)));

    Operator result = scan.accept(planner, null);
    assertNotNull(result);
    Page page = result.processNextBatch();
    assertNotNull(page);
    assertEquals(3, page.getPositionCount());
    assertNull(result.processNextBatch());
  }

  @Test
  @DisplayName("LimitNode wraps child in LimitOperator")
  void limitNodeProducesLimitOperator() {
    TableScanNode scan = new TableScanNode("logs", List.of("a"));
    LimitNode limit = new LimitNode(scan, 2);
    Page testPage = buildBigintPage(5);
    LocalExecutionPlanner planner =
        new LocalExecutionPlanner(node -> new TestPageSource(List.of(testPage)));

    Operator result = limit.accept(planner, null);
    Page page = result.processNextBatch();
    assertNotNull(page);
    assertEquals(2, page.getPositionCount());
    assertNull(result.processNextBatch());
  }

  @Test
  @DisplayName("Project(TableScan) produces ProjectOperator wrapping source")
  void projectNodeProducesProjectOperator() {
    TableScanNode scan = new TableScanNode("logs", List.of("a", "b"));
    ProjectNode project = new ProjectNode(scan, List.of("a"));
    // Source has 2 columns, 3 rows
    Page testPage = buildMultiColumnPage(2, 3);
    LocalExecutionPlanner planner =
        new LocalExecutionPlanner(node -> new TestPageSource(List.of(testPage)));

    Operator result = project.accept(planner, null);
    Page page = result.processNextBatch();
    assertNotNull(page);
    assertEquals(1, page.getChannelCount()); // projected to 1 column
    assertEquals(3, page.getPositionCount());
  }

  @Test
  @DisplayName("Project selects second column correctly")
  void projectSecondColumn() {
    TableScanNode scan = new TableScanNode("logs", List.of("a", "b"));
    ProjectNode project = new ProjectNode(scan, List.of("b"));
    // 2 columns, 3 rows: col0 = [0,10,20], col1 = [1,11,21]
    Page testPage = buildMultiColumnPage(2, 3);
    LocalExecutionPlanner planner =
        new LocalExecutionPlanner(node -> new TestPageSource(List.of(testPage)));

    Operator result = project.accept(planner, null);
    Page page = result.processNextBatch();
    assertNotNull(page);
    assertEquals(1, page.getChannelCount());
    // The projected column should be the original column 1 (values: 1, 11, 21)
    assertEquals(1L, BigintType.BIGINT.getLong(page.getBlock(0), 0));
    assertEquals(11L, BigintType.BIGINT.getLong(page.getBlock(0), 1));
  }

  @Test
  @DisplayName("Filter(TableScan) produces FilterOperator with parsed equality predicate")
  void filterNodeProducesFilterOperator() {
    TableScanNode scan = new TableScanNode("logs", List.of("status"));
    FilterNode filter = new FilterNode(scan, "status = 200");
    // Source has 1 BIGINT column with values: 100, 200, 300, 200, 400
    Page testPage = TestPageSource.buildBigintPageWithValues(100L, 200L, 300L, 200L, 400L);
    LocalExecutionPlanner planner =
        new LocalExecutionPlanner(node -> new TestPageSource(List.of(testPage)));

    Operator result = filter.accept(planner, null);
    Page page = result.processNextBatch();
    assertNotNull(page);
    assertEquals(2, page.getPositionCount());
    assertEquals(200L, BigintType.BIGINT.getLong(page.getBlock(0), 0));
    assertEquals(200L, BigintType.BIGINT.getLong(page.getBlock(0), 1));
  }

  @Test
  @DisplayName("Filter with greater-than comparison")
  void filterWithGreaterThan() {
    TableScanNode scan = new TableScanNode("logs", List.of("status"));
    FilterNode filter = new FilterNode(scan, "status > 200");
    // Source: 100, 200, 300, 500
    Page testPage = TestPageSource.buildBigintPageWithValues(100L, 200L, 300L, 500L);
    LocalExecutionPlanner planner =
        new LocalExecutionPlanner(node -> new TestPageSource(List.of(testPage)));

    Operator result = filter.accept(planner, null);
    Page page = result.processNextBatch();
    assertNotNull(page);
    assertEquals(2, page.getPositionCount());
    assertEquals(300L, BigintType.BIGINT.getLong(page.getBlock(0), 0));
    assertEquals(500L, BigintType.BIGINT.getLong(page.getBlock(0), 1));
  }

  @Test
  @DisplayName("Filter with AND compound predicate")
  void filterWithAnd() {
    TableScanNode scan = new TableScanNode("logs", List.of("status"));
    FilterNode filter = new FilterNode(scan, "status > 200 AND status < 500");
    // Source: 100, 200, 300, 500
    Page testPage = TestPageSource.buildBigintPageWithValues(100L, 200L, 300L, 500L);
    LocalExecutionPlanner planner =
        new LocalExecutionPlanner(node -> new TestPageSource(List.of(testPage)));

    Operator result = filter.accept(planner, null);
    Page page = result.processNextBatch();
    assertNotNull(page);
    assertEquals(1, page.getPositionCount());
    assertEquals(300L, BigintType.BIGINT.getLong(page.getBlock(0), 0));
  }

  @Test
  @DisplayName("Filter with string equality on VARCHAR column")
  void filterWithStringEquality() {
    TableScanNode scan = new TableScanNode("logs", List.of("category", "status"));
    FilterNode filter = new FilterNode(scan, "category = 'error'");
    // Source: ("error", 500), ("info", 200), ("error", 503)
    Page testPage = buildCategoryValuePage("error", 500L, "info", 200L, "error", 503L);
    Map<String, Type> typeMap =
        Map.of(
            "category", VarcharType.VARCHAR,
            "status", BigintType.BIGINT);
    LocalExecutionPlanner planner =
        new LocalExecutionPlanner(node -> new TestPageSource(List.of(testPage)), typeMap);

    Operator result = filter.accept(planner, null);
    Page page = result.processNextBatch();
    assertNotNull(page);
    assertEquals(2, page.getPositionCount());
  }

  @Test
  @DisplayName("Sort(TableScan) produces SortOperator")
  void sortNodeProducesSortOperator() {
    TableScanNode scan = new TableScanNode("logs", List.of("a"));
    SortNode sort = new SortNode(scan, List.of("a"), List.of(true));
    // Source values: 30, 10, 20
    Page testPage = TestPageSource.buildBigintPageWithValues(30L, 10L, 20L);
    LocalExecutionPlanner planner =
        new LocalExecutionPlanner(node -> new TestPageSource(List.of(testPage)));

    Operator result = sort.accept(planner, null);
    Page page = result.processNextBatch();
    assertNotNull(page);
    assertEquals(3, page.getPositionCount());
    // Should be sorted ascending: 10, 20, 30
    assertEquals(10L, BigintType.BIGINT.getLong(page.getBlock(0), 0));
    assertEquals(20L, BigintType.BIGINT.getLong(page.getBlock(0), 1));
    assertEquals(30L, BigintType.BIGINT.getLong(page.getBlock(0), 2));
  }

  @Test
  @DisplayName("Aggregation(TableScan) produces HashAggregationOperator with COUNT(*)")
  void aggregationCountProducesOperator() {
    TableScanNode scan = new TableScanNode("logs", List.of("category", "amount"));
    AggregationNode agg =
        new AggregationNode(
            scan, List.of("category"), List.of("COUNT(*)"), AggregationNode.Step.PARTIAL);
    // Source: [("a",10), ("b",20), ("a",30)]
    Page testPage = buildCategoryValuePage("a", 10L, "b", 20L, "a", 30L);
    Map<String, Type> typeMap =
        Map.of(
            "category", VarcharType.VARCHAR,
            "amount", BigintType.BIGINT);
    LocalExecutionPlanner planner =
        new LocalExecutionPlanner(node -> new TestPageSource(List.of(testPage)), typeMap);

    Operator result = agg.accept(planner, null);
    Page page = result.processNextBatch();
    assertNotNull(page);
    assertEquals(2, page.getPositionCount()); // 2 groups
    assertEquals(2, page.getChannelCount()); // category + count

    Map<String, Long> counts = new HashMap<>();
    for (int i = 0; i < page.getPositionCount(); i++) {
      String key = VarcharType.VARCHAR.getSlice(page.getBlock(0), i).toStringUtf8();
      long count = BigintType.BIGINT.getLong(page.getBlock(1), i);
      counts.put(key, count);
    }
    assertEquals(2L, counts.get("a"));
    assertEquals(1L, counts.get("b"));
  }

  @Test
  @DisplayName("Aggregation(TableScan) produces HashAggregationOperator with SUM(amount)")
  void aggregationSumProducesOperator() {
    TableScanNode scan = new TableScanNode("logs", List.of("category", "amount"));
    AggregationNode agg =
        new AggregationNode(
            scan, List.of("category"), List.of("SUM(amount)"), AggregationNode.Step.PARTIAL);
    Page testPage = buildCategoryValuePage("a", 10L, "b", 20L, "a", 30L);
    Map<String, Type> typeMap =
        Map.of(
            "category", VarcharType.VARCHAR,
            "amount", BigintType.BIGINT);
    LocalExecutionPlanner planner =
        new LocalExecutionPlanner(node -> new TestPageSource(List.of(testPage)), typeMap);

    Operator result = agg.accept(planner, null);
    Page page = result.processNextBatch();
    assertNotNull(page);

    Map<String, Long> sums = new HashMap<>();
    for (int i = 0; i < page.getPositionCount(); i++) {
      String key = VarcharType.VARCHAR.getSlice(page.getBlock(0), i).toStringUtf8();
      long sum = BigintType.BIGINT.getLong(page.getBlock(1), i);
      sums.put(key, sum);
    }
    assertEquals(40L, sums.get("a"));
    assertEquals(20L, sums.get("b"));
  }

  @Test
  @DisplayName("Chained pipeline: Limit(Project(TableScan))")
  void chainedPipeline() {
    TableScanNode scan = new TableScanNode("logs", List.of("a", "b"));
    ProjectNode project = new ProjectNode(scan, List.of("a"));
    LimitNode limit = new LimitNode(project, 2);
    // 2 columns, 5 rows
    Page testPage = buildMultiColumnPage(2, 5);
    LocalExecutionPlanner planner =
        new LocalExecutionPlanner(node -> new TestPageSource(List.of(testPage)));

    Operator result = limit.accept(planner, null);
    Page page = result.processNextBatch();
    assertNotNull(page);
    assertEquals(1, page.getChannelCount()); // project to 1 column
    assertEquals(2, page.getPositionCount()); // limited to 2 rows
    assertNull(result.processNextBatch());
  }
}
