/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.planner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import io.trino.spi.type.BigintType;
import io.trino.spi.type.VarcharType;
import io.trino.sql.tree.Statement;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.dqe.coordinator.metadata.TableInfo;
import org.opensearch.sql.dqe.coordinator.metadata.TableInfo.ColumnInfo;
import org.opensearch.sql.dqe.planner.plan.*;
import org.opensearch.sql.dqe.trino.parser.DqeSqlParser;

@DisplayName("LogicalPlanner: AST → DqePlanNode tree")
class LogicalPlannerTest {

  private final DqeSqlParser parser = new DqeSqlParser();

  @Test
  @DisplayName("SELECT a FROM t produces Project(TableScan)")
  void simpleSelect() {
    Statement stmt = parser.parse("SELECT a FROM t");
    DqePlanNode plan = LogicalPlanner.plan(stmt, this::mockTableInfo);
    assertInstanceOf(ProjectNode.class, plan);
    assertInstanceOf(TableScanNode.class, ((ProjectNode) plan).getChild());
  }

  @Test
  @DisplayName("SELECT a FROM t WHERE x=1 produces Project(Filter(TableScan))")
  void selectWithFilter() {
    Statement stmt = parser.parse("SELECT a FROM t WHERE x = 1");
    DqePlanNode plan = LogicalPlanner.plan(stmt, this::mockTableInfo);
    assertInstanceOf(ProjectNode.class, plan);
    assertInstanceOf(FilterNode.class, ((ProjectNode) plan).getChild());
  }

  @Test
  @DisplayName("SELECT a FROM t LIMIT 10 produces Limit(Project(TableScan))")
  void selectWithLimit() {
    Statement stmt = parser.parse("SELECT a FROM t LIMIT 10");
    DqePlanNode plan = LogicalPlanner.plan(stmt, this::mockTableInfo);
    assertInstanceOf(LimitNode.class, plan);
    assertEquals(10, ((LimitNode) plan).getCount());
  }

  @Test
  @DisplayName("SELECT a, COUNT(*) FROM t GROUP BY a produces plan with AggregationNode")
  void selectWithGroupBy() {
    Statement stmt = parser.parse("SELECT a, COUNT(*) FROM t GROUP BY a");
    DqePlanNode plan = LogicalPlanner.plan(stmt, this::mockTableInfo);
    assertNotNull(findNode(plan, AggregationNode.class));
  }

  @Test
  @DisplayName("SELECT a FROM t ORDER BY a ASC produces plan with SortNode")
  void selectWithOrderBy() {
    Statement stmt = parser.parse("SELECT a FROM t ORDER BY a ASC");
    DqePlanNode plan = LogicalPlanner.plan(stmt, this::mockTableInfo);
    assertNotNull(findNode(plan, SortNode.class));
  }

  private <T> T findNode(DqePlanNode root, Class<T> type) {
    if (type.isInstance(root)) return type.cast(root);
    for (DqePlanNode child : root.getChildren()) {
      T found = findNode(child, type);
      if (found != null) return found;
    }
    return null;
  }

  private TableInfo mockTableInfo(String indexName) {
    return new TableInfo(
        indexName,
        List.of(
            new ColumnInfo("a", "keyword", VarcharType.VARCHAR),
            new ColumnInfo("x", "long", BigintType.BIGINT)));
  }
}
