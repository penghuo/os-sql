/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.integration;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.distributed.data.Page;
import org.opensearch.sql.distributed.planner.FilterNode;
import org.opensearch.sql.distributed.planner.LuceneTableScanNode;
import org.opensearch.sql.distributed.planner.PlanNodeId;
import org.opensearch.sql.distributed.planner.ProjectNode;

/**
 * IC-1 Test 1: Local execution of filter+project pipelines. Validates: source=test | where age > 30
 * | fields name, age
 */
class LocalFilterProjectTest extends LocalExecutionTestBase {

  private final RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
  private final RexBuilder rexBuilder = new RexBuilder(typeFactory);

  @Test
  @DisplayName("Filter: where age > 30 removes rows below threshold")
  void filterByAge() {
    // Schema: name(0:VARCHAR), age(1:LONG), status(2:VARCHAR)
    // Data: 5 rows, ages: 25, 35, 45, 20, 50
    List<Page> sourcePages =
        List.of(
            nameAgeStatusPage(
                new String[] {"Alice", "Bob", "Charlie", "Dave", "Eve"},
                new long[] {25, 35, 45, 20, 50},
                new String[] {"active", "active", "inactive", "active", "inactive"}));

    // PlanNode: Filter(age > 30) -> Scan
    RelDataType longType = typeFactory.createSqlType(SqlTypeName.BIGINT);
    RexNode ageRef = rexBuilder.makeInputRef(longType, 1);
    RexNode literal30 = rexBuilder.makeBigintLiteral(java.math.BigDecimal.valueOf(30));
    RexNode predicate = rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN, ageRef, literal30);

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
    FilterNode filter = new FilterNode(PlanNodeId.next("filter"), scan, predicate);

    List<Page> output = execute(filter, sourcePages);

    // Should get 3 rows: Bob(35), Charlie(45), Eve(50)
    int total = totalPositions(output);
    assertEquals(3, total, "Filter age>30 should produce 3 rows");

    // Verify ages are all > 30
    List<Long> ages = collectLongs(output, 1);
    for (long age : ages) {
      assertTrue(age > 30, "All output ages should be > 30, got " + age);
    }
  }

  @Test
  @DisplayName("Project: select specific columns from source")
  void projectColumns() {
    // Schema: name(0:VARCHAR), age(1:LONG), status(2:VARCHAR)
    List<Page> sourcePages =
        List.of(
            nameAgeStatusPage(
                new String[] {"Alice", "Bob"},
                new long[] {25, 35},
                new String[] {"active", "inactive"}));

    // PlanNode: Project(name, age) -> Scan (select columns 0 and 1)
    RelDataType rowType =
        typeFactory
            .builder()
            .add("name", SqlTypeName.VARCHAR)
            .add("age", SqlTypeName.BIGINT)
            .add("status", SqlTypeName.VARCHAR)
            .build();

    RelDataType outType =
        typeFactory
            .builder()
            .add("name", SqlTypeName.VARCHAR)
            .add("age", SqlTypeName.BIGINT)
            .build();

    LuceneTableScanNode scan =
        new LuceneTableScanNode(
            PlanNodeId.next("scan"), "test", rowType, List.of("name", "age", "status"));

    RexNode nameRef = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 0);
    RexNode ageRef = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.BIGINT), 1);
    ProjectNode project =
        new ProjectNode(PlanNodeId.next("project"), scan, List.of(nameRef, ageRef), outType);

    List<Page> output = execute(project, sourcePages);

    // Should get 2 rows with 2 columns
    int total = totalPositions(output);
    assertEquals(2, total);
    for (Page page : output) {
      assertEquals(2, page.getChannelCount(), "Project should output 2 columns");
    }
  }

  @Test
  @DisplayName("Filter + Project: where age > 30 | fields name, age")
  void filterThenProject() {
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

    RelDataType outType =
        typeFactory
            .builder()
            .add("name", SqlTypeName.VARCHAR)
            .add("age", SqlTypeName.BIGINT)
            .build();

    LuceneTableScanNode scan =
        new LuceneTableScanNode(
            PlanNodeId.next("scan"), "test", rowType, List.of("name", "age", "status"));

    // Filter: age > 30
    RexNode ageRef = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.BIGINT), 1);
    RexNode literal30 = rexBuilder.makeBigintLiteral(java.math.BigDecimal.valueOf(30));
    RexNode predicate = rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN, ageRef, literal30);
    FilterNode filter = new FilterNode(PlanNodeId.next("filter"), scan, predicate);

    // Project: name, age
    RexNode nameRef = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 0);
    RexNode ageRef2 = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.BIGINT), 1);
    ProjectNode project =
        new ProjectNode(PlanNodeId.next("project"), filter, List.of(nameRef, ageRef2), outType);

    List<Page> output = execute(project, sourcePages);

    // Should get 3 rows (Bob, Charlie, Eve) with 2 columns (name, age)
    int total = totalPositions(output);
    assertEquals(3, total, "Filter+Project should produce 3 rows");
    for (Page page : output) {
      assertEquals(2, page.getChannelCount(), "Should have 2 output columns");
    }

    List<Long> ages = collectLongs(output, 1);
    assertTrue(ages.containsAll(List.of(35L, 45L, 50L)), "Ages should be 35, 45, 50");
  }

  @Test
  @DisplayName("Filter with no matching rows produces empty output")
  void filterNoMatch() {
    List<Page> sourcePages =
        List.of(
            nameAgeStatusPage(
                new String[] {"Alice", "Bob"}, new long[] {25, 30}, new String[] {"a", "b"}));

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

    // Filter: age > 100 (no match)
    RexNode ageRef = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.BIGINT), 1);
    RexNode literal100 = rexBuilder.makeBigintLiteral(java.math.BigDecimal.valueOf(100));
    RexNode predicate = rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN, ageRef, literal100);
    FilterNode filter = new FilterNode(PlanNodeId.next("filter"), scan, predicate);

    List<Page> output = execute(filter, sourcePages);
    assertEquals(0, totalPositions(output), "No rows should match age > 100");
  }
}
