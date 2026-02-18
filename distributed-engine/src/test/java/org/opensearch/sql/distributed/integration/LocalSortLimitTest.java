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
import org.opensearch.sql.distributed.data.Page;
import org.opensearch.sql.distributed.planner.LimitNode;
import org.opensearch.sql.distributed.planner.LuceneTableScanNode;
import org.opensearch.sql.distributed.planner.PlanNodeId;
import org.opensearch.sql.distributed.planner.SortNode;
import org.opensearch.sql.distributed.planner.TopNNode;

/**
 * IC-1 Test 3: Local execution of sort and limit pipelines. Validates: source=test | sort age |
 * head 10
 */
class LocalSortLimitTest extends LocalExecutionTestBase {

  private final RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();

  @Test
  @DisplayName("Sort by age ascending produces correctly ordered output")
  void sortByAgeAscending() {
    List<Page> sourcePages =
        List.of(
            nameAgeStatusPage(
                new String[] {"Charlie", "Alice", "Eve", "Bob", "Dave"},
                new long[] {45, 25, 50, 35, 20},
                new String[] {"c", "a", "e", "b", "d"}));

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

    // Sort by age (column 1) ascending
    var collation =
        RelCollations.of(new RelFieldCollation(1, RelFieldCollation.Direction.ASCENDING));
    SortNode sort = new SortNode(PlanNodeId.next("sort"), scan, collation);

    List<Page> output = execute(sort, sourcePages);

    assertEquals(5, totalPositions(output));
    List<Long> ages = collectLongs(output, 1);
    // Verify ascending order
    for (int i = 1; i < ages.size(); i++) {
      assertTrue(
          ages.get(i) >= ages.get(i - 1),
          "Ages should be ascending: " + ages.get(i - 1) + " <= " + ages.get(i));
    }
    assertEquals(List.of(20L, 25L, 35L, 45L, 50L), ages);
  }

  @Test
  @DisplayName("TopN: sort age | head 3 returns top 3 youngest")
  void topNReturnsLimitedRows() {
    List<Page> sourcePages =
        List.of(
            nameAgeStatusPage(
                new String[] {"Charlie", "Alice", "Eve", "Bob", "Dave"},
                new long[] {45, 25, 50, 35, 20},
                new String[] {"c", "a", "e", "b", "d"}));

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

    // TopN: sort by age ascending, limit 3
    var collation =
        RelCollations.of(new RelFieldCollation(1, RelFieldCollation.Direction.ASCENDING));
    TopNNode topN = new TopNNode(PlanNodeId.next("topn"), scan, collation, 3);

    List<Page> output = execute(topN, sourcePages);

    assertEquals(3, totalPositions(output), "TopN with limit=3 should return 3 rows");
    List<Long> ages = collectLongs(output, 1);
    assertEquals(List.of(20L, 25L, 35L), ages, "Should return youngest 3 in order");
  }

  @Test
  @DisplayName("Sort descending returns rows in reverse order")
  void sortDescending() {
    List<Page> sourcePages =
        List.of(
            nameAgeStatusPage(
                new String[] {"Alice", "Bob", "Charlie"},
                new long[] {25, 35, 45},
                new String[] {"a", "b", "c"}));

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
        RelCollations.of(new RelFieldCollation(1, RelFieldCollation.Direction.DESCENDING));
    SortNode sort = new SortNode(PlanNodeId.next("sort"), scan, collation);

    List<Page> output = execute(sort, sourcePages);

    List<Long> ages = collectLongs(output, 1);
    assertEquals(List.of(45L, 35L, 25L), ages, "Should be descending");
  }

  @Test
  @DisplayName("Limit without sort preserves input order")
  void limitWithoutSort() {
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

    LimitNode limit = new LimitNode(PlanNodeId.next("limit"), scan, 2, 0);

    List<Page> output = execute(limit, sourcePages);

    assertEquals(2, totalPositions(output), "Limit 2 should return 2 rows");
  }

  @Test
  @DisplayName("TopN with limit larger than input returns all rows sorted")
  void topNLimitLargerThanInput() {
    List<Page> sourcePages =
        List.of(
            nameAgeStatusPage(
                new String[] {"Bob", "Alice"}, new long[] {35, 25}, new String[] {"b", "a"}));

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
    TopNNode topN = new TopNNode(PlanNodeId.next("topn"), scan, collation, 100);

    List<Page> output = execute(topN, sourcePages);

    assertEquals(2, totalPositions(output), "Should return all 2 rows");
    List<Long> ages = collectLongs(output, 1);
    assertEquals(List.of(25L, 35L), ages, "Should be sorted ascending");
  }
}
