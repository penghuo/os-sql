/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.integration;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.distributed.data.DoubleArrayBlock;
import org.opensearch.sql.distributed.data.LongArrayBlock;
import org.opensearch.sql.distributed.data.Page;
import org.opensearch.sql.distributed.planner.AggregationNode;
import org.opensearch.sql.distributed.planner.LuceneTableScanNode;
import org.opensearch.sql.distributed.planner.PlanNodeId;

/**
 * IC-1 Test 2: Local execution of aggregation pipelines. Validates: source=test | stats count() by
 * status
 */
class LocalAggregationTest extends LocalExecutionTestBase {

  private final RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();

  @Test
  @DisplayName("COUNT(*) with GROUP BY produces correct group counts")
  void countByGroup() {
    // Schema: name(0:VARCHAR), age(1:LONG), status(2:VARCHAR)
    // Data: active×3, inactive×2
    List<Page> sourcePages =
        List.of(
            nameAgeStatusPage(
                new String[] {"Alice", "Bob", "Charlie", "Dave", "Eve"},
                new long[] {25, 35, 45, 20, 50},
                new String[] {"active", "active", "inactive", "active", "inactive"}));

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

    // COUNT(*) with GROUP BY status (column 2)
    AggregateCall countStar =
        AggregateCall.create(
            SqlStdOperatorTable.COUNT,
            false,
            List.of(),
            -1,
            typeFactory.createSqlType(SqlTypeName.BIGINT),
            "cnt");

    AggregationNode agg =
        new AggregationNode(
            PlanNodeId.next("agg"),
            scan,
            ImmutableBitSet.of(2), // group by status
            List.of(countStar),
            AggregationNode.AggregationMode.SINGLE);

    List<Page> output = execute(agg, sourcePages);

    // Should get 2 groups: active(3), inactive(2)
    int total = totalPositions(output);
    assertEquals(2, total, "Should have 2 groups");

    // Collect counts (second column in output: [groupKey, agg1])
    List<Long> counts = collectLongs(output, 1);
    assertTrue(counts.contains(3L), "active group should have count=3");
    assertTrue(counts.contains(2L), "inactive group should have count=2");
  }

  @Test
  @DisplayName("COUNT(*) without GROUP BY produces single row")
  void countStarNoGroupBy() {
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

    AggregateCall countStar =
        AggregateCall.create(
            SqlStdOperatorTable.COUNT,
            false,
            List.of(),
            -1,
            typeFactory.createSqlType(SqlTypeName.BIGINT),
            "cnt");

    AggregationNode agg =
        new AggregationNode(
            PlanNodeId.next("agg"),
            scan,
            ImmutableBitSet.of(), // no group by
            List.of(countStar),
            AggregationNode.AggregationMode.SINGLE);

    List<Page> output = execute(agg, sourcePages);

    // Single row with count=3
    assertEquals(1, totalPositions(output), "COUNT(*) without GROUP BY should produce 1 row");
    List<Long> counts = collectLongs(output, 0);
    assertEquals(1, counts.size());
    assertEquals(3L, counts.get(0), "Total count should be 3");
  }

  @Test
  @DisplayName("SUM(age) with GROUP BY produces correct sums")
  void sumByGroup() {
    // active ages: 25+35+20=80, inactive ages: 45+50=95
    List<Page> sourcePages =
        List.of(
            nameAgeStatusPage(
                new String[] {"Alice", "Bob", "Charlie", "Dave", "Eve"},
                new long[] {25, 35, 45, 20, 50},
                new String[] {"active", "active", "inactive", "active", "inactive"}));

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

    // SUM(age) GROUP BY status
    AggregateCall sumAge =
        AggregateCall.create(
            SqlStdOperatorTable.SUM,
            false,
            List.of(1),
            -1,
            typeFactory.createSqlType(SqlTypeName.DOUBLE),
            "sum_age");

    AggregationNode agg =
        new AggregationNode(
            PlanNodeId.next("agg"),
            scan,
            ImmutableBitSet.of(2), // group by status
            List.of(sumAge),
            AggregationNode.AggregationMode.SINGLE);

    List<Page> output = execute(agg, sourcePages);

    assertEquals(2, totalPositions(output), "Should have 2 groups");

    // Collect sums (DoubleArrayBlock from SumAccumulator)
    List<Double> sums = new java.util.ArrayList<>();
    for (Page page : output) {
      var block = page.getBlock(1);
      if (block instanceof DoubleArrayBlock doubleBlock) {
        for (int i = 0; i < doubleBlock.getPositionCount(); i++) {
          sums.add(doubleBlock.getDouble(i));
        }
      } else if (block instanceof LongArrayBlock longBlock) {
        for (int i = 0; i < longBlock.getPositionCount(); i++) {
          sums.add((double) longBlock.getLong(i));
        }
      }
    }
    assertTrue(sums.contains(80.0), "active group sum should be 80.0");
    assertTrue(sums.contains(95.0), "inactive group sum should be 95.0");
  }

  @Test
  @DisplayName("Aggregation on empty input produces correct result")
  void aggregateEmptyInput() {
    List<Page> sourcePages = List.of(); // no data

    RelDataType rowType =
        typeFactory
            .builder()
            .add("name", SqlTypeName.VARCHAR)
            .add("age", SqlTypeName.BIGINT)
            .build();

    LuceneTableScanNode scan =
        new LuceneTableScanNode(PlanNodeId.next("scan"), "test", rowType, List.of("name", "age"));

    AggregateCall countStar =
        AggregateCall.create(
            SqlStdOperatorTable.COUNT,
            false,
            List.of(),
            -1,
            typeFactory.createSqlType(SqlTypeName.BIGINT),
            "cnt");

    AggregationNode agg =
        new AggregationNode(
            PlanNodeId.next("agg"),
            scan,
            ImmutableBitSet.of(), // no group by
            List.of(countStar),
            AggregationNode.AggregationMode.SINGLE);

    List<Page> output = execute(agg, sourcePages);

    // COUNT(*) on empty input should return 1 row with count=0
    assertEquals(1, totalPositions(output), "COUNT(*) on empty should produce 1 row");
    List<Long> counts = collectLongs(output, 0);
    assertEquals(0L, counts.get(0), "Count of empty input should be 0");
  }
}
