/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.opensearch.sql.dqe.operator.TestPageSource.buildCategoryValuePage;

import io.trino.spi.Page;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("HashAggregationOperator")
class HashAggregationOperatorTest {

  @Test
  @DisplayName("COUNT(*) with single group-by key")
  void countWithGroupBy() {
    // Rows: [("a", 1), ("b", 2), ("a", 3)]
    Page input = buildCategoryValuePage("a", 1L, "b", 2L, "a", 3L);
    Operator source = new TestPageSource(List.of(input));
    List<Type> columnTypes = List.of(VarcharType.VARCHAR, BigintType.BIGINT);

    HashAggregationOperator agg =
        new HashAggregationOperator(
            source,
            List.of(0), // group by column 0 (category)
            List.of(HashAggregationOperator.count()),
            columnTypes);

    Page result = agg.processNextBatch();
    assertNotNull(result);
    assertEquals(2, result.getPositionCount()); // 2 groups: "a" and "b"
    assertEquals(2, result.getChannelCount()); // category + count

    // Extract results into a map for order-independent assertions
    Map<String, Long> counts = new HashMap<>();
    for (int i = 0; i < result.getPositionCount(); i++) {
      String key = VarcharType.VARCHAR.getSlice(result.getBlock(0), i).toStringUtf8();
      long count = BigintType.BIGINT.getLong(result.getBlock(1), i);
      counts.put(key, count);
    }
    assertEquals(2L, counts.get("a"));
    assertEquals(1L, counts.get("b"));

    // Subsequent call returns null
    assertNull(agg.processNextBatch());
  }

  @Test
  @DisplayName("SUM with single group-by key")
  void sumWithGroupBy() {
    // Rows: [("a", 10), ("b", 20), ("a", 30)]
    Page input = buildCategoryValuePage("a", 10L, "b", 20L, "a", 30L);
    Operator source = new TestPageSource(List.of(input));
    List<Type> columnTypes = List.of(VarcharType.VARCHAR, BigintType.BIGINT);

    HashAggregationOperator agg =
        new HashAggregationOperator(
            source,
            List.of(0),
            List.of(HashAggregationOperator.sum(1, BigintType.BIGINT)),
            columnTypes);

    Page result = agg.processNextBatch();
    assertNotNull(result);

    Map<String, Long> sums = new HashMap<>();
    for (int i = 0; i < result.getPositionCount(); i++) {
      String key = VarcharType.VARCHAR.getSlice(result.getBlock(0), i).toStringUtf8();
      long sum = BigintType.BIGINT.getLong(result.getBlock(1), i);
      sums.put(key, sum);
    }
    assertEquals(40L, sums.get("a"));
    assertEquals(20L, sums.get("b"));
  }

  @Test
  @DisplayName("MIN with single group-by key")
  void minWithGroupBy() {
    // Rows: [("a", 10), ("b", 20), ("a", 30)]
    Page input = buildCategoryValuePage("a", 10L, "b", 20L, "a", 30L);
    Operator source = new TestPageSource(List.of(input));
    List<Type> columnTypes = List.of(VarcharType.VARCHAR, BigintType.BIGINT);

    HashAggregationOperator agg =
        new HashAggregationOperator(
            source,
            List.of(0),
            List.of(HashAggregationOperator.min(1, BigintType.BIGINT)),
            columnTypes);

    Page result = agg.processNextBatch();
    assertNotNull(result);

    Map<String, Long> mins = new HashMap<>();
    for (int i = 0; i < result.getPositionCount(); i++) {
      String key = VarcharType.VARCHAR.getSlice(result.getBlock(0), i).toStringUtf8();
      long min = BigintType.BIGINT.getLong(result.getBlock(1), i);
      mins.put(key, min);
    }
    assertEquals(10L, mins.get("a"));
    assertEquals(20L, mins.get("b"));
  }

  @Test
  @DisplayName("MAX with single group-by key")
  void maxWithGroupBy() {
    // Rows: [("a", 10), ("b", 20), ("a", 30)]
    Page input = buildCategoryValuePage("a", 10L, "b", 20L, "a", 30L);
    Operator source = new TestPageSource(List.of(input));
    List<Type> columnTypes = List.of(VarcharType.VARCHAR, BigintType.BIGINT);

    HashAggregationOperator agg =
        new HashAggregationOperator(
            source,
            List.of(0),
            List.of(HashAggregationOperator.max(1, BigintType.BIGINT)),
            columnTypes);

    Page result = agg.processNextBatch();
    assertNotNull(result);

    Map<String, Long> maxs = new HashMap<>();
    for (int i = 0; i < result.getPositionCount(); i++) {
      String key = VarcharType.VARCHAR.getSlice(result.getBlock(0), i).toStringUtf8();
      long max = BigintType.BIGINT.getLong(result.getBlock(1), i);
      maxs.put(key, max);
    }
    assertEquals(30L, maxs.get("a"));
    assertEquals(20L, maxs.get("b"));
  }

  @Test
  @DisplayName("AVG with single group-by key")
  void avgWithGroupBy() {
    // Rows: [("a", 10), ("b", 20), ("a", 30)]
    Page input = buildCategoryValuePage("a", 10L, "b", 20L, "a", 30L);
    Operator source = new TestPageSource(List.of(input));
    List<Type> columnTypes = List.of(VarcharType.VARCHAR, BigintType.BIGINT);

    HashAggregationOperator agg =
        new HashAggregationOperator(
            source,
            List.of(0),
            List.of(HashAggregationOperator.avg(1, BigintType.BIGINT)),
            columnTypes);

    Page result = agg.processNextBatch();
    assertNotNull(result);

    Map<String, Double> avgs = new HashMap<>();
    for (int i = 0; i < result.getPositionCount(); i++) {
      String key = VarcharType.VARCHAR.getSlice(result.getBlock(0), i).toStringUtf8();
      double avg = DoubleType.DOUBLE.getDouble(result.getBlock(1), i);
      avgs.put(key, avg);
    }
    assertEquals(20.0, avgs.get("a"), 0.001);
    assertEquals(20.0, avgs.get("b"), 0.001);
  }

  @Test
  @DisplayName("Multiple aggregates in single query")
  void multipleAggregates() {
    Page input = buildCategoryValuePage("a", 10L, "b", 20L, "a", 30L);
    Operator source = new TestPageSource(List.of(input));
    List<Type> columnTypes = List.of(VarcharType.VARCHAR, BigintType.BIGINT);

    HashAggregationOperator agg =
        new HashAggregationOperator(
            source,
            List.of(0),
            List.of(
                HashAggregationOperator.count(), HashAggregationOperator.sum(1, BigintType.BIGINT)),
            columnTypes);

    Page result = agg.processNextBatch();
    assertNotNull(result);
    assertEquals(3, result.getChannelCount()); // category + count + sum
    assertEquals(2, result.getPositionCount()); // 2 groups
  }

  @Test
  @DisplayName("Aggregation across multiple input pages")
  void acrossMultiplePages() {
    Page page1 = buildCategoryValuePage("a", 10L, "b", 20L);
    Page page2 = buildCategoryValuePage("a", 30L, "b", 40L);
    Operator source = new TestPageSource(List.of(page1, page2));
    List<Type> columnTypes = List.of(VarcharType.VARCHAR, BigintType.BIGINT);

    HashAggregationOperator agg =
        new HashAggregationOperator(
            source,
            List.of(0),
            List.of(HashAggregationOperator.sum(1, BigintType.BIGINT)),
            columnTypes);

    Page result = agg.processNextBatch();
    assertNotNull(result);

    Map<String, Long> sums = new HashMap<>();
    for (int i = 0; i < result.getPositionCount(); i++) {
      String key = VarcharType.VARCHAR.getSlice(result.getBlock(0), i).toStringUtf8();
      long sum = BigintType.BIGINT.getLong(result.getBlock(1), i);
      sums.put(key, sum);
    }
    assertEquals(40L, sums.get("a"));
    assertEquals(60L, sums.get("b"));
  }

  @Test
  @DisplayName("Returns null for empty source")
  void emptySource() {
    Operator source = new TestPageSource(List.of());
    List<Type> columnTypes = List.of(VarcharType.VARCHAR, BigintType.BIGINT);

    HashAggregationOperator agg =
        new HashAggregationOperator(
            source, List.of(0), List.of(HashAggregationOperator.count()), columnTypes);

    assertNull(agg.processNextBatch());
  }
}
