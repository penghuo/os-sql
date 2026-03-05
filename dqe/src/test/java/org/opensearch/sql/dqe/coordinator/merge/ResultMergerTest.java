/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.coordinator.merge;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.airlift.slice.Slices;
import io.trino.spi.Page;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.dqe.planner.plan.AggregationNode;

@DisplayName("ResultMerger: coordinator-side shard result combination (Page-based)")
class ResultMergerTest {

  private final ResultMerger merger = new ResultMerger();

  // ---------------------------------------------------------------------------
  // Passthrough merge
  // ---------------------------------------------------------------------------
  @Nested
  @DisplayName("Passthrough merge")
  class PassthroughMergeTests {

    @Test
    @DisplayName("Concatenates pages from multiple shards")
    void passthroughMerge() {
      // Shard 0: [1, 2], Shard 1: [3], Shard 2: [4, 5]
      List<List<Page>> shardResults =
          List.of(
              List.of(buildBigintPage(1L, 2L)),
              List.of(buildBigintPage(3L)),
              List.of(buildBigintPage(4L, 5L)));

      List<Page> result = merger.mergePassthrough(shardResults);

      assertEquals(3, result.size());
      int totalRows = result.stream().mapToInt(Page::getPositionCount).sum();
      assertEquals(5, totalRows);
    }

    @Test
    @DisplayName("Handles empty shard results")
    void passthroughMergeEmptyShards() {
      List<List<Page>> shardResults = List.of(List.of(), List.of());

      List<Page> result = merger.mergePassthrough(shardResults);

      assertTrue(result.isEmpty());
    }

    @Test
    @DisplayName("Handles single shard result")
    void passthroughMergeSingleShard() {
      List<List<Page>> shardResults = List.of(List.of(buildBigintPage(42L)));

      List<Page> result = merger.mergePassthrough(shardResults);

      assertEquals(1, result.size());
      assertEquals(1, result.get(0).getPositionCount());
      assertEquals(42L, BigintType.BIGINT.getLong(result.get(0).getBlock(0), 0));
    }

    @Test
    @DisplayName("Handles mix of empty and non-empty shard results")
    void passthroughMergeMixedEmpty() {
      List<List<Page>> shardResults =
          List.of(List.of(), List.of(buildBigintPage(1L)), List.of(), List.of(buildBigintPage(2L)));

      List<Page> result = merger.mergePassthrough(shardResults);

      assertEquals(2, result.size());
    }
  }

  // ---------------------------------------------------------------------------
  // Aggregation merge
  // ---------------------------------------------------------------------------
  @Nested
  @DisplayName("Aggregation merge")
  class AggregationMergeTests {

    @Test
    @DisplayName("Combines COUNT partial results across shards")
    void aggregationMergeCount() {
      // Shard 0: category=a count=2, category=b count=1
      // Shard 1: category=a count=3, category=b count=2
      List<Type> columnTypes = List.of(VarcharType.VARCHAR, BigintType.BIGINT);

      List<List<Page>> shardResults =
          List.of(
              List.of(buildCategoryCountPage("a", 2L, "b", 1L)),
              List.of(buildCategoryCountPage("a", 3L, "b", 2L)));

      AggregationNode finalAgg =
          new AggregationNode(
              null, List.of("category"), List.of("COUNT(*)"), AggregationNode.Step.FINAL);

      List<Page> result = merger.mergeAggregation(shardResults, finalAgg, columnTypes);

      assertEquals(1, result.size());
      Page page = result.get(0);
      assertEquals(2, page.getPositionCount()); // 2 groups

      // Find rows by category
      long countA = 0;
      long countB = 0;
      for (int i = 0; i < page.getPositionCount(); i++) {
        String cat = VarcharType.VARCHAR.getSlice(page.getBlock(0), i).toStringUtf8();
        long count = BigintType.BIGINT.getLong(page.getBlock(1), i);
        if ("a".equals(cat)) countA = count;
        else if ("b".equals(cat)) countB = count;
      }
      assertEquals(5, countA); // 2 + 3
      assertEquals(3, countB); // 1 + 2
    }

    @Test
    @DisplayName("Combines SUM partial results across shards")
    void aggregationMergeSum() {
      List<Type> columnTypes = List.of(VarcharType.VARCHAR, BigintType.BIGINT);

      List<List<Page>> shardResults =
          List.of(
              List.of(buildCategoryCountPage("a", 100L, "b", 50L)),
              List.of(buildCategoryCountPage("a", 200L)));

      AggregationNode finalAgg =
          new AggregationNode(
              null, List.of("category"), List.of("SUM(amount)"), AggregationNode.Step.FINAL);

      List<Page> result = merger.mergeAggregation(shardResults, finalAgg, columnTypes);

      Page page = result.get(0);
      long sumA = 0;
      long sumB = 0;
      for (int i = 0; i < page.getPositionCount(); i++) {
        String cat = VarcharType.VARCHAR.getSlice(page.getBlock(0), i).toStringUtf8();
        long val = BigintType.BIGINT.getLong(page.getBlock(1), i);
        if ("a".equals(cat)) sumA = val;
        else if ("b".equals(cat)) sumB = val;
      }
      assertEquals(300, sumA);
      assertEquals(50, sumB);
    }

    @Test
    @DisplayName("Combines MIN partial results across shards")
    void aggregationMergeMin() {
      List<Type> columnTypes = List.of(VarcharType.VARCHAR, BigintType.BIGINT);

      List<List<Page>> shardResults =
          List.of(
              List.of(buildCategoryCountPage("a", 10L)),
              List.of(buildCategoryCountPage("a", 5L)),
              List.of(buildCategoryCountPage("a", 8L)));

      AggregationNode finalAgg =
          new AggregationNode(
              null, List.of("category"), List.of("MIN(price)"), AggregationNode.Step.FINAL);

      List<Page> result = merger.mergeAggregation(shardResults, finalAgg, columnTypes);

      assertEquals(1, result.size());
      Page page = result.get(0);
      assertEquals(1, page.getPositionCount());
      assertEquals(5L, BigintType.BIGINT.getLong(page.getBlock(1), 0));
    }

    @Test
    @DisplayName("Combines MAX partial results across shards")
    void aggregationMergeMax() {
      List<Type> columnTypes = List.of(VarcharType.VARCHAR, BigintType.BIGINT);

      List<List<Page>> shardResults =
          List.of(
              List.of(buildCategoryCountPage("a", 10L)),
              List.of(buildCategoryCountPage("a", 25L)),
              List.of(buildCategoryCountPage("a", 8L)));

      AggregationNode finalAgg =
          new AggregationNode(
              null, List.of("category"), List.of("MAX(price)"), AggregationNode.Step.FINAL);

      List<Page> result = merger.mergeAggregation(shardResults, finalAgg, columnTypes);

      assertEquals(1, result.size());
      Page page = result.get(0);
      assertEquals(1, page.getPositionCount());
      assertEquals(25L, BigintType.BIGINT.getLong(page.getBlock(1), 0));
    }

    @Test
    @DisplayName("Combines multiple aggregate functions in a single query")
    void aggregationMergeMultipleFunctions() {
      // 4 agg columns: COUNT(*), SUM(salary), MIN(salary), MAX(salary)
      List<Type> columnTypes =
          List.of(
              VarcharType.VARCHAR,
              BigintType.BIGINT,
              BigintType.BIGINT,
              BigintType.BIGINT,
              BigintType.BIGINT);

      // Shard 0: (eng, 3, 300, 80, 120)
      // Shard 1: (eng, 2, 250, 100, 150)
      List<List<Page>> shardResults =
          List.of(
              List.of(buildMultiAggPage("eng", 3L, 300L, 80L, 120L)),
              List.of(buildMultiAggPage("eng", 2L, 250L, 100L, 150L)));

      AggregationNode finalAgg =
          new AggregationNode(
              null,
              List.of("dept"),
              List.of("COUNT(*)", "SUM(salary)", "MIN(salary)", "MAX(salary)"),
              AggregationNode.Step.FINAL);

      List<Page> result = merger.mergeAggregation(shardResults, finalAgg, columnTypes);

      assertEquals(1, result.size());
      Page page = result.get(0);
      assertEquals(1, page.getPositionCount());
      assertEquals("eng", VarcharType.VARCHAR.getSlice(page.getBlock(0), 0).toStringUtf8());
      assertEquals(5L, BigintType.BIGINT.getLong(page.getBlock(1), 0)); // COUNT: 3+2
      assertEquals(550L, BigintType.BIGINT.getLong(page.getBlock(2), 0)); // SUM: 300+250
      assertEquals(80L, BigintType.BIGINT.getLong(page.getBlock(3), 0)); // MIN: min(80,100)
      assertEquals(150L, BigintType.BIGINT.getLong(page.getBlock(4), 0)); // MAX: max(120,150)
    }

    @Test
    @DisplayName("Aggregation with no group-by keys (global aggregation)")
    void aggregationMergeNoGroupBy() {
      List<Type> columnTypes = List.of(BigintType.BIGINT);

      List<List<Page>> shardResults =
          List.of(
              List.of(buildBigintPage(10L)),
              List.of(buildBigintPage(20L)),
              List.of(buildBigintPage(15L)));

      AggregationNode finalAgg =
          new AggregationNode(null, List.of(), List.of("COUNT(*)"), AggregationNode.Step.FINAL);

      List<Page> result = merger.mergeAggregation(shardResults, finalAgg, columnTypes);

      assertEquals(1, result.size());
      Page page = result.get(0);
      assertEquals(1, page.getPositionCount());
      assertEquals(45L, BigintType.BIGINT.getLong(page.getBlock(0), 0)); // 10+20+15
    }

    @Test
    @DisplayName("Aggregation where a group appears in only one shard")
    void aggregationMergeSingleShardGroup() {
      List<Type> columnTypes = List.of(VarcharType.VARCHAR, BigintType.BIGINT);

      List<List<Page>> shardResults =
          List.of(
              List.of(buildCategoryCountPage("a", 5L)), List.of(buildCategoryCountPage("b", 3L)));

      AggregationNode finalAgg =
          new AggregationNode(
              null, List.of("category"), List.of("COUNT(*)"), AggregationNode.Step.FINAL);

      List<Page> result = merger.mergeAggregation(shardResults, finalAgg, columnTypes);

      assertEquals(1, result.size());
      Page page = result.get(0);
      assertEquals(2, page.getPositionCount());

      long countA = 0;
      long countB = 0;
      for (int i = 0; i < page.getPositionCount(); i++) {
        String cat = VarcharType.VARCHAR.getSlice(page.getBlock(0), i).toStringUtf8();
        long count = BigintType.BIGINT.getLong(page.getBlock(1), i);
        if ("a".equals(cat)) countA = count;
        else if ("b".equals(cat)) countB = count;
      }
      assertEquals(5, countA);
      assertEquals(3, countB);
    }
  }

  // ---------------------------------------------------------------------------
  // Sorted merge with limit
  // ---------------------------------------------------------------------------
  @Nested
  @DisplayName("Sorted merge with limit")
  class SortedMergeTests {

    @Test
    @DisplayName("Sorts combined results ascending and applies limit")
    void sortedMergeWithLimit() {
      List<Type> columnTypes = List.of(BigintType.BIGINT);

      List<List<Page>> shardResults =
          List.of(List.of(buildBigintPage(3L, 1L)), List.of(buildBigintPage(4L, 2L)));

      List<Page> result =
          merger.mergeSorted(shardResults, List.of(0), List.of(true), columnTypes, 3);

      int totalRows = result.stream().mapToInt(Page::getPositionCount).sum();
      assertEquals(3, totalRows);

      Page page = result.get(0);
      assertEquals(1L, BigintType.BIGINT.getLong(page.getBlock(0), 0));
      assertEquals(2L, BigintType.BIGINT.getLong(page.getBlock(0), 1));
      assertEquals(3L, BigintType.BIGINT.getLong(page.getBlock(0), 2));
    }

    @Test
    @DisplayName("Sorts combined results descending")
    void sortedMergeDescending() {
      List<Type> columnTypes = List.of(BigintType.BIGINT);

      List<List<Page>> shardResults =
          List.of(List.of(buildBigintPage(1L, 3L)), List.of(buildBigintPage(4L, 2L)));

      List<Page> result =
          merger.mergeSorted(shardResults, List.of(0), List.of(false), columnTypes, 2);

      Page page = result.get(0);
      assertEquals(2, page.getPositionCount());
      assertEquals(4L, BigintType.BIGINT.getLong(page.getBlock(0), 0));
      assertEquals(3L, BigintType.BIGINT.getLong(page.getBlock(0), 1));
    }

    @Test
    @DisplayName("Limit larger than total rows returns all rows sorted")
    void sortedMergeLimitLargerThanTotal() {
      List<Type> columnTypes = List.of(BigintType.BIGINT);

      List<List<Page>> shardResults =
          List.of(List.of(buildBigintPage(3L)), List.of(buildBigintPage(1L)));

      List<Page> result =
          merger.mergeSorted(shardResults, List.of(0), List.of(true), columnTypes, 100);

      Page page = result.get(0);
      assertEquals(2, page.getPositionCount());
      assertEquals(1L, BigintType.BIGINT.getLong(page.getBlock(0), 0));
      assertEquals(3L, BigintType.BIGINT.getLong(page.getBlock(0), 1));
    }

    @Test
    @DisplayName("Multi-key sort with mixed ascending/descending")
    void sortedMergeMultiKey() {
      List<Type> columnTypes = List.of(BigintType.BIGINT, BigintType.BIGINT);

      // Shard 0: (1,10), (2,5); Shard 1: (1,20), (2,3)
      List<List<Page>> shardResults =
          List.of(
              List.of(buildTwoColumnBigintPage(1L, 10L, 2L, 5L)),
              List.of(buildTwoColumnBigintPage(1L, 20L, 2L, 3L)));

      // Sort by col 0 ascending, then col 1 descending
      List<Page> result =
          merger.mergeSorted(shardResults, List.of(0, 1), List.of(true, false), columnTypes, 4);

      Page page = result.get(0);
      assertEquals(4, page.getPositionCount());
      // x=1, y=20 first (x asc, y desc)
      assertEquals(1L, BigintType.BIGINT.getLong(page.getBlock(0), 0));
      assertEquals(20L, BigintType.BIGINT.getLong(page.getBlock(1), 0));
      // x=1, y=10
      assertEquals(1L, BigintType.BIGINT.getLong(page.getBlock(0), 1));
      assertEquals(10L, BigintType.BIGINT.getLong(page.getBlock(1), 1));
      // x=2, y=5
      assertEquals(2L, BigintType.BIGINT.getLong(page.getBlock(0), 2));
      assertEquals(5L, BigintType.BIGINT.getLong(page.getBlock(1), 2));
      // x=2, y=3
      assertEquals(2L, BigintType.BIGINT.getLong(page.getBlock(0), 3));
      assertEquals(3L, BigintType.BIGINT.getLong(page.getBlock(1), 3));
    }

    @Test
    @DisplayName("Sorted merge with string values")
    void sortedMergeStrings() {
      List<Type> columnTypes = List.of(VarcharType.VARCHAR);

      List<List<Page>> shardResults =
          List.of(List.of(buildVarcharPage("charlie")), List.of(buildVarcharPage("alice", "bob")));

      List<Page> result =
          merger.mergeSorted(shardResults, List.of(0), List.of(true), columnTypes, 3);

      Page page = result.get(0);
      assertEquals(3, page.getPositionCount());
      assertEquals("alice", VarcharType.VARCHAR.getSlice(page.getBlock(0), 0).toStringUtf8());
      assertEquals("bob", VarcharType.VARCHAR.getSlice(page.getBlock(0), 1).toStringUtf8());
      assertEquals("charlie", VarcharType.VARCHAR.getSlice(page.getBlock(0), 2).toStringUtf8());
    }

    @Test
    @DisplayName("Sorted merge with empty shard results")
    void sortedMergeEmpty() {
      List<Type> columnTypes = List.of(BigintType.BIGINT);

      List<List<Page>> shardResults = List.of(List.of(), List.of(buildBigintPage(1L)), List.of());

      List<Page> result =
          merger.mergeSorted(shardResults, List.of(0), List.of(true), columnTypes, 10);

      int totalRows = result.stream().mapToInt(Page::getPositionCount).sum();
      assertEquals(1, totalRows);
      assertEquals(1L, BigintType.BIGINT.getLong(result.get(0).getBlock(0), 0));
    }
  }

  // -- Helper methods to build test Pages --

  /** Build a single-column BIGINT page with the given values. */
  private static Page buildBigintPage(long... values) {
    BlockBuilder builder = BigintType.BIGINT.createBlockBuilder(null, values.length);
    for (long v : values) {
      BigintType.BIGINT.writeLong(builder, v);
    }
    return new Page(builder.build());
  }

  /** Build a two-column page: VARCHAR (category) and BIGINT (count) from paired entries. */
  private static Page buildCategoryCountPage(Object... entries) {
    int numRows = entries.length / 2;
    BlockBuilder catBuilder = VarcharType.VARCHAR.createBlockBuilder(null, numRows);
    BlockBuilder valBuilder = BigintType.BIGINT.createBlockBuilder(null, numRows);

    for (int i = 0; i < entries.length; i += 2) {
      VarcharType.VARCHAR.writeSlice(catBuilder, Slices.utf8Slice((String) entries[i]));
      BigintType.BIGINT.writeLong(valBuilder, ((Number) entries[i + 1]).longValue());
    }
    return new Page(catBuilder.build(), valBuilder.build());
  }

  /** Build a multi-agg page: VARCHAR (dept) + 4 BIGINT columns. */
  private static Page buildMultiAggPage(String dept, long count, long sum, long min, long max) {
    BlockBuilder deptBuilder = VarcharType.VARCHAR.createBlockBuilder(null, 1);
    VarcharType.VARCHAR.writeSlice(deptBuilder, Slices.utf8Slice(dept));

    BlockBuilder countBuilder = BigintType.BIGINT.createBlockBuilder(null, 1);
    BigintType.BIGINT.writeLong(countBuilder, count);

    BlockBuilder sumBuilder = BigintType.BIGINT.createBlockBuilder(null, 1);
    BigintType.BIGINT.writeLong(sumBuilder, sum);

    BlockBuilder minBuilder = BigintType.BIGINT.createBlockBuilder(null, 1);
    BigintType.BIGINT.writeLong(minBuilder, min);

    BlockBuilder maxBuilder = BigintType.BIGINT.createBlockBuilder(null, 1);
    BigintType.BIGINT.writeLong(maxBuilder, max);

    return new Page(
        deptBuilder.build(),
        countBuilder.build(),
        sumBuilder.build(),
        minBuilder.build(),
        maxBuilder.build());
  }

  /** Build a two-column BIGINT page from paired values. */
  private static Page buildTwoColumnBigintPage(long... values) {
    int numRows = values.length / 2;
    BlockBuilder col0 = BigintType.BIGINT.createBlockBuilder(null, numRows);
    BlockBuilder col1 = BigintType.BIGINT.createBlockBuilder(null, numRows);
    for (int i = 0; i < values.length; i += 2) {
      BigintType.BIGINT.writeLong(col0, values[i]);
      BigintType.BIGINT.writeLong(col1, values[i + 1]);
    }
    return new Page(col0.build(), col1.build());
  }

  /** Build a single-column VARCHAR page with the given values. */
  private static Page buildVarcharPage(String... values) {
    BlockBuilder builder = VarcharType.VARCHAR.createBlockBuilder(null, values.length);
    for (String v : values) {
      VarcharType.VARCHAR.writeSlice(builder, Slices.utf8Slice(v));
    }
    return new Page(builder.build());
  }
}
