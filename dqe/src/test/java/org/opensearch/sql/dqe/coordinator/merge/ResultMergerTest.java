/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.coordinator.merge;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.dqe.planner.plan.AggregationNode;

@DisplayName("ResultMerger: coordinator-side shard result combination")
class ResultMergerTest {

  private final ResultMerger merger = new ResultMerger();

  // ---------------------------------------------------------------------------
  // Cycle 13a: Passthrough merge
  // ---------------------------------------------------------------------------
  @Nested
  @DisplayName("Passthrough merge")
  class PassthroughMergeTests {

    @Test
    @DisplayName("Concatenates rows from multiple shards")
    void passthroughMerge() {
      List<String> shardResults =
          List.of("[{\"a\":1},{\"a\":2}]", "[{\"a\":3}]", "[{\"a\":4},{\"a\":5}]");

      List<Map<String, Object>> result = merger.mergePassthrough(shardResults);

      assertEquals(5, result.size());
      assertEquals(1, ((Number) result.get(0).get("a")).intValue());
      assertEquals(2, ((Number) result.get(1).get("a")).intValue());
      assertEquals(3, ((Number) result.get(2).get("a")).intValue());
      assertEquals(4, ((Number) result.get(3).get("a")).intValue());
      assertEquals(5, ((Number) result.get(4).get("a")).intValue());
    }

    @Test
    @DisplayName("Handles empty shard results")
    void passthroughMergeEmptyShards() {
      List<String> shardResults = List.of("[]", "[]");

      List<Map<String, Object>> result = merger.mergePassthrough(shardResults);

      assertTrue(result.isEmpty());
    }

    @Test
    @DisplayName("Handles single shard result")
    void passthroughMergeSingleShard() {
      List<String> shardResults = List.of("[{\"x\":\"hello\",\"y\":42}]");

      List<Map<String, Object>> result = merger.mergePassthrough(shardResults);

      assertEquals(1, result.size());
      assertEquals("hello", result.get(0).get("x"));
      assertEquals(42, ((Number) result.get(0).get("y")).intValue());
    }

    @Test
    @DisplayName("Handles mix of empty and non-empty shard results")
    void passthroughMergeMixedEmpty() {
      List<String> shardResults = List.of("[]", "[{\"a\":1}]", "[]", "[{\"a\":2}]");

      List<Map<String, Object>> result = merger.mergePassthrough(shardResults);

      assertEquals(2, result.size());
    }
  }

  // ---------------------------------------------------------------------------
  // Cycle 13b: Aggregation merge
  // ---------------------------------------------------------------------------
  @Nested
  @DisplayName("Aggregation merge")
  class AggregationMergeTests {

    @Test
    @DisplayName("Combines COUNT partial results across shards")
    void aggregationMergeCount() {
      List<String> shardResults =
          List.of(
              "[{\"category\":\"a\",\"COUNT(*)\":2},{\"category\":\"b\",\"COUNT(*)\":1}]",
              "[{\"category\":\"a\",\"COUNT(*)\":3},{\"category\":\"b\",\"COUNT(*)\":2}]");

      AggregationNode finalAgg =
          new AggregationNode(
              null, List.of("category"), List.of("COUNT(*)"), AggregationNode.Step.FINAL);

      List<Map<String, Object>> result = merger.mergeAggregation(shardResults, finalAgg);

      assertEquals(2, result.size());
      Map<String, Object> rowA =
          result.stream().filter(r -> "a".equals(r.get("category"))).findFirst().orElseThrow();
      assertEquals(5, ((Number) rowA.get("COUNT(*)")).intValue());

      Map<String, Object> rowB =
          result.stream().filter(r -> "b".equals(r.get("category"))).findFirst().orElseThrow();
      assertEquals(3, ((Number) rowB.get("COUNT(*)")).intValue());
    }

    @Test
    @DisplayName("Combines SUM partial results across shards")
    void aggregationMergeSum() {
      List<String> shardResults =
          List.of(
              "[{\"category\":\"a\",\"SUM(amount)\":100},{\"category\":\"b\",\"SUM(amount)\":50}]",
              "[{\"category\":\"a\",\"SUM(amount)\":200}]");

      AggregationNode finalAgg =
          new AggregationNode(
              null, List.of("category"), List.of("SUM(amount)"), AggregationNode.Step.FINAL);

      List<Map<String, Object>> result = merger.mergeAggregation(shardResults, finalAgg);

      Map<String, Object> rowA =
          result.stream().filter(r -> "a".equals(r.get("category"))).findFirst().orElseThrow();
      assertEquals(300, ((Number) rowA.get("SUM(amount)")).intValue());

      Map<String, Object> rowB =
          result.stream().filter(r -> "b".equals(r.get("category"))).findFirst().orElseThrow();
      assertEquals(50, ((Number) rowB.get("SUM(amount)")).intValue());
    }

    @Test
    @DisplayName("Combines MIN partial results across shards")
    void aggregationMergeMin() {
      List<String> shardResults =
          List.of(
              "[{\"category\":\"a\",\"MIN(price)\":10}]",
              "[{\"category\":\"a\",\"MIN(price)\":5}]",
              "[{\"category\":\"a\",\"MIN(price)\":8}]");

      AggregationNode finalAgg =
          new AggregationNode(
              null, List.of("category"), List.of("MIN(price)"), AggregationNode.Step.FINAL);

      List<Map<String, Object>> result = merger.mergeAggregation(shardResults, finalAgg);

      assertEquals(1, result.size());
      assertEquals(5, ((Number) result.get(0).get("MIN(price)")).intValue());
    }

    @Test
    @DisplayName("Combines MAX partial results across shards")
    void aggregationMergeMax() {
      List<String> shardResults =
          List.of(
              "[{\"category\":\"a\",\"MAX(price)\":10}]",
              "[{\"category\":\"a\",\"MAX(price)\":25}]",
              "[{\"category\":\"a\",\"MAX(price)\":8}]");

      AggregationNode finalAgg =
          new AggregationNode(
              null, List.of("category"), List.of("MAX(price)"), AggregationNode.Step.FINAL);

      List<Map<String, Object>> result = merger.mergeAggregation(shardResults, finalAgg);

      assertEquals(1, result.size());
      assertEquals(25, ((Number) result.get(0).get("MAX(price)")).intValue());
    }

    @Test
    @DisplayName("Combines multiple aggregate functions in a single query")
    void aggregationMergeMultipleFunctions() {
      List<String> shardResults =
          List.of(
              "[{\"dept\":\"eng\",\"COUNT(*)\":3,\"SUM(salary)\":300,\"MIN(salary)\":80,\"MAX(salary)\":120}]",
              "[{\"dept\":\"eng\",\"COUNT(*)\":2,\"SUM(salary)\":250,\"MIN(salary)\":100,\"MAX(salary)\":150}]");

      AggregationNode finalAgg =
          new AggregationNode(
              null,
              List.of("dept"),
              List.of("COUNT(*)", "SUM(salary)", "MIN(salary)", "MAX(salary)"),
              AggregationNode.Step.FINAL);

      List<Map<String, Object>> result = merger.mergeAggregation(shardResults, finalAgg);

      assertEquals(1, result.size());
      Map<String, Object> row = result.get(0);
      assertEquals("eng", row.get("dept"));
      assertEquals(5, ((Number) row.get("COUNT(*)")).intValue());
      assertEquals(550, ((Number) row.get("SUM(salary)")).intValue());
      assertEquals(80, ((Number) row.get("MIN(salary)")).intValue());
      assertEquals(150, ((Number) row.get("MAX(salary)")).intValue());
    }

    @Test
    @DisplayName("Aggregation with no group-by keys (global aggregation)")
    void aggregationMergeNoGroupBy() {
      List<String> shardResults =
          List.of("[{\"COUNT(*)\":10}]", "[{\"COUNT(*)\":20}]", "[{\"COUNT(*)\":15}]");

      AggregationNode finalAgg =
          new AggregationNode(null, List.of(), List.of("COUNT(*)"), AggregationNode.Step.FINAL);

      List<Map<String, Object>> result = merger.mergeAggregation(shardResults, finalAgg);

      assertEquals(1, result.size());
      assertEquals(45, ((Number) result.get(0).get("COUNT(*)")).intValue());
    }

    @Test
    @DisplayName("Aggregation where a group appears in only one shard")
    void aggregationMergeSingleShardGroup() {
      List<String> shardResults =
          List.of("[{\"category\":\"a\",\"COUNT(*)\":5}]", "[{\"category\":\"b\",\"COUNT(*)\":3}]");

      AggregationNode finalAgg =
          new AggregationNode(
              null, List.of("category"), List.of("COUNT(*)"), AggregationNode.Step.FINAL);

      List<Map<String, Object>> result = merger.mergeAggregation(shardResults, finalAgg);

      assertEquals(2, result.size());
      Map<String, Object> rowA =
          result.stream().filter(r -> "a".equals(r.get("category"))).findFirst().orElseThrow();
      assertEquals(5, ((Number) rowA.get("COUNT(*)")).intValue());

      Map<String, Object> rowB =
          result.stream().filter(r -> "b".equals(r.get("category"))).findFirst().orElseThrow();
      assertEquals(3, ((Number) rowB.get("COUNT(*)")).intValue());
    }
  }

  // ---------------------------------------------------------------------------
  // Cycle 13c: Sorted merge with limit
  // ---------------------------------------------------------------------------
  @Nested
  @DisplayName("Sorted merge with limit")
  class SortedMergeTests {

    @Test
    @DisplayName("Sorts combined results ascending and applies limit")
    void sortedMergeWithLimit() {
      List<String> shardResults = List.of("[{\"a\":3},{\"a\":1}]", "[{\"a\":4},{\"a\":2}]");

      List<Map<String, Object>> result =
          merger.mergeSorted(shardResults, List.of("a"), List.of(true), 3);

      assertEquals(3, result.size());
      assertEquals(1, ((Number) result.get(0).get("a")).intValue());
      assertEquals(2, ((Number) result.get(1).get("a")).intValue());
      assertEquals(3, ((Number) result.get(2).get("a")).intValue());
    }

    @Test
    @DisplayName("Sorts combined results descending")
    void sortedMergeDescending() {
      List<String> shardResults = List.of("[{\"a\":1},{\"a\":3}]", "[{\"a\":4},{\"a\":2}]");

      List<Map<String, Object>> result =
          merger.mergeSorted(shardResults, List.of("a"), List.of(false), 2);

      assertEquals(2, result.size());
      assertEquals(4, ((Number) result.get(0).get("a")).intValue());
      assertEquals(3, ((Number) result.get(1).get("a")).intValue());
    }

    @Test
    @DisplayName("Limit larger than total rows returns all rows sorted")
    void sortedMergeLimitLargerThanTotal() {
      List<String> shardResults = List.of("[{\"a\":3}]", "[{\"a\":1}]");

      List<Map<String, Object>> result =
          merger.mergeSorted(shardResults, List.of("a"), List.of(true), 100);

      assertEquals(2, result.size());
      assertEquals(1, ((Number) result.get(0).get("a")).intValue());
      assertEquals(3, ((Number) result.get(1).get("a")).intValue());
    }

    @Test
    @DisplayName("Multi-key sort with mixed ascending/descending")
    void sortedMergeMultiKey() {
      List<String> shardResults =
          List.of(
              "[{\"x\":1,\"y\":10},{\"x\":2,\"y\":5}]", "[{\"x\":1,\"y\":20},{\"x\":2,\"y\":3}]");

      // Sort by x ascending, then y descending
      List<Map<String, Object>> result =
          merger.mergeSorted(shardResults, List.of("x", "y"), List.of(true, false), 4);

      assertEquals(4, result.size());
      // x=1, y=20 first (x asc, y desc)
      assertEquals(1, ((Number) result.get(0).get("x")).intValue());
      assertEquals(20, ((Number) result.get(0).get("y")).intValue());
      // x=1, y=10
      assertEquals(1, ((Number) result.get(1).get("x")).intValue());
      assertEquals(10, ((Number) result.get(1).get("y")).intValue());
      // x=2, y=5
      assertEquals(2, ((Number) result.get(2).get("x")).intValue());
      assertEquals(5, ((Number) result.get(2).get("y")).intValue());
      // x=2, y=3
      assertEquals(2, ((Number) result.get(3).get("x")).intValue());
      assertEquals(3, ((Number) result.get(3).get("y")).intValue());
    }

    @Test
    @DisplayName("Sorted merge with string values")
    void sortedMergeStrings() {
      List<String> shardResults =
          List.of("[{\"name\":\"charlie\"}]", "[{\"name\":\"alice\"},{\"name\":\"bob\"}]");

      List<Map<String, Object>> result =
          merger.mergeSorted(shardResults, List.of("name"), List.of(true), 3);

      assertEquals(3, result.size());
      assertEquals("alice", result.get(0).get("name"));
      assertEquals("bob", result.get(1).get("name"));
      assertEquals("charlie", result.get(2).get("name"));
    }

    @Test
    @DisplayName("Sorted merge with empty shard results")
    void sortedMergeEmpty() {
      List<String> shardResults = List.of("[]", "[{\"a\":1}]", "[]");

      List<Map<String, Object>> result =
          merger.mergeSorted(shardResults, List.of("a"), List.of(true), 10);

      assertEquals(1, result.size());
      assertEquals(1, ((Number) result.get(0).get("a")).intValue());
    }
  }
}
