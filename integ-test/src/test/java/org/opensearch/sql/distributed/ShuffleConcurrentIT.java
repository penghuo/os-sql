/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;
import org.junit.jupiter.api.DisplayName;

/**
 * IC-4.6: Shuffle concurrency integration test. Verifies that 5+ concurrent multi-stage queries
 * (joins, aggregations, sorts) complete correctly with no deadlocks or cross-contamination.
 */
public class ShuffleConcurrentIT extends ShuffleITBase {

  private static final int CONCURRENT_QUERIES = 5;

  @Test
  @DisplayName("5 concurrent multi-stage queries all succeed")
  public void testConcurrentMultiStageQueries() throws Exception {
    List<String> queries =
        List.of(
            // Join query
            String.format(
                "source=%s | inner join left=a, right=b ON a.dept = b.dept %s"
                    + " | stats count() by b.dept_head",
                SG_TEST_INDEX, DEPT_INDEX),
            // High-cardinality aggregation
            String.format("source=%s | stats count() by name", SG_TEST_INDEX),
            // Sort + limit
            String.format("source=%s | sort - salary | head 50 | fields id, salary", SG_TEST_INDEX),
            // Filter + aggregation
            String.format("source=%s | where age > 30 | stats avg(salary) by dept", SG_TEST_INDEX),
            // Orders aggregation
            String.format("source=%s | stats sum(amount), count() by dept", ORDERS_INDEX));

    runConcurrentQueries(queries);
  }

  @Test
  @DisplayName("5 concurrent join queries all succeed")
  public void testConcurrentJoinQueries() throws Exception {
    List<String> queries = new ArrayList<>();
    String[] aggFuncs = {"count()", "sum(a.salary)", "avg(a.salary)", "min(a.age)", "max(a.age)"};
    for (int i = 0; i < CONCURRENT_QUERIES; i++) {
      queries.add(
          String.format(
              "source=%s | inner join left=a, right=b ON a.dept = b.dept %s | stats %s",
              SG_TEST_INDEX, DEPT_INDEX, aggFuncs[i]));
    }
    runConcurrentQueries(queries);
  }

  @Test
  @DisplayName("5 concurrent aggregation queries with different groupings")
  public void testConcurrentAggQueries() throws Exception {
    List<String> queries =
        List.of(
            String.format("source=%s | stats count() by city", SG_TEST_INDEX),
            String.format("source=%s | stats count() by dept", SG_TEST_INDEX),
            String.format("source=%s | stats count() by name", SG_TEST_INDEX),
            String.format("source=%s | stats sum(salary) by city", SG_TEST_INDEX),
            String.format("source=%s | stats avg(age), sum(salary) by dept", SG_TEST_INDEX));
    runConcurrentQueries(queries);
  }

  @Test
  @DisplayName("Concurrent queries produce correct results")
  public void testConcurrentCorrectnessVerification() throws Exception {
    String query =
        String.format(
            "source=%s | inner join left=a, right=b ON a.dept = b.dept %s"
                + " | stats count() by b.dept_head",
            SG_TEST_INDEX, DEPT_INDEX);

    // Get expected result (sequential)
    JSONObject expected = executeQuery(query);

    // Run same query 5 times concurrently
    ExecutorService executor = Executors.newFixedThreadPool(CONCURRENT_QUERIES);
    try {
      List<Future<JSONObject>> futures = new ArrayList<>();
      for (int i = 0; i < CONCURRENT_QUERIES; i++) {
        futures.add(executor.submit(() -> executeQuery(query)));
      }

      for (int i = 0; i < futures.size(); i++) {
        JSONObject result = futures.get(i).get();
        assertResultsMatch(expected, result);
      }
    } finally {
      executor.shutdown();
    }
  }

  @Test
  @DisplayName("No deadlock under heavy concurrent multi-stage load")
  public void testNoDeadlockUnderLoad() throws Exception {
    // 10 concurrent queries mixing joins, aggs, and sorts
    List<String> queries = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      switch (i % 3) {
        case 0:
          queries.add(
              String.format(
                  "source=%s | inner join left=a, right=b ON a.dept = b.dept %s"
                      + " | stats count()",
                  SG_TEST_INDEX, DEPT_INDEX));
          break;
        case 1:
          queries.add(
              String.format("source=%s | stats count(), avg(salary) by city", SG_TEST_INDEX));
          break;
        case 2:
          queries.add(
              String.format(
                  "source=%s | sort - salary | head 100 | fields id, salary", SG_TEST_INDEX));
          break;
      }
    }
    runConcurrentQueries(queries);
  }

  /** Runs all queries concurrently and asserts none fail. */
  private void runConcurrentQueries(List<String> queries) throws Exception {
    ExecutorService executor = Executors.newFixedThreadPool(queries.size());
    try {
      List<Future<JSONObject>> futures = new ArrayList<>();
      for (String query : queries) {
        futures.add(executor.submit((Callable<JSONObject>) () -> executeQuery(query)));
      }

      List<String> errors = new ArrayList<>();
      for (int i = 0; i < futures.size(); i++) {
        try {
          JSONObject result = futures.get(i).get();
          Assert.assertNotNull("Query " + i + " returned null", result);
          Assert.assertTrue("Query " + i + " returned no datarows", result.has("datarows"));
        } catch (ExecutionException e) {
          errors.add("Query " + i + " failed: " + e.getCause().getMessage());
        }
      }

      if (!errors.isEmpty()) {
        Assert.fail("Concurrent queries failed:\n" + String.join("\n", errors));
      }
    } finally {
      executor.shutdown();
    }
  }
}
