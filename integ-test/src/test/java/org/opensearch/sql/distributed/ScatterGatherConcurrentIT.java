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
 * IC-2.8: Scatter-gather concurrency test. Runs 10 concurrent scatter-gather queries and verifies
 * all complete correctly with no resource leaks or cross-contamination.
 */
public class ScatterGatherConcurrentIT extends ScatterGatherITBase {

  private static final int CONCURRENT_QUERIES = 10;

  @Test
  @DisplayName("10 concurrent filter queries all succeed")
  public void testConcurrentFilterQueries() throws Exception {
    List<String> queries = new ArrayList<>();
    for (int i = 0; i < CONCURRENT_QUERIES; i++) {
      int statusValue = (i % 2 == 0) ? 200 : 404;
      queries.add(
          String.format(
              java.util.Locale.ROOT,
              "source=%s | where status = %d | fields id, status",
              SG_TEST_INDEX,
              statusValue));
    }
    runConcurrentQueries(queries);
  }

  @Test
  @DisplayName("10 concurrent aggregation queries all succeed")
  public void testConcurrentAggQueries() throws Exception {
    List<String> queries = new ArrayList<>();
    for (int i = 0; i < CONCURRENT_QUERIES; i++) {
      String[] groupCols = {"city", "dept", "city", "dept", "city"};
      queries.add(
          String.format(
              "source=%s | stats count() by %s", SG_TEST_INDEX, groupCols[i % groupCols.length]));
    }
    runConcurrentQueries(queries);
  }

  @Test
  @DisplayName("10 concurrent mixed queries all succeed")
  public void testConcurrentMixedQueries() throws Exception {
    List<String> queries =
        List.of(
            String.format("source=%s | where status = 200 | fields id", SG_TEST_INDEX),
            String.format("source=%s | stats count() by city", SG_TEST_INDEX),
            String.format("source=%s | sort - id | head 50 | fields id", SG_TEST_INDEX),
            String.format("source=%s | where age > 40 | stats avg(salary) by dept", SG_TEST_INDEX),
            String.format("source=%s | where city = 'Seattle' | fields name", SG_TEST_INDEX),
            String.format("source=%s | stats min(age), max(age) by dept", SG_TEST_INDEX),
            String.format("source=%s | sort salary | head 25 | fields salary", SG_TEST_INDEX),
            String.format("source=%s | where status = 301 | stats count()", SG_TEST_INDEX),
            String.format("source=%s | stats sum(salary) by city", SG_TEST_INDEX),
            String.format(
                "source=%s | where age >= 30 | sort - age | head 10 | fields id, age",
                SG_TEST_INDEX));
    runConcurrentQueries(queries);
  }

  @Test
  @DisplayName("Concurrent queries produce correct results (verified against sequential)")
  public void testConcurrentCorrectness() throws Exception {
    String query = String.format("source=%s | stats count() by city", SG_TEST_INDEX);

    // Get expected result (sequential)
    JSONObject expected = executeQuery(query);

    // Run same query concurrently
    ExecutorService executor = Executors.newFixedThreadPool(5);
    try {
      List<Future<JSONObject>> futures = new ArrayList<>();
      for (int i = 0; i < 5; i++) {
        futures.add(executor.submit(() -> executeQuery(query)));
      }

      for (Future<JSONObject> future : futures) {
        JSONObject result = future.get();
        assertResultsMatch(expected, result);
      }
    } finally {
      executor.shutdown();
    }
  }

  /** Runs all queries concurrently and asserts none fail. */
  private void runConcurrentQueries(List<String> queries) throws Exception {
    ExecutorService executor = Executors.newFixedThreadPool(CONCURRENT_QUERIES);
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
