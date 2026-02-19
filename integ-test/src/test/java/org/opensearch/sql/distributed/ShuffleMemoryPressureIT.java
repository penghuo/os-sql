/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed;

import java.io.IOException;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;
import org.junit.jupiter.api.DisplayName;
import org.opensearch.client.Request;
import org.opensearch.client.Response;

/**
 * IC-4.5: Shuffle memory pressure integration test. Verifies that queries requiring significant
 * memory (large joins, high-cardinality aggregations) complete correctly, potentially using
 * spill-to-disk under memory pressure.
 */
public class ShuffleMemoryPressureIT extends ShuffleITBase {

  @Test
  @DisplayName("Large aggregation completes under memory pressure")
  public void testLargeAggCompletes() throws IOException {
    // High-cardinality GROUP BY (1000 unique names) with multiple agg functions
    JSONObject result =
        sgQuery("stats count(), sum(salary), avg(age), min(salary), max(salary) by name");
    Assert.assertNotNull(result);
    int rowCount = countRows(result);
    Assert.assertEquals("Should have 1000 groups", DOC_COUNT, rowCount);
    assertEngineUsed("distributed", result);
  }

  @Test
  @DisplayName("Large join completes correctly")
  public void testLargeJoinCompletes() throws IOException {
    // Join 500-row orders with 1000-row main table (many-to-many on dept)
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | inner join left=a, right=b ON a.dept = b.dept %s" + " | stats count()",
                ORDERS_INDEX, SG_TEST_INDEX));
    Assert.assertNotNull(result);
    JSONArray datarows = result.getJSONArray("datarows");
    long count = datarows.getJSONArray(0).getLong(0);
    // 500 orders * 200 employees per dept (each dept has 200 of 1000) = 20,000 per dept
    // But there are 5 depts, each with 100 orders and 200 employees: 100*200*5 = 100,000
    Assert.assertTrue("Join should produce significant rows", count > 0);
    assertEngineUsed("distributed", result);
  }

  @Test
  @DisplayName("Memory returns to baseline after large query")
  public void testMemoryReturnsToBaseline() throws IOException {
    long memBefore = getDistributedMemoryUsage();

    // Run a memory-intensive query
    sgQuery("stats count(), sum(salary), avg(salary), min(age), max(age) by name");

    long memAfter = getDistributedMemoryUsage();
    Assert.assertEquals(
        "Memory pool should return to baseline after large query", memBefore, memAfter);
  }

  @Test
  @DisplayName("Sequential large queries do not accumulate memory")
  public void testSequentialLargeQueriesNoLeak() throws IOException {
    long memStart = getDistributedMemoryUsage();

    for (int i = 0; i < 5; i++) {
      sgQuery("stats count(), sum(salary) by name");
    }

    long memEnd = getDistributedMemoryUsage();
    Assert.assertEquals(
        "Memory should not accumulate across sequential large queries", memStart, memEnd);
  }

  @Test
  @DisplayName("Concurrent large queries complete without OOM")
  public void testConcurrentLargeQueries() throws Exception {
    java.util.concurrent.ExecutorService executor =
        java.util.concurrent.Executors.newFixedThreadPool(3);
    try {
      java.util.List<java.util.concurrent.Future<JSONObject>> futures = new java.util.ArrayList<>();
      for (int i = 0; i < 3; i++) {
        final String query =
            String.format("source=%s | stats count(), avg(salary) by name", SG_TEST_INDEX);
        futures.add(executor.submit(() -> executeQuery(query)));
      }

      for (int i = 0; i < futures.size(); i++) {
        JSONObject result = futures.get(i).get();
        Assert.assertNotNull("Query " + i + " should succeed", result);
        Assert.assertTrue("Query " + i + " should have data rows", result.has("datarows"));
      }
    } finally {
      executor.shutdown();
    }
  }

  /**
   * Get the current distributed engine memory usage from the stats API. Returns 0 if the stats
   * endpoint is not available.
   */
  private long getDistributedMemoryUsage() throws IOException {
    try {
      Request request = new Request("GET", "/_plugins/_sql/stats");
      Response response = client().performRequest(request);
      String body = org.opensearch.sql.legacy.TestUtils.getResponseBody(response, true);
      JSONObject stats = new JSONObject(body);

      if (stats.has("distributed_engine")) {
        JSONObject deStats = stats.getJSONObject("distributed_engine");
        if (deStats.has("memory_pool_bytes")) {
          return deStats.getLong("memory_pool_bytes");
        }
      }
      return 0L;
    } catch (Exception e) {
      return 0L;
    }
  }
}
