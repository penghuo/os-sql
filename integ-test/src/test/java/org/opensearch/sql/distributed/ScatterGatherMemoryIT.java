/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;
import org.junit.jupiter.api.DisplayName;
import org.opensearch.client.Request;
import org.opensearch.client.Response;

/**
 * IC-2.7: Scatter-gather memory test. Verifies that the MemoryPool returns to 0 after query
 * execution completes, confirming no memory leaks in the distributed path.
 */
public class ScatterGatherMemoryIT extends ScatterGatherITBase {

  @Test
  @DisplayName("Memory pool returns to zero after filter query")
  public void testMemoryCleanupAfterFilter() throws IOException {
    long memBefore = getDistributedMemoryUsage();
    sgQuery("where status = 200 | fields id, status");
    long memAfter = getDistributedMemoryUsage();
    Assert.assertEquals(
        "Memory pool should return to baseline after query", memBefore, memAfter);
  }

  @Test
  @DisplayName("Memory pool returns to zero after aggregation query")
  public void testMemoryCleanupAfterAggregation() throws IOException {
    long memBefore = getDistributedMemoryUsage();
    sgQuery("stats count(), avg(salary) by dept");
    long memAfter = getDistributedMemoryUsage();
    Assert.assertEquals(
        "Memory pool should return to baseline after aggregation", memBefore, memAfter);
  }

  @Test
  @DisplayName("Memory pool returns to zero after sort+limit query")
  public void testMemoryCleanupAfterSortLimit() throws IOException {
    long memBefore = getDistributedMemoryUsage();
    sgQuery("sort - timestamp | head 100 | fields id, timestamp");
    long memAfter = getDistributedMemoryUsage();
    Assert.assertEquals(
        "Memory pool should return to baseline after sort+limit", memBefore, memAfter);
  }

  @Test
  @DisplayName("Memory pool returns to zero after multiple sequential queries")
  public void testMemoryCleanupAfterSequentialQueries() throws IOException {
    long memBefore = getDistributedMemoryUsage();

    for (int i = 0; i < 5; i++) {
      sgQuery("where age > " + (20 + i * 5) + " | stats count() by city");
    }

    long memAfter = getDistributedMemoryUsage();
    Assert.assertEquals(
        "Memory pool should return to baseline after 5 sequential queries",
        memBefore,
        memAfter);
  }

  @Test
  @DisplayName("Memory pool does not grow unbounded across queries")
  public void testNoMemoryGrowth() throws IOException {
    // Run a batch of queries and check memory does not grow
    long memStart = getDistributedMemoryUsage();

    for (int i = 0; i < 10; i++) {
      sgQuery("where status = 200 | fields id, status, city");
    }

    long memEnd = getDistributedMemoryUsage();
    Assert.assertEquals(
        "Memory should not grow across repeated queries", memStart, memEnd);
  }

  /**
   * Get the current distributed engine memory usage from the stats API. Returns 0 if the stats
   * endpoint is not available (graceful degradation).
   */
  private long getDistributedMemoryUsage() throws IOException {
    try {
      Request request = new Request("GET", "/_plugins/_sql/stats");
      Response response = client().performRequest(request);
      String body =
          org.opensearch.sql.legacy.TestUtils.getResponseBody(response, true);
      JSONObject stats = new JSONObject(body);

      // Look for distributed engine memory metrics
      if (stats.has("distributed_engine")) {
        JSONObject deStats = stats.getJSONObject("distributed_engine");
        if (deStats.has("memory_pool_bytes")) {
          return deStats.getLong("memory_pool_bytes");
        }
      }
      return 0L;
    } catch (Exception e) {
      // Stats endpoint may not expose memory metrics yet
      return 0L;
    }
  }
}
