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

/**
 * IC-2.3: Scatter-gather sort and limit test. Verifies that {@code source=test | sort - timestamp |
 * head 100} correctly performs a global sort across all 5 shards and returns the top 100.
 */
public class ScatterGatherSortLimitIT extends ScatterGatherITBase {

  @Test
  @DisplayName("Sort descending by timestamp, head 100")
  public void testSortDescTimestampHead100() throws IOException {
    JSONObject result = sgQuery("sort - timestamp | head 100");
    assertEquals(100, countRows(result));
    assertEngineUsed("distributed", result);
  }

  @Test
  @DisplayName("Sort+limit matches DSL path exactly (order matters)")
  public void testSortLimitMatchesDSL() throws IOException {
    JSONObject[] results = dualPathQuery("sort - timestamp | head 100 | fields id, timestamp");
    assertResultsMatchOrdered(results[0], results[1]);
  }

  @Test
  @DisplayName("Sort ascending by age, head 50")
  public void testSortAscAgeHead50() throws IOException {
    JSONObject[] results = dualPathQuery("sort age | head 50 | fields id, age");
    assertResultsMatchOrdered(results[0], results[1]);
  }

  @Test
  @DisplayName("Sort by multiple columns")
  public void testSortMultiColumn() throws IOException {
    JSONObject[] results = dualPathQuery("sort city, - salary | head 25 | fields city, salary");
    assertResultsMatchOrdered(results[0], results[1]);
  }

  @Test
  @DisplayName("Sort result ordering is correct (descending)")
  public void testSortOrderingDescending() throws IOException {
    JSONObject result = sgQuery("sort - id | head 10 | fields id");
    JSONArray datarows = result.getJSONArray("datarows");
    for (int i = 1; i < datarows.length(); i++) {
      long prev = datarows.getJSONArray(i - 1).getLong(0);
      long curr = datarows.getJSONArray(i).getLong(0);
      Assert.assertTrue(
          "Expected descending order, got " + prev + " before " + curr,
          prev >= curr);
    }
  }

  @Test
  @DisplayName("Sort result ordering is correct (ascending)")
  public void testSortOrderingAscending() throws IOException {
    JSONObject result = sgQuery("sort id | head 10 | fields id");
    JSONArray datarows = result.getJSONArray("datarows");
    for (int i = 1; i < datarows.length(); i++) {
      long prev = datarows.getJSONArray(i - 1).getLong(0);
      long curr = datarows.getJSONArray(i).getLong(0);
      Assert.assertTrue(
          "Expected ascending order, got " + prev + " before " + curr,
          prev <= curr);
    }
  }

  @Test
  @DisplayName("Head without sort")
  public void testHeadWithoutSort() throws IOException {
    JSONObject result = sgQuery("head 10");
    assertEquals(10, countRows(result));
  }
}
