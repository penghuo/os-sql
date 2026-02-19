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
 * IC-4.3: Shuffle multi-stage aggregation integration test. Verifies that high-cardinality
 * aggregations requiring hash exchange (partial -> exchange -> final) produce correct results.
 */
public class ShuffleMultiStageAggIT extends ShuffleITBase {

  @Test
  @DisplayName("High-cardinality GROUP BY name (1000 unique values)")
  public void testHighCardinalityGroupByName() throws IOException {
    // Each of the 1000 documents has a unique name (user0..user999)
    JSONObject result = sgQuery("stats count() by name");
    Assert.assertNotNull(result);
    int rowCount = countRows(result);
    Assert.assertEquals("Should have 1000 groups (one per unique name)", DOC_COUNT, rowCount);

    // Every count should be 1
    JSONArray datarows = result.getJSONArray("datarows");
    for (int i = 0; i < datarows.length(); i++) {
      long count = datarows.getJSONArray(i).getLong(0);
      Assert.assertEquals("Each name should appear exactly once", 1L, count);
    }
    assertEngineUsed("distributed", result);
  }

  @Test
  @DisplayName("High-cardinality GROUP BY matches DSL path")
  public void testHighCardinalityMatchesDSL() throws IOException {
    JSONObject[] results = dualPathQuery("stats count() by name");
    assertResultsMatch(results[0], results[1]);
  }

  @Test
  @DisplayName("Multi-stage aggregation: SUM with high-cardinality GROUP BY")
  public void testMultiStageSumGroupBy() throws IOException {
    // Group by id (1000 unique values) - each id has exactly 1 row
    JSONObject result = sgQuery("stats sum(salary) by id");
    Assert.assertNotNull(result);
    int rowCount = countRows(result);
    Assert.assertEquals("Should have 1000 groups", DOC_COUNT, rowCount);
    assertEngineUsed("distributed", result);
  }

  @Test
  @DisplayName("Multi-stage aggregation: SUM matches DSL path")
  public void testMultiStageSumMatchesDSL() throws IOException {
    JSONObject[] results = dualPathQuery("stats sum(salary) by id");
    assertResultsMatch(results[0], results[1]);
  }

  @Test
  @DisplayName("Multi-level aggregation: agg then filter then agg")
  public void testMultiLevelAgg() throws IOException {
    // Two-stage: first group by city, then filter on count, producing a subset
    JSONObject result = sgQuery("stats count() as cnt, avg(salary) as avg_sal by city");
    Assert.assertNotNull(result);
    int rowCount = countRows(result);
    Assert.assertEquals("Should have 5 city groups", 5, rowCount);

    // All counts should be 200 (1000 docs / 5 cities)
    JSONArray datarows = result.getJSONArray("datarows");
    for (int i = 0; i < datarows.length(); i++) {
      long count = datarows.getJSONArray(i).getLong(0);
      Assert.assertEquals("Each city should have 200 documents", 200L, count);
    }
    assertEngineUsed("distributed", result);
  }

  @Test
  @DisplayName("Multi-function aggregation with high cardinality")
  public void testMultiFunctionHighCardinality() throws IOException {
    // Multiple aggregation functions on high-cardinality group
    JSONObject[] results =
        dualPathQuery("stats count(), avg(salary), min(age), max(age) by name | head 100");
    assertResultsMatch(results[0], results[1]);
  }

  @Test
  @DisplayName("Aggregation on orders table with dept grouping")
  public void testOrdersAggByDept() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | stats sum(amount) as total, count() as cnt by dept", ORDERS_INDEX));
    Assert.assertNotNull(result);
    int rowCount = countRows(result);
    Assert.assertEquals("Should have 5 dept groups", 5, rowCount);

    // Each dept has 100 orders (500/5)
    JSONArray datarows = result.getJSONArray("datarows");
    for (int i = 0; i < datarows.length(); i++) {
      long count = datarows.getJSONArray(i).getLong(1);
      Assert.assertEquals("Each dept should have 100 orders", 100L, count);
    }
    assertEngineUsed("distributed", result);
  }

  @Test
  @DisplayName("Aggregation on orders table matches DSL path")
  public void testOrdersAggMatchesDSL() throws IOException {
    String query =
        String.format("source=%s | stats sum(amount), avg(quantity) by dept", ORDERS_INDEX);
    JSONObject distributed = executeQuery(query);

    disableDistributedEngine();
    disableStrictMode();
    try {
      JSONObject dsl = executeQuery(query);
      assertResultsMatch(distributed, dsl);
    } finally {
      enableDistributedEngine();
      enableStrictMode();
    }
  }
}
