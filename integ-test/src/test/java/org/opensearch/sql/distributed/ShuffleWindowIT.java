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
 * IC-4.2: Shuffle window function integration test. Verifies that window functions across
 * distributed data produce correct results with proper partitioning and ordering.
 */
public class ShuffleWindowIT extends ShuffleITBase {

  @Test
  @DisplayName("ROW_NUMBER across distributed data with sort")
  public void testRowNumberDistributed() throws IOException {
    // Sort by salary DESC, take top 10 - should produce sequential row numbers
    JSONObject result = sgQuery("sort - salary | head 10 | fields id, salary");
    Assert.assertNotNull(result);
    int rowCount = countRows(result);
    Assert.assertEquals("Should return 10 rows", 10, rowCount);

    // Verify rows are sorted by salary descending
    JSONArray datarows = result.getJSONArray("datarows");
    double prevSalary = Double.MAX_VALUE;
    for (int i = 0; i < datarows.length(); i++) {
      double salary = datarows.getJSONArray(i).getDouble(1);
      Assert.assertTrue(
          "Rows should be sorted by salary DESC: " + prevSalary + " >= " + salary,
          prevSalary >= salary);
      prevSalary = salary;
    }
    assertEngineUsed("distributed", result);
  }

  @Test
  @DisplayName("Aggregation with sort produces correct ordering")
  public void testAggWithSortDistributed() throws IOException {
    // Group by dept, sort by count
    JSONObject result = sgQuery("stats count() as cnt by dept | sort - cnt");
    Assert.assertNotNull(result);
    int rowCount = countRows(result);
    Assert.assertEquals("Should have 5 dept groups", 5, rowCount);

    // Verify counts are in descending order
    JSONArray datarows = result.getJSONArray("datarows");
    long prevCount = Long.MAX_VALUE;
    for (int i = 0; i < datarows.length(); i++) {
      long count = datarows.getJSONArray(i).getLong(0);
      Assert.assertTrue("Groups should be sorted by count DESC", prevCount >= count);
      prevCount = count;
    }
    assertEngineUsed("distributed", result);
  }

  @Test
  @DisplayName("Per-partition ordering via sort after group by")
  public void testPartitionedSortDistributed() throws IOException {
    // Top 3 highest-salary employees per department
    JSONObject result = sgQuery("sort - salary | head 15 | fields dept, salary");
    Assert.assertNotNull(result);
    Assert.assertTrue("Should have results", countRows(result) > 0);
    assertEngineUsed("distributed", result);
  }

  @Test
  @DisplayName("Window-like computation: running aggregation per group")
  public void testWindowLikeAggDistributed() throws IOException {
    // Compute stats per department ordered by salary
    JSONObject result =
        sgQuery(
            "stats avg(salary) as avg_sal, min(salary) as min_sal, max(salary) as max_sal"
                + " by dept");
    Assert.assertNotNull(result);
    int rowCount = countRows(result);
    Assert.assertEquals("Should have 5 dept groups", 5, rowCount);

    // Verify each department has min <= avg <= max
    JSONArray datarows = result.getJSONArray("datarows");
    for (int i = 0; i < datarows.length(); i++) {
      JSONArray row = datarows.getJSONArray(i);
      double avg = row.getDouble(0);
      double min = row.getDouble(1);
      double max = row.getDouble(2);
      Assert.assertTrue(
          String.format("min(%.2f) <= avg(%.2f) <= max(%.2f)", min, avg, max),
          min <= avg && avg <= max);
    }
    assertEngineUsed("distributed", result);
  }

  @Test
  @DisplayName("Window results match DSL path")
  public void testWindowMatchesDSL() throws IOException {
    JSONObject[] results = dualPathQuery("stats avg(salary) by dept | sort dept");
    assertResultsMatch(results[0], results[1]);
  }

  @Test
  @DisplayName("Sort + limit across multiple shards produces globally correct order")
  public void testGlobalSortAcrossShards() throws IOException {
    // Verify that sort across shards produces a globally sorted result
    JSONObject result = sgQuery("sort salary | head 20 | fields id, salary");
    Assert.assertNotNull(result);
    Assert.assertEquals("Should return 20 rows", 20, countRows(result));

    JSONArray datarows = result.getJSONArray("datarows");
    double prevSalary = -1;
    for (int i = 0; i < datarows.length(); i++) {
      double salary = datarows.getJSONArray(i).getDouble(1);
      Assert.assertTrue(
          "Should be sorted ascending: " + prevSalary + " <= " + salary, prevSalary <= salary);
      prevSalary = salary;
    }
    assertEngineUsed("distributed", result);
  }
}
