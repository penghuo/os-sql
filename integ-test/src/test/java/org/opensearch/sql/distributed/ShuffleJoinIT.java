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
 * IC-4.1: Shuffle join integration test. Verifies that hash join across two indices executes
 * correctly via the distributed engine with hash exchange.
 */
public class ShuffleJoinIT extends ShuffleITBase {

  @Test
  @DisplayName("Inner join between main index and dept dimension table")
  public void testInnerJoinWithDeptTable() throws IOException {
    // Join scatter_gather_test with shuffle_dept on dept field
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | inner join left=a, right=b ON a.dept = b.dept %s"
                    + " | stats count() by b.dept_head",
                SG_TEST_INDEX, DEPT_INDEX));
    Assert.assertNotNull(result);
    Assert.assertTrue("Join should produce data rows", result.has("datarows"));
    // Each of the 5 departments has 200 employees (1000/5), so 5 groups
    int rowCount = countRows(result);
    Assert.assertEquals("Should have 5 department head groups", 5, rowCount);
    assertEngineUsed("distributed", result);
  }

  @Test
  @DisplayName("Inner join produces correct row count")
  public void testInnerJoinRowCount() throws IOException {
    // Every row in scatter_gather_test has a dept that matches shuffle_dept
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | inner join left=a, right=b ON a.dept = b.dept %s"
                    + " | fields a.id, b.dept_head | head 1000",
                SG_TEST_INDEX, DEPT_INDEX));
    Assert.assertNotNull(result);
    // All 1000 rows should join (every dept has a match)
    int rowCount = countRows(result);
    Assert.assertEquals("All 1000 rows should have a dept match", DOC_COUNT, rowCount);
    assertEngineUsed("distributed", result);
  }

  @Test
  @DisplayName("Left join preserves all probe-side rows")
  public void testLeftJoinPreservesProbeRows() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | left join left=a, right=b ON a.dept = b.dept %s" + " | stats count()",
                SG_TEST_INDEX, DEPT_INDEX));
    Assert.assertNotNull(result);
    JSONArray datarows = result.getJSONArray("datarows");
    long count = datarows.getJSONArray(0).getLong(0);
    Assert.assertEquals("Left join should preserve all 1000 probe rows", DOC_COUNT, count);
    assertEngineUsed("distributed", result);
  }

  @Test
  @DisplayName("Join between orders and dept with aggregation")
  public void testJoinOrdersWithDeptAgg() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | inner join left=a, right=b ON a.dept = b.dept %s"
                    + " | stats sum(amount) by b.dept_head",
                ORDERS_INDEX, DEPT_INDEX));
    Assert.assertNotNull(result);
    Assert.assertTrue("Should have data rows", result.has("datarows"));
    int rowCount = countRows(result);
    Assert.assertEquals("Should have 5 groups by dept_head", 5, rowCount);
    assertEngineUsed("distributed", result);
  }

  @Test
  @DisplayName("Join result matches DSL path")
  public void testJoinMatchesDSL() throws IOException {
    String query =
        String.format(
            "source=%s | inner join left=a, right=b ON a.dept = b.dept %s"
                + " | stats count() by b.dept_head",
            SG_TEST_INDEX, DEPT_INDEX);

    // Distributed path (already enabled)
    JSONObject distributed = executeQuery(query);

    // DSL path
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

  @Test
  @DisplayName("Self-join on same index")
  public void testSelfJoin() throws IOException {
    // Join orders with itself on customer_id to find co-orders
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | inner join left=a, right=b ON a.customer_id = b.customer_id"
                    + " AND a.order_id < b.order_id %s"
                    + " | stats count()",
                ORDERS_INDEX, ORDERS_INDEX));
    Assert.assertNotNull(result);
    Assert.assertTrue("Self-join should produce results", result.has("datarows"));
    assertEngineUsed("distributed", result);
  }
}
