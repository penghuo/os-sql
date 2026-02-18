/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.jupiter.api.DisplayName;

/**
 * IC-2.9: Feature flag OFF test. Disables the distributed engine feature flag and verifies that all
 * queries fall back to the DSL path, producing the same results.
 */
public class ScatterGatherFeatureFlagOffIT extends ScatterGatherITBase {

  @Override
  protected void init() throws Exception {
    super.init();
    // Keep distributed engine disabled for these tests
    disableDistributedEngine();
  }

  @After
  public void restoreDistributedEngine() throws IOException {
    // Restore distributed engine for other test classes
    enableDistributedEngine();
  }

  @Test
  @DisplayName("Filter query works with flag off (DSL fallback)")
  public void testFilterWithFlagOff() throws IOException {
    JSONObject result =
        sgQuery("where status = 200 | fields id, status");
    int rowCount = countRows(result);
    assertEquals(400, rowCount);
    // Should NOT use distributed engine
    assertEngineNotDistributed(result);
  }

  @Test
  @DisplayName("Aggregation query works with flag off (DSL fallback)")
  public void testAggWithFlagOff() throws IOException {
    JSONObject result = sgQuery("stats count() by city");
    assertEquals(5, countRows(result));
    assertEngineNotDistributed(result);
  }

  @Test
  @DisplayName("Sort+limit works with flag off (DSL fallback)")
  public void testSortLimitWithFlagOff() throws IOException {
    JSONObject result = sgQuery("sort - id | head 10 | fields id");
    assertEquals(10, countRows(result));
    assertEngineNotDistributed(result);
  }

  @Test
  @DisplayName("Flag off results match flag on results for filter")
  public void testFlagOffMatchesFlagOnFilter() throws IOException {
    JSONObject dslResult =
        sgQuery("where status = 200 | fields id, status");

    enableDistributedEngine();
    JSONObject distResult;
    try {
      distResult = sgQuery("where status = 200 | fields id, status");
    } finally {
      disableDistributedEngine();
    }

    assertResultsMatch(dslResult, distResult);
  }

  @Test
  @DisplayName("Flag off results match flag on results for aggregation")
  public void testFlagOffMatchesFlagOnAgg() throws IOException {
    JSONObject dslResult = sgQuery("stats count(), avg(salary) by dept");

    enableDistributedEngine();
    JSONObject distResult;
    try {
      distResult = sgQuery("stats count(), avg(salary) by dept");
    } finally {
      disableDistributedEngine();
    }

    assertResultsMatch(dslResult, distResult);
  }

  @Test
  @DisplayName("Flag off results match flag on results for sort+limit")
  public void testFlagOffMatchesFlagOnSort() throws IOException {
    JSONObject dslResult = sgQuery("sort - id | head 20 | fields id");

    enableDistributedEngine();
    JSONObject distResult;
    try {
      distResult = sgQuery("sort - id | head 20 | fields id");
    } finally {
      disableDistributedEngine();
    }

    assertResultsMatchOrdered(dslResult, distResult);
  }

  @Test
  @DisplayName("Toggling flag on and off preserves correctness")
  public void testFlagToggle() throws IOException {
    // Off
    JSONObject result1 = sgQuery("stats count() by city");

    // On
    enableDistributedEngine();
    JSONObject result2;
    try {
      result2 = sgQuery("stats count() by city");
    } finally {
      disableDistributedEngine();
    }

    // Off again
    JSONObject result3 = sgQuery("stats count() by city");

    assertResultsMatch(result1, result2);
    assertResultsMatch(result2, result3);
  }

  /** Assert the engine tag is NOT "distributed". */
  private void assertEngineNotDistributed(JSONObject response) {
    if (response.has("engine")) {
      Assert.assertNotEquals(
          "Expected non-distributed engine when flag is off",
          "distributed",
          response.getString("engine"));
    }
  }
}
