/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.Test;
import org.junit.jupiter.api.DisplayName;

/**
 * IC-2.4: Scatter-gather combined filter + aggregation test. Verifies that {@code source=test |
 * where age > 25 | stats avg(salary) by dept} correctly filters then aggregates across shards.
 */
public class ScatterGatherFilterAggIT extends ScatterGatherITBase {

  @Test
  @DisplayName("Filter then aggregate: where age > 25, avg(salary) by dept")
  public void testFilterThenAgg() throws IOException {
    JSONObject result = sgQuery("where age > 25 | stats avg(salary) by dept");
    // 5 departments
    int rowCount = countRows(result);
    assertEquals(5, rowCount);
    assertEngineUsed("distributed", result);
  }

  @Test
  @DisplayName("Filter+agg matches DSL path")
  public void testFilterAggMatchesDSL() throws IOException {
    JSONObject[] results = dualPathQuery("where age > 25 | stats avg(salary) by dept");
    assertResultsMatch(results[0], results[1]);
  }

  @Test
  @DisplayName("Filter then count by city")
  public void testFilterCountByCity() throws IOException {
    JSONObject[] results = dualPathQuery("where status = 200 | stats count() by city");
    assertResultsMatch(results[0], results[1]);
  }

  @Test
  @DisplayName("Filter then sum(salary) by dept")
  public void testFilterSumByDept() throws IOException {
    JSONObject[] results =
        dualPathQuery("where age >= 30 AND age < 40 | stats sum(salary) by dept");
    assertResultsMatch(results[0], results[1]);
  }

  @Test
  @DisplayName("Narrow filter then aggregate (few matching rows)")
  public void testNarrowFilterThenAgg() throws IOException {
    JSONObject[] results =
        dualPathQuery("where city = 'Seattle' AND status = 404 | stats count() by dept");
    assertResultsMatch(results[0], results[1]);
  }

  @Test
  @DisplayName("Filter eliminating all rows returns empty aggregation")
  public void testFilterEliminatesAll() throws IOException {
    JSONObject result = sgQuery("where status = 999 | stats count() by dept");
    assertEquals(0, countRows(result));
  }
}
