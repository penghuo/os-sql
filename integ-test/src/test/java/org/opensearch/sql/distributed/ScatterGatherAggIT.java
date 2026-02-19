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
 * IC-2.2: Scatter-gather aggregation test. Verifies that {@code source=test | stats count() by
 * city} executes correctly across 5 shards.
 */
public class ScatterGatherAggIT extends ScatterGatherITBase {

  @Test
  @DisplayName("Count by city produces correct groups and counts")
  public void testCountByCity() throws IOException {
    JSONObject result = sgQuery("stats count() by city");
    // 5 cities, each appears 200 times (1000/5)
    int rowCount = countRows(result);
    assertEquals(5, rowCount);
    assertEngineUsed("distributed", result);
  }

  @Test
  @DisplayName("Count by city matches DSL path")
  public void testCountByCityMatchesDSL() throws IOException {
    JSONObject[] results = dualPathQuery("stats count() by city");
    assertResultsMatch(results[0], results[1]);
  }

  @Test
  @DisplayName("Sum aggregation across shards")
  public void testSumByShard() throws IOException {
    JSONObject[] results = dualPathQuery("stats sum(salary) by dept");
    assertResultsMatch(results[0], results[1]);
  }

  @Test
  @DisplayName("Average aggregation across shards")
  public void testAvgAcrossShards() throws IOException {
    JSONObject[] results = dualPathQuery("stats avg(age) by dept");
    assertResultsMatch(results[0], results[1]);
  }

  @Test
  @DisplayName("Min/Max aggregation across shards")
  public void testMinMaxAcrossShards() throws IOException {
    JSONObject[] results = dualPathQuery("stats min(age), max(age) by city");
    assertResultsMatch(results[0], results[1]);
  }

  @Test
  @DisplayName("Count without group-by (global aggregation)")
  public void testGlobalCount() throws IOException {
    JSONObject result = sgQuery("stats count()");
    assertEquals(1, countRows(result));
    // Total should be 1000
    JSONObject[] results = dualPathQuery("stats count()");
    assertResultsMatch(results[0], results[1]);
  }

  @Test
  @DisplayName("Multiple aggregation functions in one query")
  public void testMultipleAggFunctions() throws IOException {
    JSONObject[] results = dualPathQuery("stats count(), avg(salary), min(age), max(age) by dept");
    assertResultsMatch(results[0], results[1]);
  }
}
