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
 * IC-2.1: Scatter-gather filter test. Verifies that {@code source=test | where status = 200}
 * executes correctly across 5 shards via the distributed engine.
 */
public class ScatterGatherFilterIT extends ScatterGatherITBase {

  @Test
  @DisplayName("Filter status=200 returns correct count across all shards")
  public void testFilterByStatus() throws IOException {
    JSONObject result = sgQuery("where status = 200");
    // status=200 appears at indices 0,4,5,9,10,... (every 5th starting at 0 and 4)
    // Pattern: i % 5 == 0 -> status[0]=200, i % 5 == 4 -> status[4]=200
    // So 200 docs for i%5==0 and 200 for i%5==4 = 400 total
    int rowCount = countRows(result);
    assertEquals(400, rowCount);
    assertEngineUsed("distributed", result);
  }

  @Test
  @DisplayName("Filter results match DSL path exactly")
  public void testFilterMatchesDSLPath() throws IOException {
    JSONObject[] results = dualPathQuery("where status = 200 | fields id, status");
    assertResultsMatch(results[0], results[1]);
  }

  @Test
  @DisplayName("Filter with range condition")
  public void testFilterRange() throws IOException {
    JSONObject result = sgQuery("where age > 50 | fields id, age");
    // ages are 20 + (i % 40), so age > 50 means i % 40 > 30, i.e., 31..39 = 9 values per cycle
    // 1000/40 = 25 cycles, so 25 * 9 = 225 rows
    int rowCount = countRows(result);
    assertEquals(225, rowCount);
  }

  @Test
  @DisplayName("Filter with string equality")
  public void testFilterByCity() throws IOException {
    JSONObject result = sgQuery("where city = 'Seattle' | fields id, city");
    // city cycles through 5 values, so 1000/5 = 200 rows with city=Seattle
    int rowCount = countRows(result);
    assertEquals(200, rowCount);
  }

  @Test
  @DisplayName("Filter with compound condition")
  public void testFilterCompound() throws IOException {
    JSONObject[] results =
        dualPathQuery("where status = 200 AND city = 'Seattle' | fields id, status, city");
    assertResultsMatch(results[0], results[1]);
  }

  @Test
  @DisplayName("Filter returning zero rows")
  public void testFilterNoResults() throws IOException {
    JSONObject result = sgQuery("where status = 999");
    assertEquals(0, countRows(result));
  }

  @Test
  @DisplayName("Multi-shard distribution is verified")
  public void testIndexHasMultipleShards() throws IOException {
    assertMultiShardDistribution();
  }
}
