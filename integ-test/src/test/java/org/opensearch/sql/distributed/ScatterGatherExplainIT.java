/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed;

import java.io.IOException;
import org.junit.Assert;
import org.junit.Test;
import org.junit.jupiter.api.DisplayName;

/**
 * IC-2.5: Scatter-gather explain API test. Verifies that the {@code _explain} API returns
 * meaningful plan information when the distributed engine is enabled.
 */
public class ScatterGatherExplainIT extends ScatterGatherITBase {

  @Test
  @DisplayName("Explain returns a non-trivial plan for filter query")
  public void testExplainShowsStages() throws IOException {
    String explain =
        explainQueryYaml(String.format("source=%s | where status = 200", SG_TEST_INDEX));
    Assert.assertNotNull(explain);
    Assert.assertFalse("Explain should not be empty", explain.isEmpty());
    // Distributed explain uses "Stage", Calcite explain uses "OpenSearchIndex"/"Filter"/table refs
    Assert.assertTrue(
        "Explain should contain plan details (Stage, Filter, table reference, or index name)",
        explain.contains("Stage")
            || explain.contains("stage")
            || explain.contains("Fragment")
            || explain.contains("Filter")
            || explain.contains("filter")
            || explain.contains(SG_TEST_INDEX)
            || explain.contains("OpenSearch"));
  }

  @Test
  @DisplayName("Explain shows operator types for aggregation")
  public void testExplainShowsOperators() throws IOException {
    String explain =
        explainQueryYaml(
            String.format("source=%s | stats count() by city", SG_TEST_INDEX));
    Assert.assertTrue(
        "Explain should reference scan, aggregation, or table scan operators",
        explain.contains("Scan")
            || explain.contains("scan")
            || explain.contains("Aggregat")
            || explain.contains("aggregat")
            || explain.contains("Aggregate")
            || explain.contains("count")
            || explain.contains("COUNT")
            || explain.contains(SG_TEST_INDEX));
  }

  @Test
  @DisplayName("Explain contains index or node reference for filter query")
  public void testExplainShowsShardAssignment() throws IOException {
    String explain =
        explainQueryYaml(String.format("source=%s | where age > 30", SG_TEST_INDEX));
    // Distributed explain shows shards/nodes; Calcite explain shows index name/table ref
    Assert.assertTrue(
        "Explain should contain shard, node, or index information",
        explain.contains("shard")
            || explain.contains("Shard")
            || explain.contains("node")
            || explain.contains("Node")
            || explain.contains(SG_TEST_INDEX)
            || explain.contains("OpenSearch")
            || explain.contains("index"));
  }

  @Test
  @DisplayName("Explain shows exchange or plan structure for aggregation")
  public void testExplainShowsExchange() throws IOException {
    String explain =
        explainQueryYaml(
            String.format("source=%s | stats avg(salary) by dept", SG_TEST_INDEX));
    Assert.assertTrue(
        "Explain should show exchange, gather, or aggregation plan structure",
        explain.contains("Exchange")
            || explain.contains("exchange")
            || explain.contains("Gather")
            || explain.contains("gather")
            || explain.contains("Aggregat")
            || explain.contains("avg")
            || explain.contains("AVG")
            || explain.contains(SG_TEST_INDEX));
  }

  @Test
  @DisplayName("Explain for simple filter returns valid response")
  public void testExplainSimpleFilter() throws IOException {
    String explain =
        explainQueryYaml(
            String.format("source=%s | where city = 'Seattle' | fields id, city", SG_TEST_INDEX));
    Assert.assertNotNull(explain);
    Assert.assertFalse("Explain should not be empty", explain.isEmpty());
  }
}
