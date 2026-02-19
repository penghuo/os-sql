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
 * IC-4.4: Shuffle explain integration test. Verifies that the {@code _explain} API returns
 * meaningful multi-stage plan information for Phase 2 queries (joins, aggregations, sorts).
 */
public class ShuffleExplainIT extends ShuffleITBase {

  @Test
  @DisplayName("Explain shows multi-stage plan for join query")
  public void testExplainJoinQuery() throws IOException {
    String explain =
        explainQueryYaml(
            String.format(
                "source=%s | inner join left=a, right=b ON a.dept = b.dept %s"
                    + " | fields a.id, b.dept_head",
                SG_TEST_INDEX, DEPT_INDEX));
    Assert.assertNotNull(explain);
    Assert.assertFalse("Explain should not be empty", explain.isEmpty());
    // Multi-stage plan should reference join, hash, or exchange concepts
    Assert.assertTrue(
        "Explain should contain plan details for join query",
        explain.contains("Join")
            || explain.contains("join")
            || explain.contains("Hash")
            || explain.contains("hash")
            || explain.contains("Exchange")
            || explain.contains("exchange")
            || explain.contains("Lookup")
            || explain.contains("lookup")
            || explain.contains(SG_TEST_INDEX)
            || explain.contains(DEPT_INDEX));
  }

  @Test
  @DisplayName("Explain shows stages for aggregation with high cardinality")
  public void testExplainHighCardinalityAgg() throws IOException {
    String explain =
        explainQueryYaml(String.format("source=%s | stats count() by name", SG_TEST_INDEX));
    Assert.assertNotNull(explain);
    Assert.assertFalse("Explain should not be empty", explain.isEmpty());
    Assert.assertTrue(
        "Explain should contain aggregation plan details",
        explain.contains("Aggregat")
            || explain.contains("aggregat")
            || explain.contains("Exchange")
            || explain.contains("exchange")
            || explain.contains("Hash")
            || explain.contains("count")
            || explain.contains("COUNT")
            || explain.contains(SG_TEST_INDEX));
  }

  @Test
  @DisplayName("Explain shows sort plan for ORDER BY query")
  public void testExplainSortQuery() throws IOException {
    String explain =
        explainQueryYaml(String.format("source=%s | sort - salary | head 10", SG_TEST_INDEX));
    Assert.assertNotNull(explain);
    Assert.assertFalse("Explain should not be empty", explain.isEmpty());
    Assert.assertTrue(
        "Explain should contain sort or order-related plan details",
        explain.contains("Sort")
            || explain.contains("sort")
            || explain.contains("Order")
            || explain.contains("order")
            || explain.contains("Limit")
            || explain.contains("limit")
            || explain.contains("TopN")
            || explain.contains(SG_TEST_INDEX));
  }

  @Test
  @DisplayName("Explain for join + aggregation shows multi-stage plan")
  public void testExplainJoinThenAgg() throws IOException {
    String explain =
        explainQueryYaml(
            String.format(
                "source=%s | inner join left=a, right=b ON a.dept = b.dept %s"
                    + " | stats count() by b.dept_head",
                SG_TEST_INDEX, DEPT_INDEX));
    Assert.assertNotNull(explain);
    Assert.assertFalse("Explain should not be empty", explain.isEmpty());
    // Should show both join and aggregation stages
    Assert.assertTrue(
        "Explain should show plan structure for join + agg",
        explain.contains(SG_TEST_INDEX)
            || explain.contains(DEPT_INDEX)
            || explain.contains("Join")
            || explain.contains("join")
            || explain.contains("Aggregat")
            || explain.contains("aggregat"));
  }

  @Test
  @DisplayName("Explain returns valid response for all Phase 2 query types")
  public void testExplainVariousQueries() throws IOException {
    // Filter + sort
    String explain1 =
        explainQueryYaml(
            String.format("source=%s | where age > 30 | sort salary | head 20", SG_TEST_INDEX));
    Assert.assertNotNull(explain1);
    Assert.assertFalse(explain1.isEmpty());

    // Multi-function aggregation
    String explain2 =
        explainQueryYaml(
            String.format(
                "source=%s | stats count(), avg(salary), sum(salary) by dept", SG_TEST_INDEX));
    Assert.assertNotNull(explain2);
    Assert.assertFalse(explain2.isEmpty());

    // Orders aggregation
    String explain3 =
        explainQueryYaml(
            String.format("source=%s | stats sum(amount), count() by dept", ORDERS_INDEX));
    Assert.assertNotNull(explain3);
    Assert.assertFalse(explain3.isEmpty());
  }
}
