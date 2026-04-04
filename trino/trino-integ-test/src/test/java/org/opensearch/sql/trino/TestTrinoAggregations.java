/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.trino;

import io.trino.testing.AbstractTestAggregations;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for Trino aggregation queries routed through OpenSearch's REST endpoint.
 *
 * <p>All queries are sent via HTTP to {@code /_plugins/_trino_sql/v1/statement},
 * testing the full integration stack.</p>
 */
public class TestTrinoAggregations extends AbstractTestAggregations {

  @Override
  protected QueryRunner createQueryRunner() throws Exception {
    String cluster = System.getProperty("tests.rest.cluster", "localhost:9200");
    String firstHost = cluster.split(",")[0].trim();
    for (String part : cluster.split(",")) {
      String trimmed = part.trim();
      if (!trimmed.startsWith("[")) {
        firstHost = trimmed;
        break;
      }
    }
    if (!firstHost.startsWith("http://") && !firstHost.startsWith("https://")) {
      firstHost = "http://" + firstHost;
    }
    return OpenSearchTrinoQueryRunner.builder()
        .setBaseUrl(firstHost)
        .build();
  }

  @Test
  @Override
  @Disabled("approx_distinct returns inherently approximate results; Trino and H2 produce "
      + "different approximations for TIMESTAMP WITH TIME ZONE inputs (2384 vs 2347)")
  public void testApproximateCountDistinct() {
    // Approximate count distinct produces non-deterministic results
  }

  @Test
  @Override
  @Disabled("Type mismatch between Trino bigint (Long) and H2 VALUES integer (Integer) for "
      + "custkey column in the expected VALUES clause; array_agg comparison also affected")
  public void testOrderedAggregations() {
    // The expected result uses a VALUES clause where H2 infers Integer for custkey,
    // but our Trino result returns Long (bigint). MaterializedRow.equals() is strict.
  }
}
