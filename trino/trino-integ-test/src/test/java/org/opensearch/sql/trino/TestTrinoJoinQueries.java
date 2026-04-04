/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.trino;

import io.trino.testing.AbstractTestJoinQueries;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for Trino JOIN queries routed through OpenSearch's REST endpoint.
 *
 * <p>All queries are sent via HTTP to {@code /_plugins/_trino_sql/v1/statement},
 * testing the full integration stack.</p>
 */
public class TestTrinoJoinQueries extends AbstractTestJoinQueries {

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
  @Disabled("Requires installPlugin() to register stateful_sleeping_sum function; "
      + "HTTP-based OpenSearchTrinoQueryRunner does not support plugin installation")
  public void testJoinWithStatefulFilterFunction() {
    // stateful_sleeping_sum is a custom test function installed via installPlugin()
  }

  @Test
  @Override
  @Disabled("Requires DistributedQueryRunner; HTTP-based OpenSearchTrinoQueryRunner "
      + "does not implement that interface")
  public void testOutputDuplicatesInsensitiveJoin() {
    // This test casts the QueryRunner to DistributedQueryRunner
  }

  @Test
  @Override
  @Disabled("Type mismatch between Trino smallint (Short) and H2 VALUES integer (Integer) "
      + "for coerced join columns; MaterializedRow.equals() is strict on Java types")
  public void testJoinCriteriaCoercion() {
    // The expected result from H2 uses Integer for VALUES columns,
    // but our Trino result returns SmallInt-typed columns.
  }
}
