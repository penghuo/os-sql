/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.trino;

import io.trino.testing.AbstractTestQueries;
import io.trino.testing.QueryRunner;

/**
 * Integration tests for Trino SQL queries routed through OpenSearch's REST endpoint.
 *
 * <p>All queries are sent via HTTP to {@code /_plugins/_trino_sql/v1/statement},
 * testing the full integration stack.</p>
 */
public class TestTrinoQueries extends AbstractTestQueries {

  @Override
  protected QueryRunner createQueryRunner() throws Exception {
    String cluster = System.getProperty("tests.rest.cluster", "localhost:9200");
    // The cluster property may contain multiple comma-separated host:port values.
    // Take the first one and ensure it has an http:// scheme.
    String firstHost = cluster.split(",")[0].trim();
    // Prefer the IPv4 address if the cluster URL contains both IPv6 and IPv4
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
}
