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
    String host = System.getProperty("tests.rest.cluster", "http://localhost:9200");
    return OpenSearchTrinoQueryRunner.builder()
        .setBaseUrl(host)
        .build();
  }
}
