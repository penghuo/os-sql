/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.client.Request;
import org.opensearch.sql.ppl.PPLIntegTestCase;

/**
 * Smoke test for the experimental PPL→SqlNode→SqlValidator path against a real OpenSearch cluster.
 * Enables {@code plugins.calcite.sqlnode.enabled} and runs a focused subset of basic PPL queries to
 * surface translation gaps.
 *
 * <p>Tests that fail here are documented gaps in the new path; they fall back to the legacy
 * RelBuilder path under fallback-allowed mode. Without fallback, they will throw and fail this test
 * class — making the surface visible.
 */
public class CalcitePPLSqlNodeBasicIT extends PPLIntegTestCase {

  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();

    Request request1 = new Request("PUT", "/test/_doc/1?refresh=true");
    request1.setJsonEntity("{\"name\": \"hello\", \"age\": 20}");
    client().performRequest(request1);
    Request request2 = new Request("PUT", "/test/_doc/2?refresh=true");
    request2.setJsonEntity("{\"name\": \"world\", \"age\": 30}");
    client().performRequest(request2);

    loadIndex(Index.BANK);
  }

  @Test
  public void source_query_under_sqlnode_path() throws IOException {
    JSONObject actual = executeQuery("source=test");
    verifySchema(actual, schema("name", "string"), schema("age", "bigint"));
    verifyDataRows(actual, rows("hello", 20), rows("world", 30));
  }

  @Test
  public void source_with_fields_under_sqlnode_path() throws IOException {
    JSONObject actual = executeQuery("source=test | fields name");
    verifySchema(actual, schema("name", "string"));
    verifyDataRows(actual, rows("hello"), rows("world"));
  }

  @Test
  public void source_with_where_under_sqlnode_path() throws IOException {
    JSONObject actual = executeQuery("source=test | where age > 25 | fields name");
    verifySchema(actual, schema("name", "string"));
    verifyDataRows(actual, rows("world"));
  }

  @Test
  public void stats_under_sqlnode_path() throws IOException {
    JSONObject actual = executeQuery("source=" + TEST_INDEX_BANK + " | stats count()");
    verifySchema(actual, schema("count()", "bigint"));
  }
}
