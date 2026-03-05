/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe;

import java.io.IOException;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.sql.legacy.OpenSearchSQLRestTestCase;

/**
 * Integration tests for the DQE (Distributed Query Engine) Trino SQL endpoint. Tests exercise the
 * full path: REST request -> coordinator -> shard execution -> result merge -> REST response.
 *
 * <p>Test data: 10 documents in {@code dqe_test_logs} with fields category (keyword), status
 * (long), message (text), amount (double).
 */
public class DqeIntegIT extends OpenSearchSQLRestTestCase {

  private static final String INDEX_NAME = "dqe_test_logs";
  private static boolean indexCreated = false;

  @BeforeClass
  public static void setupTestData() throws IOException {
    // Handled in setUp per-instance since BeforeClass can't use instance client()
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
    if (!indexCreated) {
      createTestIndex();
      indexCreated = true;
    }
  }

  @AfterClass
  public static void resetFlag() {
    indexCreated = false;
  }

  @Override
  protected boolean preserveIndicesUponCompletion() {
    return false;
  }

  private void createTestIndex() throws IOException {
    // Create index with settings and mappings
    Request createIndex = new Request("PUT", "/" + INDEX_NAME);
    createIndex.setJsonEntity(
        "{"
            + "\"settings\": {\"number_of_shards\": 2, \"number_of_replicas\": 0},"
            + "\"mappings\": {\"properties\": {"
            + "\"category\": {\"type\": \"keyword\"},"
            + "\"status\": {\"type\": \"long\"},"
            + "\"message\": {\"type\": \"text\"},"
            + "\"amount\": {\"type\": \"double\"}"
            + "}}"
            + "}");
    client().performRequest(createIndex);

    // Bulk index documents
    Request bulk = new Request("POST", "/_bulk?refresh=wait_for");
    StringBuilder sb = new StringBuilder();
    Object[][] docs = {
      {"error", 500, "Internal server error", 10.5},
      {"error", 500, "Database timeout", 20.0},
      {"warn", 400, "Bad request", 5.0},
      {"warn", 400, "Missing parameter", 3.0},
      {"info", 200, "Request completed", 1.0},
      {"info", 200, "Health check passed", 0.5},
      {"info", 200, "Cache refreshed", 2.0},
      {"debug", 200, "Connection pool stats", 0.1},
      {"error", 503, "Service unavailable", 15.0},
      {"warn", 429, "Rate limited", 8.0}
    };
    for (int i = 0; i < docs.length; i++) {
      sb.append("{\"index\": {\"_index\": \"")
          .append(INDEX_NAME)
          .append("\", \"_id\": \"")
          .append(i)
          .append("\"}}\n");
      sb.append("{\"category\": \"")
          .append(docs[i][0])
          .append("\", \"status\": ")
          .append(docs[i][1])
          .append(", \"message\": \"")
          .append(docs[i][2])
          .append("\", \"amount\": ")
          .append(docs[i][3])
          .append("}\n");
    }
    bulk.setJsonEntity(sb.toString());
    client().performRequest(bulk);
  }

  // ---- Tests 1-6: Should pass with current code ----

  @Test
  public void testSimpleSelect() throws IOException {
    JSONObject result = executeTrinoSql("SELECT category, status FROM " + INDEX_NAME);
    JSONArray datarows = result.getJSONArray("datarows");
    assertEquals(10, datarows.length());

    JSONArray schema = result.getJSONArray("schema");
    assertEquals(2, schema.length());
    assertEquals("category", schema.getJSONObject(0).getString("name"));
    assertEquals("status", schema.getJSONObject(1).getString("name"));
  }

  @Test
  public void testWhereEqualityLong() throws IOException {
    JSONObject result =
        executeTrinoSql("SELECT category, status FROM " + INDEX_NAME + " WHERE status = 200");
    JSONArray datarows = result.getJSONArray("datarows");
    assertEquals(4, datarows.length());
    for (int i = 0; i < datarows.length(); i++) {
      assertEquals(200, datarows.getJSONArray(i).getInt(1));
    }
  }

  @Test
  public void testGroupByCount() throws IOException {
    JSONObject result =
        executeTrinoSql("SELECT category, COUNT(*) FROM " + INDEX_NAME + " GROUP BY category");
    JSONArray datarows = result.getJSONArray("datarows");
    assertEquals(4, datarows.length());

    // Verify total count across all groups equals 10
    long totalCount = 0;
    for (int i = 0; i < datarows.length(); i++) {
      totalCount += datarows.getJSONArray(i).getLong(1);
    }
    assertEquals(10, totalCount);
  }

  @Test
  public void testOrderByDescLimit() throws IOException {
    JSONObject result =
        executeTrinoSql(
            "SELECT category, status FROM " + INDEX_NAME + " ORDER BY status DESC LIMIT 3");
    JSONArray datarows = result.getJSONArray("datarows");
    assertEquals(3, datarows.length());
    // First row should have the highest status (503 or 500)
    assertTrue(datarows.getJSONArray(0).getInt(1) >= 500);
  }

  @Test
  public void testWhereWithGroupBy() throws IOException {
    JSONObject result =
        executeTrinoSql(
            "SELECT category, COUNT(*) FROM "
                + INDEX_NAME
                + " WHERE status = 200 GROUP BY category");
    JSONArray datarows = result.getJSONArray("datarows");
    // status=200: info(3) + debug(1) = 2 groups
    assertEquals(2, datarows.length());
  }

  @Test
  public void testExplain() throws IOException {
    JSONObject result = executeTrinoSqlExplain("SELECT category, status FROM " + INDEX_NAME);
    assertTrue(result.has("logical_plan"));
    assertTrue(result.has("optimized_plan"));
    assertTrue(result.has("fragments"));
  }

  // ---- Tests 7-10: Marked @Ignore until expression evaluation is implemented ----

  @Ignore("Requires ExpressionEvaluator: buildPredicate only supports equality on long")
  @Test
  public void testWhereGreaterThan() throws IOException {
    JSONObject result =
        executeTrinoSql("SELECT category, status FROM " + INDEX_NAME + " WHERE status > 200");
    JSONArray datarows = result.getJSONArray("datarows");
    // status > 200: 500, 500, 400, 400, 503, 429 = 6 rows
    assertEquals(6, datarows.length());
    for (int i = 0; i < datarows.length(); i++) {
      assertTrue(datarows.getJSONArray(i).getInt(1) > 200);
    }
  }

  @Ignore("Requires ExpressionEvaluator: buildPredicate only supports equality on long")
  @Test
  public void testWhereAndOr() throws IOException {
    JSONObject result =
        executeTrinoSql(
            "SELECT category, status FROM "
                + INDEX_NAME
                + " WHERE status >= 500 AND category = 'error'");
    JSONArray datarows = result.getJSONArray("datarows");
    // error with status >= 500: error/500, error/500, error/503 = 3 rows
    assertEquals(3, datarows.length());
    for (int i = 0; i < datarows.length(); i++) {
      assertEquals("error", datarows.getJSONArray(i).getString(0));
      assertTrue(datarows.getJSONArray(i).getInt(1) >= 500);
    }
  }

  @Ignore("Requires ExpressionEvaluator: buildPredicate cannot parse string literals")
  @Test
  public void testWhereStringEquality() throws IOException {
    JSONObject result =
        executeTrinoSql("SELECT category, status FROM " + INDEX_NAME + " WHERE category = 'error'");
    JSONArray datarows = result.getJSONArray("datarows");
    // error: 3 rows
    assertEquals(3, datarows.length());
    for (int i = 0; i < datarows.length(); i++) {
      assertEquals("error", datarows.getJSONArray(i).getString(0));
    }
  }

  @Ignore("Requires ExpressionEvaluator: buildPredicate cannot handle arithmetic expressions")
  @Test
  public void testWhereArithmeticInPredicate() throws IOException {
    JSONObject result =
        executeTrinoSql(
            "SELECT category, status FROM " + INDEX_NAME + " WHERE (status + 100) > 600");
    JSONArray datarows = result.getJSONArray("datarows");
    // (status + 100) > 600 means status > 500: error/503 = 1 row
    assertEquals(1, datarows.length());
    assertTrue(datarows.getJSONArray(0).getInt(1) > 500);
  }

  // ---- Helper methods ----

  private JSONObject executeTrinoSql(String sql) throws IOException {
    Request request = new Request("POST", "/_plugins/_trino_sql");
    request.setJsonEntity("{\"query\": \"" + sql.replace("\"", "\\\"") + "\"}");
    Response response = client().performRequest(request);
    assertEquals(RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));
    String body = org.opensearch.sql.legacy.TestUtils.getResponseBody(response);
    return new JSONObject(body);
  }

  private JSONObject executeTrinoSqlExplain(String sql) throws IOException {
    Request request = new Request("POST", "/_plugins/_trino_sql/_explain");
    request.setJsonEntity("{\"query\": \"" + sql.replace("\"", "\\\"") + "\"}");
    Response response = client().performRequest(request);
    assertEquals(RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));
    String body = org.opensearch.sql.legacy.TestUtils.getResponseBody(response);
    return new JSONObject(body);
  }
}
