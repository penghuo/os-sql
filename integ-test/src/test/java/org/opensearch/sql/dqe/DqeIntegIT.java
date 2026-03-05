/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe;

import static org.opensearch.sql.legacy.TestUtils.getResponseBody;
import static org.opensearch.sql.legacy.TestUtils.isIndexExist;

import java.io.IOException;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.sql.legacy.SQLIntegTestCase;

/**
 * Integration tests for the DQE (Distributed Query Engine) pipeline exercising the full
 * {@code /_plugins/_trino_sql} REST endpoint on a real OpenSearch cluster.
 *
 * <p>Each test sends a SQL query through the REST layer, which triggers the real transport path:
 * RestTrinoSqlAction -> TransportTrinoSqlAction -> TransportShardExecuteAction ->
 * OpenSearchPageSource, then merges results back to the coordinator and returns JSON.
 */
public class DqeIntegIT extends SQLIntegTestCase {

  private static final String DQE_ENDPOINT = "/_plugins/_trino_sql";
  private static final String DQE_EXPLAIN_ENDPOINT = "/_plugins/_trino_sql/_explain";
  private static final String TEST_INDEX = "dqe_test_logs";

  @Override
  protected void init() throws Exception {
    super.init();

    // 1. Enable DQE (it defaults to false)
    updateClusterSettings(
        new ClusterSetting("persistent", "plugins.dqe.enabled", "true"));

    // 2. Create test index with mapping (2 shards to test distributed execution)
    if (!isIndexExist(client(), TEST_INDEX)) {
      createTestIndex();
      loadTestData();
    }
  }

  private void createTestIndex() throws IOException {
    String mapping =
        "{"
            + "\"settings\": {\"number_of_shards\": 2, \"number_of_replicas\": 0},"
            + "\"mappings\": {\"properties\": {"
            + "  \"category\": {\"type\": \"keyword\"},"
            + "  \"status\": {\"type\": \"long\"},"
            + "  \"message\": {\"type\": \"text\"},"
            + "  \"amount\": {\"type\": \"double\"}"
            + "}}}";

    Request request = new Request("PUT", "/" + TEST_INDEX);
    request.setJsonEntity(mapping);
    client().performRequest(request);
  }

  private void loadTestData() throws IOException {
    StringBuilder bulk = new StringBuilder();
    String[][] data = {
      {"error", "500", "Internal server error", "10.5"},
      {"error", "500", "Database timeout", "20.0"},
      {"warn", "400", "Bad request", "5.0"},
      {"warn", "400", "Missing parameter", "3.0"},
      {"info", "200", "Request completed", "1.0"},
      {"info", "200", "Health check passed", "0.5"},
      {"info", "200", "Cache refreshed", "2.0"},
      {"debug", "200", "Connection pool stats", "0.1"},
      {"error", "503", "Service unavailable", "15.0"},
      {"warn", "429", "Rate limited", "8.0"},
    };

    for (int i = 0; i < data.length; i++) {
      bulk.append("{\"index\": {\"_id\": \"").append(i).append("\"}}\n");
      bulk.append("{\"category\": \"")
          .append(data[i][0])
          .append("\", \"status\": ")
          .append(data[i][1])
          .append(", \"message\": \"")
          .append(data[i][2])
          .append("\", \"amount\": ")
          .append(data[i][3])
          .append("}\n");
    }

    Request request = new Request("POST", "/" + TEST_INDEX + "/_bulk?refresh=wait_for");
    request.setJsonEntity(bulk.toString());
    client().performRequest(request);
  }

  private JSONObject executeDqeQuery(String sql) throws IOException {
    Request request = new Request("POST", DQE_ENDPOINT);
    request.setJsonEntity("{\"query\": \"" + sql + "\"}");
    Response response = client().performRequest(request);
    return new JSONObject(getResponseBody(response));
  }

  private JSONObject executeDqeExplain(String sql) throws IOException {
    Request request = new Request("POST", DQE_EXPLAIN_ENDPOINT);
    request.setJsonEntity("{\"query\": \"" + sql + "\"}");
    Response response = client().performRequest(request);
    return new JSONObject(getResponseBody(response));
  }

  // === TESTS ===

  @Test
  public void simpleSelect() throws IOException {
    JSONObject result =
        executeDqeQuery("SELECT category, status FROM " + TEST_INDEX);

    Assert.assertEquals(200, result.getInt("status"));
    Assert.assertEquals(10, result.getInt("total"));

    JSONArray schema = result.getJSONArray("schema");
    Assert.assertEquals(2, schema.length());

    JSONArray datarows = result.getJSONArray("datarows");
    Assert.assertEquals(10, datarows.length());
  }

  @Test
  public void selectWithWhere() throws IOException {
    JSONObject result =
        executeDqeQuery(
            "SELECT category, status FROM " + TEST_INDEX + " WHERE status = 200");

    Assert.assertEquals(200, result.getInt("status"));
    // 4 documents have status=200 (3 info + 1 debug)
    Assert.assertEquals(4, result.getInt("total"));

    JSONArray datarows = result.getJSONArray("datarows");
    for (int i = 0; i < datarows.length(); i++) {
      JSONArray row = datarows.getJSONArray(i);
      Assert.assertEquals(200L, row.getLong(1));
    }
  }

  @Test
  public void groupByWithCount() throws IOException {
    JSONObject result =
        executeDqeQuery(
            "SELECT category, COUNT(*) FROM " + TEST_INDEX + " GROUP BY category");

    Assert.assertEquals(200, result.getInt("status"));

    JSONArray datarows = result.getJSONArray("datarows");
    Assert.assertTrue(
        "GROUP BY should produce rows", datarows.length() > 0);

    // Verify total count adds up to 10
    long totalCount = 0;
    for (int i = 0; i < datarows.length(); i++) {
      totalCount += datarows.getJSONArray(i).getLong(1);
    }
    Assert.assertEquals(10L, totalCount);
  }

  @Test
  public void orderByWithLimit() throws IOException {
    JSONObject result =
        executeDqeQuery(
            "SELECT category, status FROM "
                + TEST_INDEX
                + " ORDER BY status DESC LIMIT 3");

    Assert.assertEquals(200, result.getInt("status"));
    Assert.assertEquals(3, result.getInt("total"));

    JSONArray datarows = result.getJSONArray("datarows");
    // First row should have highest status
    long prevStatus = Long.MAX_VALUE;
    for (int i = 0; i < datarows.length(); i++) {
      long status = datarows.getJSONArray(i).getLong(1);
      Assert.assertTrue(
          "Rows should be descending by status", status <= prevStatus);
      prevStatus = status;
    }
  }

  @Test
  public void explainQuery() throws IOException {
    JSONObject result =
        executeDqeExplain(
            "SELECT category FROM " + TEST_INDEX + " WHERE status = 200");

    String plan = result.getString("plan");
    // Plan should mention the index name and a TableScanNode
    Assert.assertTrue(
        "Explain plan should contain index name", plan.contains(TEST_INDEX));
    Assert.assertTrue(
        "Explain plan should contain TableScanNode", plan.contains("TableScanNode"));
  }

  @Test
  public void dqeDisabledReturnsError() throws IOException {
    // Disable DQE
    updateClusterSettings(
        new ClusterSetting("persistent", "plugins.dqe.enabled", "false"));

    try {
      Request request = new Request("POST", DQE_ENDPOINT);
      request.setJsonEntity("{\"query\": \"SELECT 1\"}");

      try {
        client().performRequest(request);
        Assert.fail("Expected error when DQE is disabled");
      } catch (ResponseException e) {
        // Expected: DQE is disabled, should return a server error
        int statusCode = e.getResponse().getStatusLine().getStatusCode();
        Assert.assertTrue(
            "Expected 4xx or 5xx status when DQE is disabled, got: " + statusCode,
            statusCode >= 400);
      }
    } finally {
      // Re-enable for other tests
      updateClusterSettings(
          new ClusterSetting("persistent", "plugins.dqe.enabled", "true"));
    }
  }
}
