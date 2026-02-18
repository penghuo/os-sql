/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed;

import static org.opensearch.sql.legacy.TestUtils.isIndexExist;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import java.util.Locale;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Assert;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.Response;
import org.opensearch.sql.ppl.PPLIntegTestCase;

/**
 * Base class for IC-2 scatter-gather integration tests. Sets up a 5-shard test index with known
 * data and enables the distributed engine for all tests.
 *
 * <p>Test data: 1000 documents with fields: id (long), status (int), city (keyword), age (int),
 * salary (double), dept (keyword), timestamp (date), name (keyword).
 */
public abstract class ScatterGatherITBase extends PPLIntegTestCase {

  protected static final String SG_TEST_INDEX = "scatter_gather_test";
  protected static final int SHARD_COUNT = 5;
  protected static final int DOC_COUNT = 1000;

  @Override
  protected void init() throws Exception {
    super.init();
    enableCalcite();
    enableDistributedEngine();
    enableStrictMode();
    ensureTestIndex();
  }

  /** Creates the 5-shard test index with known data if it does not already exist. */
  private void ensureTestIndex() throws IOException {
    if (isIndexExist(client(), SG_TEST_INDEX)) {
      return;
    }

    // Create index with 5 shards
    String settings =
        String.format(
            Locale.ROOT,
            "{"
                + "\"settings\": {"
                + "  \"number_of_shards\": %d,"
                + "  \"number_of_replicas\": 0"
                + "},"
                + "\"mappings\": {"
                + "  \"properties\": {"
                + "    \"id\": {\"type\": \"long\"},"
                + "    \"status\": {\"type\": \"integer\"},"
                + "    \"city\": {\"type\": \"keyword\"},"
                + "    \"age\": {\"type\": \"integer\"},"
                + "    \"salary\": {\"type\": \"double\"},"
                + "    \"dept\": {\"type\": \"keyword\"},"
                + "    \"timestamp\": {\"type\": \"date\"},"
                + "    \"name\": {\"type\": \"keyword\"}"
                + "  }"
                + "}"
                + "}",
            SHARD_COUNT);

    Request createRequest = new Request("PUT", "/" + SG_TEST_INDEX);
    createRequest.setJsonEntity(settings);
    client().performRequest(createRequest);

    // Bulk-load test documents
    bulkLoadTestData();

    // Wait for all shards to be active
    Request refreshRequest = new Request("POST", "/" + SG_TEST_INDEX + "/_refresh");
    client().performRequest(refreshRequest);
  }

  /** Loads DOC_COUNT documents with deterministic data for reproducible assertions. */
  private void bulkLoadTestData() throws IOException {
    String[] cities = {"Seattle", "Portland", "SanFrancisco", "LosAngeles", "NewYork"};
    String[] depts = {"Engineering", "Sales", "Marketing", "Support", "HR"};
    int[] statuses = {200, 301, 404, 500, 200};
    long baseTimestamp = 1700000000000L; // ~Nov 2023

    StringBuilder bulk = new StringBuilder();
    for (int i = 0; i < DOC_COUNT; i++) {
      bulk.append(
          String.format(Locale.ROOT, "{\"index\":{\"_id\":\"%d\"}}\n", i));
      bulk.append(
          String.format(
              Locale.ROOT,
              "{\"id\":%d,\"status\":%d,\"city\":\"%s\",\"age\":%d,"
                  + "\"salary\":%.2f,\"dept\":\"%s\",\"timestamp\":%d,\"name\":\"user%d\"}\n",
              i,
              statuses[i % statuses.length],
              cities[i % cities.length],
              20 + (i % 40), // ages 20-59
              30000.0 + (i * 50.0), // salaries 30000-79950
              depts[i % depts.length],
              baseTimestamp + (i * 60000L), // 1 minute apart
              i));
    }

    Request bulkRequest = new Request("POST", "/" + SG_TEST_INDEX + "/_bulk");
    bulkRequest.setJsonEntity(bulk.toString());
    RequestOptions.Builder options = RequestOptions.DEFAULT.toBuilder();
    options.addHeader("Content-Type", "application/x-ndjson");
    bulkRequest.setOptions(options);
    Response response = client().performRequest(bulkRequest);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
  }

  /** Execute a PPL query against the scatter-gather test index. */
  protected JSONObject sgQuery(String pplSuffix) throws IOException {
    return executeQuery(String.format("source=%s | %s", SG_TEST_INDEX, pplSuffix));
  }

  /** Execute a full PPL query string (when 'source=' is already included). */
  protected JSONObject sgQueryRaw(String ppl) throws IOException {
    return executeQuery(ppl);
  }

  /** Execute the same query via both distributed and DSL paths, return both results. */
  protected JSONObject[] dualPathQuery(String pplSuffix) throws IOException {
    String query = String.format("source=%s | %s", SG_TEST_INDEX, pplSuffix);

    // Distributed path (already enabled)
    JSONObject distributed = executeQuery(query);

    // DSL path
    disableDistributedEngine();
    try {
      JSONObject dsl = executeQuery(query);
      return new JSONObject[] {distributed, dsl};
    } finally {
      enableDistributedEngine();
    }
  }

  /** Count total rows in a response. */
  protected int countRows(JSONObject response) {
    return response.getJSONArray("datarows").length();
  }

  /** Extract all values from a single column as a JSONArray. */
  protected JSONArray extractColumn(JSONObject response, int colIndex) {
    JSONArray datarows = response.getJSONArray("datarows");
    JSONArray column = new JSONArray();
    for (int i = 0; i < datarows.length(); i++) {
      column.put(datarows.getJSONArray(i).get(colIndex));
    }
    return column;
  }

  /** Assert that two query results have identical data rows (order-insensitive). */
  protected void assertResultsMatch(JSONObject expected, JSONObject actual) {
    JSONArray expectedRows = expected.getJSONArray("datarows");
    JSONArray actualRows = actual.getJSONArray("datarows");

    Assert.assertEquals(
        "Row count mismatch", expectedRows.length(), actualRows.length());

    // Compare sorted string representations for order-insensitive match
    java.util.List<String> expectedSorted = new java.util.ArrayList<>();
    java.util.List<String> actualSorted = new java.util.ArrayList<>();
    for (int i = 0; i < expectedRows.length(); i++) {
      expectedSorted.add(expectedRows.getJSONArray(i).toString());
    }
    for (int i = 0; i < actualRows.length(); i++) {
      actualSorted.add(actualRows.getJSONArray(i).toString());
    }
    java.util.Collections.sort(expectedSorted);
    java.util.Collections.sort(actualSorted);

    Assert.assertEquals("Data rows mismatch", expectedSorted, actualSorted);
  }

  /** Assert that two query results have identical data rows in exact order. */
  protected void assertResultsMatchOrdered(JSONObject expected, JSONObject actual) {
    JSONArray expectedRows = expected.getJSONArray("datarows");
    JSONArray actualRows = actual.getJSONArray("datarows");

    Assert.assertEquals(
        "Row count mismatch", expectedRows.length(), actualRows.length());

    for (int i = 0; i < expectedRows.length(); i++) {
      Assert.assertEquals(
          "Row " + i + " mismatch",
          expectedRows.getJSONArray(i).toString(),
          actualRows.getJSONArray(i).toString());
    }
  }

  /**
   * Verify the scatter-gather test index has documents distributed across multiple shards. This
   * confirms the test is actually exercising multi-shard scatter-gather.
   */
  protected void assertMultiShardDistribution() throws IOException {
    Request request = new Request("GET", "/" + SG_TEST_INDEX + "/_search_shards");
    Response response = client().performRequest(request);
    String body =
        org.opensearch.sql.legacy.TestUtils.getResponseBody(response, true);
    JSONObject shardInfo = new JSONObject(body);
    JSONArray shards = shardInfo.getJSONArray("shards");
    Assert.assertTrue(
        "Expected documents across multiple shards, got " + shards.length(),
        shards.length() >= 2);
  }
}
