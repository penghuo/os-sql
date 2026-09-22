/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.opensearch.sql.legacy.TestUtils.getResponseBody;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;
import static org.opensearch.sql.plugin.rest.RestPPLQueryAction.ASYNC_JOB_API_ENDPOINT;
import static org.opensearch.sql.plugin.rest.RestPPLQueryAction.QUERY_API_ENDPOINT;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;

/** Integration tests for the PPL asynchronous query lifecycle API. */
public class PPLAsyncQueryIT extends PPLIntegTestCase {

  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
    loadIndex(Index.BANK);
  }

  @Test
  public void asyncQueryReturnsFullFinalSnapshotEquivalentToSync() throws Exception {
    String query =
        "source=" + TEST_INDEX_BANK + " | sort account_number | fields account_number, firstname";
    JSONObject synchronous = executeQuery(query);

    Response submitResponse = submit(query, "0s", "5m");
    JSONObject submit = new JSONObject(getResponseBody(submitResponse, true));

    assertEquals(200, submitResponse.getStatusLine().getStatusCode());
    assertEquals("no-store", submitResponse.getHeader("Cache-Control"));
    assertEquals("RUNNING", submit.getString("status"));
    assertTrue(submit.has("id"));
    assertMinimalRunningSnapshot(submit);

    String id = submit.getString("id");
    JSONObject completed = pollUntilTerminal(id);

    assertEquals("SUCCEEDED", completed.getString("status"));
    assertEquals(id, completed.getString("id"));
    assertEquals(
        synchronous.getJSONArray("schema").toString(), completed.getJSONArray("schema").toString());
    assertEquals(
        synchronous.getJSONArray("datarows").toString(),
        completed.getJSONArray("datarows").toString());
    assertEquals(synchronous.getInt("total"), completed.getInt("total"));
    assertTrue(completed.has("took"));
    assertFalse(completed.has("size"));
    assertFalse(completed.has("progress"));
    assertFalse(completed.has("window"));
    assertFalse(completed.has("update_mode"));
    assertFalse(completed.has("start_time_in_millis"));
    assertFalse(completed.has("expiration_time_in_millis"));

    JSONObject deleted = delete(id);
    assertEquals("SUCCEEDED", deleted.getString("status"));
    ResponseException missing = assertThrows(ResponseException.class, () -> get(id, null));
    assertEquals(404, missing.getResponse().getStatusLine().getStatusCode());
  }

  @Test
  public void fastCompletionReturnsFinalResultWithoutId() throws IOException {
    String query = "source=" + TEST_INDEX_BANK + " | stats count()";

    JSONObject response = new JSONObject(getResponseBody(submit(query, "30s", "5m"), true));

    assertEquals("SUCCEEDED", response.getString("status"));
    assertFalse(response.has("id"));
    assertEquals(1, response.getJSONArray("datarows").length());
    assertFalse(response.has("size"));
    assertFalse(response.has("progress"));
  }

  @Test
  public void existingSynchronousResponseRemainsUnchanged() throws IOException {
    JSONObject response = executeQuery("source=" + TEST_INDEX_BANK + " | head 1");

    assertFalse(response.has("id"));
    assertFalse(response.has("status"));
    assertTrue(response.has("size"));
  }

  @Test
  public void fastFailureReturnsSanitizedTerminalResponseWithoutId() throws IOException {
    JSONObject response =
        new JSONObject(
            getResponseBody(
                submit("source=" + TEST_INDEX_BANK + " | unsupported_async_command", "30s", "5m"),
                true));

    assertEquals("FAILED", response.getString("status"));
    assertFalse(response.has("id"));
    assertEquals("query execution failed", response.getJSONObject("error").getString("reason"));
    assertMinimalRunningSnapshot(response);
  }

  @Test
  public void asyncQueryRejectsPartialResultsAndNonJsonFormats() {
    Request partial = new Request("POST", QUERY_API_ENDPOINT);
    partial.setJsonEntity(
        new JSONObject()
            .put("query", "source=" + TEST_INDEX_BANK)
            .put("wait_for_completion_timeout", "0s")
            .put("partial_result", true)
            .toString());

    ResponseException partialFailure =
        assertThrows(ResponseException.class, () -> client().performRequest(partial));
    assertEquals(400, partialFailure.getResponse().getStatusLine().getStatusCode());

    Request csv = new Request("POST", QUERY_API_ENDPOINT + "?format=csv");
    csv.setJsonEntity(
        new JSONObject()
            .put("query", "source=" + TEST_INDEX_BANK)
            .put("wait_for_completion_timeout", "0s")
            .toString());

    ResponseException csvFailure =
        assertThrows(ResponseException.class, () -> client().performRequest(csv));
    assertEquals(400, csvFailure.getResponse().getStatusLine().getStatusCode());
  }

  @Test
  public void asyncQueryRejectsExplainAndAnalyzeModes() {
    Request explain = new Request("POST", "/_plugins/_ppl/_explain");
    explain.setJsonEntity(
        new JSONObject()
            .put("query", "source=" + TEST_INDEX_BANK)
            .put("wait_for_completion_timeout", "0s")
            .toString());

    ResponseException explainFailure =
        assertThrows(ResponseException.class, () -> client().performRequest(explain));
    assertEquals(400, explainFailure.getResponse().getStatusLine().getStatusCode());

    Request analyze = new Request("POST", QUERY_API_ENDPOINT);
    analyze.setJsonEntity(
        new JSONObject()
            .put("query", "source=" + TEST_INDEX_BANK)
            .put("analyze", true)
            .put("wait_for_completion_timeout", "0s")
            .toString());

    ResponseException analyzeFailure =
        assertThrows(ResponseException.class, () -> client().performRequest(analyze));
    assertEquals(400, analyzeFailure.getResponse().getStatusLine().getStatusCode());
  }

  private Response submit(String query, String waitForCompletion, String keepAlive)
      throws IOException {
    Request request = new Request("POST", QUERY_API_ENDPOINT);
    request.setJsonEntity(
        new JSONObject()
            .put("query", query)
            .put("wait_for_completion_timeout", waitForCompletion)
            .put("keep_alive", keepAlive)
            .toString());
    return client().performRequest(request);
  }

  private JSONObject pollUntilTerminal(String id) throws Exception {
    JSONObject response = null;
    for (int attempt = 0; attempt < 100; attempt++) {
      response = new JSONObject(getResponseBody(get(id, "5m"), true));
      if (!"RUNNING".equals(response.getString("status"))) {
        return response;
      }
      Thread.sleep(50);
    }
    assertNotNull(response);
    throw new AssertionError("PPL asynchronous query did not complete: " + response);
  }

  private Response get(String id, String keepAlive) throws IOException {
    String endpoint = ASYNC_JOB_API_ENDPOINT.replace("{id}", id);
    if (keepAlive != null) {
      endpoint += "?keep_alive=" + keepAlive;
    }
    return client().performRequest(new Request("GET", endpoint));
  }

  private JSONObject delete(String id) throws IOException {
    String endpoint = ASYNC_JOB_API_ENDPOINT.replace("{id}", id);
    Response response = client().performRequest(new Request("DELETE", endpoint));
    return new JSONObject(getResponseBody(response, true));
  }

  private static void assertMinimalRunningSnapshot(JSONObject response) {
    assertEquals(0, response.getJSONArray("schema").length());
    assertEquals(0, response.getJSONArray("datarows").length());
    assertEquals(0, response.getInt("total"));
    assertFalse(response.has("progress"));
    assertFalse(response.has("size"));
  }
}
