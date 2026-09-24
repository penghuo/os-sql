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
  public void fastFailurePreservesSynchronousErrorStatusAndMessage() throws IOException {
    String query = "source=" + TEST_INDEX_BANK + " | unsupported_async_command";

    ResponseException synchronous =
        assertThrows(ResponseException.class, () -> executeQuery(query));
    ResponseException asynchronous =
        assertThrows(ResponseException.class, () -> submit(query, "30s", "5m"));

    assertEquals(
        synchronous.getResponse().getStatusLine().getStatusCode(),
        asynchronous.getResponse().getStatusLine().getStatusCode());
    JSONObject synchronousError =
        new JSONObject(getResponseBody(synchronous.getResponse(), true)).getJSONObject("error");
    JSONObject asynchronousError =
        new JSONObject(getResponseBody(asynchronous.getResponse(), true)).getJSONObject("error");
    assertEquals(synchronousError.getString("details"), asynchronousError.getString("details"));
  }

  @Test
  public void retainedFailurePreservesMessage() throws Exception {
    JSONObject submitted =
        new JSONObject(
            getResponseBody(
                submit("source=" + TEST_INDEX_BANK + " | unsupported_async_command", "0s", "5m"),
                true));

    JSONObject failed = pollUntilTerminal(submitted.getString("id"));

    assertEquals("FAILED", failed.getString("status"));
    assertTrue(
        failed.getJSONObject("error").getString("reason").contains("unsupported_async_command"));
  }

  @Test
  public void numericAsyncLifecycleFieldsReturnBadRequest() {
    Request numericKeepAlive = new Request("POST", QUERY_API_ENDPOINT);
    numericKeepAlive.setJsonEntity(
        new JSONObject()
            .put("query", "source=" + TEST_INDEX_BANK)
            .put("keep_alive", 300)
            .toString());
    Request numericWait = new Request("POST", QUERY_API_ENDPOINT);
    numericWait.setJsonEntity(
        new JSONObject()
            .put("query", "source=" + TEST_INDEX_BANK)
            .put("wait_for_completion_timeout", 1)
            .toString());

    ResponseException keepAliveFailure =
        assertThrows(ResponseException.class, () -> client().performRequest(numericKeepAlive));
    ResponseException waitFailure =
        assertThrows(ResponseException.class, () -> client().performRequest(numericWait));

    assertEquals(400, keepAliveFailure.getResponse().getStatusLine().getStatusCode());
    assertEquals(400, waitFailure.getResponse().getStatusLine().getStatusCode());
  }

  @Test
  public void disabledPplRejectsSubmitGetAndDelete() throws Exception {
    JSONObject submitted =
        new JSONObject(
            getResponseBody(submit("source=" + TEST_INDEX_BANK + " | head 1", "0s", "5m"), true));
    String id = submitted.getString("id");

    updateClusterSettings(new ClusterSetting(PERSISTENT, "plugins.ppl.enabled", "false"));
    try {
      ResponseException submitFailure =
          assertThrows(
              ResponseException.class,
              () -> submit("source=" + TEST_INDEX_BANK + " | head 1", "0s", "5m"));
      ResponseException getFailure = assertThrows(ResponseException.class, () -> get(id, null));
      ResponseException deleteFailure =
          assertThrows(
              ResponseException.class,
              () ->
                  client()
                      .performRequest(
                          new Request("DELETE", ASYNC_JOB_API_ENDPOINT.replace("{id}", id))));

      assertEquals(400, submitFailure.getResponse().getStatusLine().getStatusCode());
      assertEquals(400, getFailure.getResponse().getStatusLine().getStatusCode());
      assertEquals(400, deleteFailure.getResponse().getStatusLine().getStatusCode());
    } finally {
      updateClusterSettings(new ClusterSetting(PERSISTENT, "plugins.ppl.enabled", null));
    }
    delete(id);
  }

  @Test
  public void partialResultRemainsAsyncAndExplicitFormatsFallBackToSync() throws IOException {
    Request partial = new Request("POST", QUERY_API_ENDPOINT);
    partial.setJsonEntity(
        new JSONObject()
            .put("query", "source=" + TEST_INDEX_BANK)
            .put("wait_for_completion_timeout", "30s")
            .put("partial_result", true)
            .toString());

    JSONObject partialResponse =
        new JSONObject(getResponseBody(client().performRequest(partial), true));
    assertEquals("SUCCEEDED", partialResponse.getString("status"));
    assertFalse(partialResponse.has("id"));

    Request csv = new Request("POST", QUERY_API_ENDPOINT + "?format=csv");
    csv.setJsonEntity(
        new JSONObject()
            .put("query", "source=" + TEST_INDEX_BANK + " | head 1 | fields account_number")
            .put("wait_for_completion_timeout", "0s")
            .toString());

    Response csvRestResponse = client().performRequest(csv);
    String csvResponse = getResponseBody(csvRestResponse, true);
    assertTrue(csvResponse.startsWith("account_number"));
    assertFalse(csvResponse.contains("\"status\""));

    Request jdbc = new Request("POST", QUERY_API_ENDPOINT + "?format=jdbc");
    jdbc.setJsonEntity(
        new JSONObject()
            .put("query", "source=" + TEST_INDEX_BANK + " | head 1")
            .put("keep_alive", "5m")
            .toString());

    Response jdbcRestResponse = client().performRequest(jdbc);
    JSONObject jdbcResponse = new JSONObject(getResponseBody(jdbcRestResponse, true));
    assertFalse(jdbcResponse.has("status"));
    assertTrue(jdbcResponse.has("size"));
  }

  @Test
  public void asyncFieldsAreIgnoredForExplainAnalyzeAndProfile() throws IOException {
    Request explain = new Request("POST", "/_plugins/_ppl/_explain");
    explain.setJsonEntity(
        new JSONObject()
            .put("query", "source=" + TEST_INDEX_BANK)
            .put("wait_for_completion_timeout", "0s")
            .toString());

    JSONObject explainResponse =
        new JSONObject(getResponseBody(client().performRequest(explain), true));
    assertTrue(explainResponse.has("calcite"));
    assertFalse(explainResponse.has("status"));

    Request explainCommand = new Request("POST", QUERY_API_ENDPOINT);
    explainCommand.setJsonEntity(
        new JSONObject()
            .put("query", "explain source=" + TEST_INDEX_BANK)
            .put("keep_alive", "5m")
            .toString());

    JSONObject explainCommandResponse =
        new JSONObject(getResponseBody(client().performRequest(explainCommand), true));
    assertTrue(explainCommandResponse.has("calcite"));
    assertFalse(explainCommandResponse.has("status"));

    Request analyze = new Request("POST", QUERY_API_ENDPOINT);
    analyze.setJsonEntity(
        new JSONObject()
            .put("query", "source=" + TEST_INDEX_BANK)
            .put("analyze", true)
            .put("wait_for_completion_timeout", "0s")
            .toString());

    JSONObject analyzeResponse =
        new JSONObject(getResponseBody(client().performRequest(analyze), true));
    assertTrue(analyzeResponse.has("logicalPlan"));
    assertFalse(analyzeResponse.has("status"));

    Request profile = new Request("POST", QUERY_API_ENDPOINT);
    profile.setJsonEntity(
        new JSONObject()
            .put("query", "source=" + TEST_INDEX_BANK + " | head 1")
            .put("profile", true)
            .put("keep_alive", "5m")
            .toString());

    JSONObject profileResponse =
        new JSONObject(getResponseBody(client().performRequest(profile), true));
    assertTrue(profileResponse.has("profile"));
    assertFalse(profileResponse.has("status"));
  }

  @Test
  public void asyncFieldsAreIgnoredWhenCalciteIsDisabled() throws IOException {
    disableCalcite();
    try {
      Request request = new Request("POST", QUERY_API_ENDPOINT);
      request.setJsonEntity(
          new JSONObject()
              .put("query", "source=" + TEST_INDEX_BANK + " | head 1")
              .put("wait_for_completion_timeout", "0s")
              .toString());

      Response restResponse = client().performRequest(request);
      JSONObject response = new JSONObject(getResponseBody(restResponse, true));
      assertFalse(response.has("status"));
      assertTrue(response.has("size"));
    } finally {
      enableCalcite();
    }
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
