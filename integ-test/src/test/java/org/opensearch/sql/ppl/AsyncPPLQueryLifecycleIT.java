/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.opensearch.sql.legacy.TestUtils.getResponseBody;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_ACCOUNT;
import static org.opensearch.sql.ppl.AsyncPPLTestHelpers.ASYNC_QUERY_ENDPOINT;
import static org.opensearch.sql.ppl.AsyncPPLTestHelpers.BASELINE_MIN_SEARCHES;
import static org.opensearch.sql.ppl.AsyncPPLTestHelpers.DOCS;
import static org.opensearch.sql.ppl.AsyncPPLTestHelpers.PPL_ENDPOINT;
import static org.opensearch.sql.ppl.AsyncPPLTestHelpers.STREAMSTATS_QUERY;
import static org.opensearch.sql.ppl.AsyncPPLTestHelpers.assertNotFound;
import static org.opensearch.sql.ppl.AsyncPPLTestHelpers.assertStopped;
import static org.opensearch.sql.ppl.AsyncPPLTestHelpers.awaitPoolsIdle;
import static org.opensearch.sql.ppl.AsyncPPLTestHelpers.awaitRunning;
import static org.opensearch.sql.ppl.AsyncPPLTestHelpers.deleteAsyncQuery;
import static org.opensearch.sql.ppl.AsyncPPLTestHelpers.getAsyncQuery;
import static org.opensearch.sql.ppl.AsyncPPLTestHelpers.indexSearchCount;
import static org.opensearch.sql.ppl.AsyncPPLTestHelpers.localNodeId;
import static org.opensearch.sql.ppl.AsyncPPLTestHelpers.pollUntilTerminal;
import static org.opensearch.sql.ppl.AsyncPPLTestHelpers.postPpl;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifyNumOfRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import org.apache.hc.core5.http.HttpHost;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.client.RestClient;
import org.opensearch.common.settings.Settings;
import org.opensearch.sql.job.QueryJobId;

/**
 * End-to-end IT for the async PPL lifecycle (issue #5765). Verifies:
 *
 * <ul>
 *   <li>sync submit without async body fields returns the sync-shape response (schema + rows, no
 *       id, no status);
 *   <li>submit with {@code wait_for_completion_timeout=0} returns the running snapshot {@code {id,
 *       status: RUNNING, schema=[], datarows=[], total=0}} without blocking on the runner;
 *   <li>submit with a wait budget longer than runner duration returns the sync-shape terminal
 *       response inline (runner-wins-race);
 *   <li>fetch on {@code GET /_plugins/_async_query/{id}} eventually returns the terminal result
 *       with schema and rows;
 *   <li>statement-level explain (query text starts with {@code explain ...}) is supported in async
 *       and returns the explain body on GET;
 *   <li>sync-only request shapes (explain endpoint, analyze endpoint, profile flag, csv format) are
 *       rejected with 400 when they carry {@code wait_for_completion_timeout};
 *   <li>{@code keep_alive} drives retention — a job is evicted after its TTL elapses;
 *   <li>{@code DELETE /_plugins/_async_query/{id}} acknowledges with the job's final status and
 *       removes it, so later GET and DELETE return 404; unknown, expired, and absent-owner ids
 *       return 404.
 * </ul>
 *
 * <p>Runs on the Calcite engine. Also covers running-query cancellation on a single node (local
 * owner DELETE and streamstats baseline) via the fixture in {@link AsyncPPLTestHelpers}; forwarded
 * cancellation, concurrent deletes, and the FAILED path live in {@link AsyncPPLMultiNodeRoutingIT}.
 */
public class AsyncPPLQueryLifecycleIT extends PPLIntegTestCase {

  @Override
  protected void init() throws Exception {
    super.init();
    enableCalcite();
    loadIndex(Index.ACCOUNT);
  }

  @Test
  public void sync_submitReturnsSyncShapeWithFullResult() throws IOException {
    JSONObject body = new JSONObject();
    body.put("query", "source=" + TEST_INDEX_ACCOUNT + " | stats count() as c");

    JSONObject response = new JSONObject(postPpl(client(), body));

    Assert.assertFalse("sync response must not carry queryId", response.has("id"));
    verifySchema(response, schema("c", "bigint"));
    verifyDataRows(response, rows(1000));
    verifyNumOfRows(response, 1);
  }

  @Test
  public void async_submitWithZeroWaitReturnsRunningSnapshot() throws IOException {
    JSONObject body = new JSONObject();
    body.put("query", "source=" + TEST_INDEX_ACCOUNT + " | stats count() as c");
    body.put("wait_for_completion_timeout", "0");

    JSONObject response = new JSONObject(postPpl(client(), body));

    Assert.assertTrue("async response must carry queryId", response.has("id"));
    Assert.assertEquals("RUNNING", response.getString("status"));
    Assert.assertEquals(0, response.getJSONArray("schema").length());
    Assert.assertEquals(0, response.getJSONArray("datarows").length());
    Assert.assertEquals(0, response.getInt("total"));
  }

  @Test
  public void async_submitWithLongWaitReturnsSyncShape() throws IOException {
    JSONObject body = new JSONObject();
    body.put("query", "source=" + TEST_INDEX_ACCOUNT + " | stats count() as c");
    // Big budget — the stats runner finishes well before 30s, so the submit response is the
    // sync-shape terminal body (no id).
    body.put("wait_for_completion_timeout", "30s");

    JSONObject response = new JSONObject(postPpl(client(), body));

    Assert.assertFalse("runner-wins response must not carry queryId", response.has("id"));
    verifySchema(response, schema("c", "bigint"));
    verifyDataRows(response, rows(1000));
  }

  @Test
  public void async_explainWithLongWaitReturnsInlineBody() throws IOException {
    JSONObject body = new JSONObject();
    body.put("query", "explain source=" + TEST_INDEX_ACCOUNT + " | stats count() as c");
    body.put("wait_for_completion_timeout", "30s");

    JSONObject response = new JSONObject(postPpl(client(), body));

    Assert.assertFalse("inline explain must not carry a polling id", response.has("id"));
    Assert.assertTrue(
        "inline explain must carry a plan tree", response.has("calcite") || response.has("root"));
  }

  @Test
  public void async_inlineFailurePreservesClientErrorStatus() {
    JSONObject body = new JSONObject();
    body.put("query", "source=");
    body.put("wait_for_completion_timeout", "30s");
    assertRejectedWith400(PPL_ENDPOINT, body);
  }

  @Test
  public void async_fetchTerminalReturnsResultWithSchemaAndRows() throws Exception {
    JSONObject body = new JSONObject();
    body.put("query", "source=" + TEST_INDEX_ACCOUNT + " | stats count() as c");
    body.put("wait_for_completion_timeout", "0");

    String queryId = new JSONObject(postPpl(client(), body)).getString("id");
    JSONObject fetched = pollUntilTerminal(client(), queryId, 30_000);

    Assert.assertEquals("SUCCEEDED", fetched.getString("status"));
    // Async GET formatter emits raw engine type ("long"), not the JDBC family ("bigint") emitted
    // by the sync formatter above.
    verifySchema(fetched, schema("c", "long"));
    verifyDataRows(fetched, rows(1000));
    verifyNumOfRows(fetched, 1);
  }

  @Test
  public void async_explainStatementReturnsExplainBodyOnGet() throws Exception {
    JSONObject body = new JSONObject();
    body.put("query", "explain source=" + TEST_INDEX_ACCOUNT + " | stats count() as c");
    body.put("wait_for_completion_timeout", "0");

    String queryId = new JSONObject(postPpl(client(), body)).getString("id");

    // Explain terminal response has no `status` field. Poll until the body is JSON-parseable with
    // the calcite/root plan-tree marker.
    long deadline = System.currentTimeMillis() + 30_000L;
    JSONObject explain = null;
    while (System.currentTimeMillis() < deadline) {
      String raw = getAsyncQuery(client(), queryId);
      try {
        JSONObject parsed = new JSONObject(raw);
        if (parsed.has("calcite") || parsed.has("root")) {
          explain = parsed;
          break;
        }
      } catch (RuntimeException ignored) {
        // not valid JSON yet; keep polling
      }
      Thread.sleep(200);
    }
    Assert.assertNotNull("cross-poll explain body never arrived", explain);
    Assert.assertTrue(
        "explain body must carry a plan tree", explain.has("calcite") || explain.has("root"));
  }

  @Test
  public void async_fallsThroughToSyncForExplainEndpoint() throws IOException {
    // Explain endpoint ignores wait_for_completion_timeout and returns the sync explain body.
    JSONObject response =
        new JSONObject(
            postPpl(
                client(),
                PPL_ENDPOINT + "/_explain",
                withAsyncWait("source=" + TEST_INDEX_ACCOUNT + " | stats count() as c")));
    Assert.assertFalse("sync-explain response must not carry queryId", response.has("id"));
    Assert.assertTrue(
        "sync-explain response must carry a plan tree",
        response.has("calcite") || response.has("root"));
  }

  @Test
  public void async_fallsThroughToSyncForProfileFlag() throws IOException {
    JSONObject body = withAsyncWait("source=" + TEST_INDEX_ACCOUNT + " | stats count() as c");
    body.put("profile", true);
    JSONObject response = new JSONObject(postPpl(client(), PPL_ENDPOINT, body));
    Assert.assertFalse("sync-profile response must not carry queryId", response.has("id"));
    // Profile output carries the normal sync result; schema + rows are present.
    Assert.assertTrue("sync-profile response must carry schema", response.has("schema"));
    Assert.assertTrue("sync-profile response must carry datarows", response.has("datarows"));
  }

  @Test
  public void async_fallsThroughToSyncForCsvFormat() throws IOException {
    // CSV format: the response body is text/csv, not JSON. Just assert it isn't a JSON async
    // snapshot and that the row count matches sync behavior.
    Request request = new Request("POST", PPL_ENDPOINT + "?format=csv");
    request.setJsonEntity(
        withAsyncWait("source=" + TEST_INDEX_ACCOUNT + " | stats count() as c").toString());
    String body =
        org.opensearch.sql.legacy.TestUtils.getResponseBody(client().performRequest(request), true);
    Assert.assertFalse("csv response must not be a JSON async snapshot", body.contains("\"id\""));
    Assert.assertTrue(
        "csv response must carry the count=1000 row, got: " + body, body.contains("1000"));
  }

  @Test
  public void async_fetchUnknownQueryIdReturns4xx() {
    Request request = new Request("GET", ASYNC_QUERY_ENDPOINT + "nodeX%3Adoes-not-exist");
    ResponseException ex =
        Assert.assertThrows(ResponseException.class, () -> client().performRequest(request));
    int code = ex.getResponse().getStatusLine().getStatusCode();
    Assert.assertTrue("expected 4xx for unknown queryId, got " + code, code >= 400 && code < 500);
  }

  @Test
  public void async_keepAliveEvictsAfterTtl() throws Exception {
    JSONObject body = new JSONObject();
    body.put("query", "source=" + TEST_INDEX_ACCOUNT + " | stats count() as c");
    body.put("wait_for_completion_timeout", "0");
    body.put("keep_alive", "1s");

    String queryId = new JSONObject(postPpl(client(), body)).getString("id");
    JSONObject terminal = pollUntilTerminal(client(), queryId, 5_000);
    Assert.assertEquals("SUCCEEDED", terminal.getString("status"));

    // Eviction fires 1s after the terminal transition.
    long deadline = System.currentTimeMillis() + 3_000L;
    while (System.currentTimeMillis() < deadline) {
      try {
        getAsyncQuery(client(), queryId);
      } catch (ResponseException ex) {
        int code = ex.getResponse().getStatusLine().getStatusCode();
        Assert.assertTrue(
            "expected 4xx after keep_alive expiry, got " + code, code >= 400 && code < 500);
        return;
      }
      Thread.sleep(200);
    }
    Assert.fail("job [" + queryId + "] was not evicted within 3s after keep_alive=1s");
  }

  @Test
  public void async_deleteCompletedJobReturnsStatusAndRemovesIt() throws Exception {
    String queryId =
        new JSONObject(
                postPpl(
                    client(),
                    withAsyncWait("source=" + TEST_INDEX_ACCOUNT + " | stats count() as c")))
            .getString("id");
    Assert.assertEquals(
        "SUCCEEDED", pollUntilTerminal(client(), queryId, 30_000).getString("status"));

    Response deleted = deleteAsyncQuery(client(), queryId);

    Assert.assertEquals(200, deleted.getStatusLine().getStatusCode());
    Assert.assertEquals(
        "SUCCEEDED", new JSONObject(getResponseBody(deleted, true)).getString("status"));
    assertNotFound(() -> getAsyncQuery(client(), queryId));
    assertNotFound(() -> deleteAsyncQuery(client(), queryId));
  }

  @Test
  public void async_deleteUnknownQueryIdReturns404() throws IOException {
    String unknownId = QueryJobId.create(localNodeId(client())).encode();
    assertNotFound(() -> deleteAsyncQuery(client(), unknownId));
  }

  @Test
  public void async_deleteExpiredJobReturns404() throws Exception {
    JSONObject body = withAsyncWait("source=" + TEST_INDEX_ACCOUNT + " | stats count() as c");
    body.put("keep_alive", "1s");
    String queryId = new JSONObject(postPpl(client(), body)).getString("id");
    Assert.assertEquals(
        "SUCCEEDED", pollUntilTerminal(client(), queryId, 5_000).getString("status"));

    long deadline = System.currentTimeMillis() + 3_000L;
    while (true) {
      try {
        getAsyncQuery(client(), queryId);
      } catch (ResponseException e) {
        Assert.assertEquals(404, e.getResponse().getStatusLine().getStatusCode());
        break;
      }
      Assert.assertTrue(
          "job [" + queryId + "] was not evicted within 3s after keep_alive=1s",
          System.currentTimeMillis() < deadline);
      Thread.sleep(200);
    }

    assertNotFound(() -> deleteAsyncQuery(client(), queryId));
  }

  @Test
  public void async_queryIdOfAbsentOwnerReturns404() {
    String absentOwnerId = QueryJobId.create("absent-node").encode();
    assertNotFound(() -> getAsyncQuery(client(), absentOwnerId));
    assertNotFound(() -> deleteAsyncQuery(client(), absentOwnerId));
  }

  @Test
  public void async_rejectsWaitForCompletionTimeoutExceedingMax() {
    JSONObject body = new JSONObject();
    body.put("query", "source=" + TEST_INDEX_ACCOUNT + " | stats count() as c");
    body.put("wait_for_completion_timeout", "120s"); // > 60s cap
    assertRejectedWith400(PPL_ENDPOINT, body);
  }

  @Test
  public void async_rejectsKeepAliveExceedingMax() {
    JSONObject body = new JSONObject();
    body.put("query", "source=" + TEST_INDEX_ACCOUNT + " | stats count() as c");
    body.put("wait_for_completion_timeout", "0");
    body.put("keep_alive", "365d"); // > 24h cap
    assertRejectedWith400(PPL_ENDPOINT, body);
  }

  @Test
  public void async_rejectsZeroKeepAlive() {
    JSONObject body = new JSONObject();
    body.put("query", "source=" + TEST_INDEX_ACCOUNT + " | stats count() as c");
    body.put("wait_for_completion_timeout", "0");
    body.put("keep_alive", "0"); // not strictly positive
    assertRejectedWith400(PPL_ENDPOINT, body);
  }

  // ---------- Running-query cancellation on a single node ----------
  //
  // The two cases below install the cancellation fixture on demand, pin a node client, and
  // drain the owner's SQL pools in a finally so a mid-test failure can't leak work.

  @Test
  public void async_baselineStreamstatsReadsEveryBatchAndReportsCorrectTotal() throws Exception {
    AsyncPPLTestHelpers.createIndex(client());
    try (RestClient owner = pinnedOwnerClient()) {
      String ownerNodeId = localNodeId(owner);
      try {
        long before = indexSearchCount(owner);
        JSONObject body = new JSONObject();
        body.put("query", STREAMSTATS_QUERY);
        // The async submit still returns ID + RUNNING if the scan can't finish inside the wait,
        // so handle both shapes.
        body.put("wait_for_completion_timeout", "60s");
        JSONObject response = new JSONObject(postPpl(owner, body));
        JSONObject terminal;
        if (response.has("id")) {
          terminal = pollUntilTerminal(owner, response.getString("id"), 60_000);
          Assert.assertEquals("SUCCEEDED", terminal.getString("status"));
        } else {
          terminal = response;
        }
        JSONArray row = terminal.getJSONArray("datarows").getJSONArray(0);
        Assert.assertEquals(
            "streamstats baseline must aggregate across the full scan to " + DOCS,
            DOCS,
            row.getInt(0));
        long after = indexSearchCount(owner);
        Assert.assertTrue(
            "baseline scan must read at least "
                + BASELINE_MIN_SEARCHES
                + " batches: before="
                + before
                + " after="
                + after,
            after - before >= BASELINE_MIN_SEARCHES);
      } finally {
        awaitPoolsIdle(owner, ownerNodeId);
      }
    }
  }

  @Test
  public void async_deleteOnOwnerCancelsStreamstatsAndStopsExecution() throws Exception {
    AsyncPPLTestHelpers.createIndex(client());
    try (RestClient owner = pinnedOwnerClient()) {
      String ownerNodeId = localNodeId(owner);
      try {
        long searchesBefore = indexSearchCount(owner);
        JSONObject body = new JSONObject();
        body.put("query", STREAMSTATS_QUERY);
        body.put("wait_for_completion_timeout", "0");
        String queryId = new JSONObject(postPpl(owner, body)).getString("id");
        AsyncPPLTestHelpers.RunningSnapshot running =
            awaitRunning(owner, ownerNodeId, searchesBefore);
        Assert.assertEquals(
            "RUNNING", new JSONObject(getAsyncQuery(owner, queryId)).getString("status"));

        long searchesAtDelete = indexSearchCount(owner);
        Response deleted = deleteAsyncQuery(owner, queryId);
        Assert.assertEquals(200, deleted.getStatusLine().getStatusCode());
        Assert.assertEquals(
            "CANCELLED", new JSONObject(getResponseBody(deleted, true)).getString("status"));
        assertStopped(owner, ownerNodeId, searchesAtDelete, running.pits);
        assertNotFound(() -> getAsyncQuery(owner, queryId));
        assertNotFound(() -> deleteAsyncQuery(owner, queryId));
      } finally {
        awaitPoolsIdle(owner, ownerNodeId);
      }
    }
  }

  /**
   * Pins a {@link RestClient} to the first cluster host so {@link #client()} round-robin cannot
   * split pre/post observations between nodes. Uses {@code buildClient(Settings.EMPTY, ...)} so the
   * remote-client options the base test case already honors are inherited.
   */
  private RestClient pinnedOwnerClient() throws IOException {
    HttpHost host = getClusterHosts().get(0);
    return buildClient(Settings.EMPTY, new HttpHost[] {host});
  }

  private static JSONObject withAsyncWait(String query) {
    JSONObject body = new JSONObject();
    body.put("query", query);
    body.put("wait_for_completion_timeout", "0");
    return body;
  }

  private void assertRejectedWith400(String endpoint, JSONObject body) {
    Request request = new Request("POST", endpoint);
    request.setJsonEntity(body.toString());
    ResponseException ex =
        Assert.assertThrows(ResponseException.class, () -> client().performRequest(request));
    Assert.assertEquals(400, ex.getResponse().getStatusLine().getStatusCode());
  }
}
