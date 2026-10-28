/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.opensearch.sql.legacy.TestUtils.getResponseBody;
import static org.opensearch.sql.ppl.AsyncPPLTestHelpers.INDEX;
import static org.opensearch.sql.ppl.AsyncPPLTestHelpers.STREAMSTATS_QUERY;
import static org.opensearch.sql.ppl.AsyncPPLTestHelpers.awaitPoolBusy;
import static org.opensearch.sql.ppl.AsyncPPLTestHelpers.awaitPoolsIdle;
import static org.opensearch.sql.ppl.AsyncPPLTestHelpers.deleteAsyncQuery;
import static org.opensearch.sql.ppl.AsyncPPLTestHelpers.getAsyncQuery;
import static org.opensearch.sql.ppl.AsyncPPLTestHelpers.localNodeId;
import static org.opensearch.sql.ppl.AsyncPPLTestHelpers.pollUntilTerminal;
import static org.opensearch.sql.ppl.AsyncPPLTestHelpers.postPpl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.opensearch.client.Request;
import org.opensearch.client.ResponseException;

/**
 * Capacity limits of async PPL (#5801): worker-pool saturation, queueing past the running limit,
 * and the queue limit and timeout.
 *
 * <p>Runs on the dedicated single-node {@code asyncCapacityIT} cluster, whose {@code sql-worker}
 * and {@code sql-complex-worker} pools each have one thread and a one-slot queue, so saturation is
 * deterministic. Every test deletes the jobs it creates and restores the settings it changes.
 */
public class AsyncPPLCapacityIT extends PPLIntegTestCase {

  private static final String COMPLEX_POOL = "sql-complex-worker";
  private static final String QUICK_QUERY = "source=" + INDEX + " | head 1";

  /** Ten thousand serial searches: outlasts every saturation check, stops promptly on DELETE. */
  private static final String SLOW_INDEX = "async_capacity_slow_fixture";

  private static final String SLOW_QUERY =
      "source=" + SLOW_INDEX + " | streamstats count() as c | stats max(c) as total";

  private final List<String> jobIds = new ArrayList<>();
  private final List<String> changedSettings = new ArrayList<>();

  @Override
  protected void init() throws Exception {
    super.init();
    enableCalcite();
    AsyncPPLTestHelpers.createIndex(client());
    AsyncPPLTestHelpers.createIndex(client(), SLOW_INDEX, 100_000);
  }

  @After
  public void cleanUp() throws Exception {
    for (String id : jobIds) {
      try {
        deleteAsyncQuery(client(), id);
      } catch (ResponseException e) {
        // Already deleted, evicted, or rejected before it was retained.
      }
    }
    jobIds.clear();
    for (String key : changedSettings) {
      setClusterSetting(key, null);
    }
    changedSettings.clear();
    awaitPoolsIdle(client(), localNodeId(client()));
  }

  @Test
  public void saturatedWorkerPoolFailsSyncAndAsyncQueriesWith429() throws Exception {
    String nodeId = localNodeId(client());
    submitAccepted(SLOW_QUERY);
    awaitPoolBusy(client(), nodeId, COMPLEX_POOL, 1, 0);
    submitAccepted(SLOW_QUERY);
    awaitPoolBusy(client(), nodeId, COMPLEX_POOL, 1, 1);
    String saturated = "[sql-complex-worker] thread pool is saturated; retry later";

    // Before the fix the rejected sync request never received a response.
    ResponseException sync =
        Assert.assertThrows(
            ResponseException.class,
            () -> postPpl(client(), new JSONObject().put("query", SLOW_QUERY)));
    Assert.assertEquals(429, sync.getResponse().getStatusLine().getStatusCode());
    Assert.assertTrue(errorBody(sync).contains(saturated));

    // Before the fix the rejected async job stayed RUNNING forever. The pool refuses it after
    // planning, so it fails inline with 429 or, once its id was returned, ends FAILED.
    JSONObject async;
    try {
      async = submitAsync(STREAMSTATS_QUERY);
    } catch (ResponseException e) {
      Assert.assertEquals(429, e.getResponse().getStatusLine().getStatusCode());
      Assert.assertTrue(errorBody(e).contains(saturated));
      return;
    }
    JSONObject terminal = pollUntilTerminal(client(), async.getString("id"), 30_000);
    Assert.assertEquals("FAILED", terminal.getString("status"));
    Assert.assertTrue(terminal.getJSONObject("error").toString().contains(saturated));
  }

  @Test
  public void submitOverTheRunningLimitWaitsAsPendingUntilASlotFrees() throws Exception {
    setClusterSetting("plugins.ppl.async.node_concurrent_running_queries", "1");
    String nodeId = localNodeId(client());
    String running = submitAccepted(SLOW_QUERY);
    awaitPoolBusy(client(), nodeId, COMPLEX_POOL, 1, 0);

    JSONObject queued = submitAsync(QUICK_QUERY);
    Assert.assertEquals("PENDING", queued.getString("status"));
    String queuedId = queued.getString("id");
    awaitStatus(queuedId, "PENDING");

    deleteAsyncQuery(client(), running);
    Assert.assertEquals(
        "SUCCEEDED", pollUntilTerminal(client(), queuedId, 30_000).getString("status"));
  }

  @Test
  public void fullQueueRejectsSubmitWith429() throws Exception {
    setClusterSetting("plugins.ppl.async.node_concurrent_running_queries", "1");
    setClusterSetting("plugins.ppl.async.max_queued_queries", "1");
    String nodeId = localNodeId(client());
    submitAccepted(SLOW_QUERY);
    awaitPoolBusy(client(), nodeId, COMPLEX_POOL, 1, 0);
    Assert.assertEquals("PENDING", submitAsync(QUICK_QUERY).getString("status"));

    ResponseException rejected =
        Assert.assertThrows(ResponseException.class, () -> submit(QUICK_QUERY));
    Assert.assertEquals(429, rejected.getResponse().getStatusLine().getStatusCode());
    Assert.assertTrue(errorBody(rejected).contains("[plugins.ppl.async.max_queued_queries]"));
  }

  @Test
  public void queuedJobFailsAfterTheQueueTimeout() throws Exception {
    setClusterSetting("plugins.ppl.async.node_concurrent_running_queries", "1");
    setClusterSetting("plugins.ppl.async.max_queued_time", "1s");
    String nodeId = localNodeId(client());
    submitAccepted(SLOW_QUERY);
    awaitPoolBusy(client(), nodeId, COMPLEX_POOL, 1, 0);

    JSONObject queued = submitAsync(QUICK_QUERY);
    Assert.assertEquals("PENDING", queued.getString("status"));

    JSONObject terminal = pollUntilTerminal(client(), queued.getString("id"), 30_000);
    Assert.assertEquals("FAILED", terminal.getString("status"));
    Assert.assertTrue(
        terminal.getJSONObject("error").toString().contains("[plugins.ppl.async.max_queued_time]"));
  }

  /** Submits with {@code wait_for_completion_timeout=0}, expects it accepted, returns its id. */
  private String submitAccepted(String query) throws IOException {
    JSONObject response = submitAsync(query);
    Assert.assertTrue(List.of("PENDING", "RUNNING").contains(response.getString("status")));
    return response.getString("id");
  }

  /** Submits with {@code wait_for_completion_timeout=0}, tracks the id, returns the response. */
  private JSONObject submitAsync(String query) throws IOException {
    JSONObject response = new JSONObject(submit(query));
    jobIds.add(response.getString("id"));
    return response;
  }

  /** Polls GET until the job reports {@code status}. */
  private void awaitStatus(String id, String status) throws Exception {
    long deadline = System.currentTimeMillis() + 10_000;
    String last = null;
    while (System.currentTimeMillis() < deadline) {
      last = new JSONObject(getAsyncQuery(client(), id)).getString("status");
      if (status.equals(last)) {
        return;
      }
      Thread.sleep(50);
    }
    Assert.fail("job never reported " + status + "; last=" + last);
  }

  private String submit(String query) throws IOException {
    return postPpl(
        client(), new JSONObject().put("query", query).put("wait_for_completion_timeout", "0"));
  }

  private static String errorBody(ResponseException e) throws IOException {
    return getResponseBody(e.getResponse(), true);
  }

  private void setClusterSetting(String key, String value) throws IOException {
    Request request = new Request("PUT", "/_cluster/settings");
    request.setJsonEntity(
        value == null
            ? "{\"transient\":{\"" + key + "\": null}}"
            : "{\"transient\":{\"" + key + "\": \"" + value + "\"}}");
    client().performRequest(request);
    if (value != null && !changedSettings.contains(key)) {
      changedSettings.add(key);
    }
  }
}
