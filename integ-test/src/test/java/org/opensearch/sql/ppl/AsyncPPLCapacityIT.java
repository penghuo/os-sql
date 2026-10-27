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
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;

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
 * the queue limit and timeout, the retained job limit, the retained result budget, and the dynamic
 * wait / keep-alive limits.
 *
 * <p>Runs on the dedicated single-node {@code asyncCapacityIT} cluster, whose {@code sql-worker}
 * and {@code sql-complex-worker} pools each have one thread and a one-slot queue, so saturation is
 * deterministic and no other IT's retained jobs count against the limits. Every test deletes the
 * jobs it creates and restores the settings it changes.
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
  public void saturatedWorkerPoolFailsSyncQueriesAndQueuesAsyncOnes() throws Exception {
    String nodeId = localNodeId(client());
    String first = submitAccepted(SLOW_QUERY);
    awaitPoolBusy(client(), nodeId, COMPLEX_POOL, 1, 0);
    String second = submitAccepted(SLOW_QUERY);
    awaitPoolBusy(client(), nodeId, COMPLEX_POOL, 1, 1);

    // Before the fix the rejected sync request never received a response.
    ResponseException sync =
        Assert.assertThrows(
            ResponseException.class,
            () -> postPpl(client(), new JSONObject().put("query", SLOW_QUERY)));
    Assert.assertEquals(429, sync.getResponse().getStatusLine().getStatusCode());
    Assert.assertTrue(
        errorBody(sync).contains("[sql-complex-worker] thread pool is saturated; retry later"));

    // Before the fix the rejected async job stayed RUNNING forever; now it waits for capacity.
    String waiting = submitAsync(STREAMSTATS_QUERY).getString("id");
    awaitStatus(waiting, "PENDING");
    // Refused and retried while the pools stay full, it is never reported as RUNNING.
    for (int i = 0; i < 20; i++) {
      Assert.assertEquals("PENDING", status(waiting));
      Thread.sleep(100);
    }
    deleteAsyncQuery(client(), first);
    deleteAsyncQuery(client(), second);
    // Once it starts, its status only moves forward: PENDING, RUNNING, SUCCEEDED.
    List<String> seen = new ArrayList<>();
    long deadline = System.currentTimeMillis() + 30_000;
    while (System.currentTimeMillis() < deadline
        && (seen.isEmpty() || !"SUCCEEDED".equals(seen.get(seen.size() - 1)))) {
      String status = status(waiting);
      if (seen.isEmpty() || !seen.get(seen.size() - 1).equals(status)) {
        seen.add(status);
      }
      Thread.sleep(20);
    }
    List<String> order = List.of("PENDING", "RUNNING", "SUCCEEDED");
    for (int i = 1; i < seen.size(); i++) {
      Assert.assertTrue(
          "status went backwards: " + seen,
          order.indexOf(seen.get(i - 1)) < order.indexOf(seen.get(i)));
    }
    Assert.assertEquals("SUCCEEDED", seen.get(seen.size() - 1));
    verifyDataRows(new JSONObject(getAsyncQuery(client(), waiting)), rows(10_000));
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
    // Running and queued jobs do not count toward the retained (finished) job limit.
    setClusterSetting("plugins.ppl.async.max_retained_jobs", "1");
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

  @Test
  public void retainedJobLimitRejectsSubmitUntilAJobIsDeleted() throws Exception {
    setClusterSetting("plugins.ppl.async.max_retained_jobs", "2");
    String first = submitAccepted(QUICK_QUERY);
    pollUntilTerminal(client(), first, 30_000);
    pollUntilTerminal(client(), submitAccepted(QUICK_QUERY), 30_000);

    ResponseException rejected =
        Assert.assertThrows(ResponseException.class, () -> submit(QUICK_QUERY));
    Assert.assertEquals(429, rejected.getResponse().getStatusLine().getStatusCode());
    Assert.assertTrue(errorBody(rejected).contains("[plugins.ppl.async.max_retained_jobs]"));

    deleteAsyncQuery(client(), first);
    Assert.assertEquals(
        "SUCCEEDED",
        pollUntilTerminal(client(), submitAccepted(QUICK_QUERY), 30_000).getString("status"));
  }

  @Test
  public void resultThatDoesNotFitTheBudgetFailsTheJobAndDropsRows() throws Exception {
    setClusterSetting("plugins.ppl.async.max_retained_result_size", "1kb");

    JSONObject terminal =
        pollUntilTerminal(client(), submitAccepted("source=" + INDEX + " | head 100"), 30_000);

    Assert.assertEquals("FAILED", terminal.getString("status"));
    Assert.assertTrue(
        terminal
            .getJSONObject("error")
            .toString()
            .contains("[plugins.ppl.async.max_retained_result_size]"));
    Assert.assertFalse(terminal.has("datarows"));
  }

  @Test
  public void exhaustedResultBudgetRejectsSubmitUntilAResultIsDeleted() throws Exception {
    String retained = submitAccepted(QUICK_QUERY);
    Assert.assertEquals(
        "SUCCEEDED", pollUntilTerminal(client(), retained, 30_000).getString("status"));
    setClusterSetting("plugins.ppl.async.max_retained_result_size", "1b");

    ResponseException rejected =
        Assert.assertThrows(ResponseException.class, () -> submit(QUICK_QUERY));
    Assert.assertEquals(429, rejected.getResponse().getStatusLine().getStatusCode());
    Assert.assertTrue(errorBody(rejected).contains("[plugins.ppl.async.max_retained_result_size]"));

    deleteAsyncQuery(client(), retained);
    submitAccepted(QUICK_QUERY);
  }

  @Test
  public void waitAndKeepAliveLimitsFollowDynamicSettings() throws Exception {
    setClusterSetting("plugins.ppl.async.max_wait_for_completion_timeout", "5s");
    setClusterSetting("plugins.ppl.async.max_keep_alive", "1m");

    assertBadRequest(
        new JSONObject().put("query", QUICK_QUERY).put("wait_for_completion_timeout", "10s"));
    assertBadRequest(new JSONObject().put("query", QUICK_QUERY).put("keep_alive", "2m"));
    JSONObject withinLimits =
        new JSONObject(
            postPpl(
                client(),
                new JSONObject()
                    .put("query", QUICK_QUERY)
                    .put("wait_for_completion_timeout", "5s")
                    .put("keep_alive", "1m")));
    Assert.assertTrue(withinLimits.has("datarows"));
    // The 5m keep_alive default exceeds the 1m limit; it is capped instead of rejected.
    submitAccepted(QUICK_QUERY);
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

  private String status(String id) throws IOException {
    return new JSONObject(getAsyncQuery(client(), id)).getString("status");
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

  private void assertBadRequest(JSONObject body) {
    ResponseException e =
        Assert.assertThrows(ResponseException.class, () -> postPpl(client(), body));
    Assert.assertEquals(400, e.getResponse().getStatusLine().getStatusCode());
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
