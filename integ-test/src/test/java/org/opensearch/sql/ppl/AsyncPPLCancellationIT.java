/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.opensearch.sql.legacy.TestUtils.getResponseBody;
import static org.opensearch.sql.ppl.AsyncPPLTestHelpers.assertNotFound;
import static org.opensearch.sql.ppl.AsyncPPLTestHelpers.deleteAsyncQuery;
import static org.opensearch.sql.ppl.AsyncPPLTestHelpers.getAsyncQuery;
import static org.opensearch.sql.ppl.AsyncPPLTestHelpers.pollUntilTerminal;
import static org.opensearch.sql.ppl.AsyncPPLTestHelpers.postPpl;
import static org.opensearch.sql.ppl.AsyncQueryFixture.BASELINE_MIN_SEARCHES;
import static org.opensearch.sql.ppl.AsyncQueryFixture.DOCS;
import static org.opensearch.sql.ppl.AsyncQueryFixture.EVENTSTATS_QUERY;
import static org.opensearch.sql.ppl.AsyncQueryFixture.OVERFLOW_QUERY;
import static org.opensearch.sql.ppl.AsyncQueryFixture.STREAMSTATS_QUERY;
import static org.opensearch.sql.ppl.AsyncQueryFixture.assertStopped;
import static org.opensearch.sql.ppl.AsyncQueryFixture.awaitPoolsIdle;
import static org.opensearch.sql.ppl.AsyncQueryFixture.awaitRunning;
import static org.opensearch.sql.ppl.AsyncQueryFixture.indexSearchCount;
import static org.opensearch.sql.ppl.AsyncQueryFixture.localNodeId;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.hc.core5.http.HttpHost;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.client.RestClient;
import org.opensearch.common.settings.Settings;
import org.opensearch.sql.legacy.SQLIntegTestCase;

/**
 * Cancels real running Calcite async queries and asserts that execution stops. Runs on the two-node
 * {@code asyncMultiNodeIT} cluster. A natural {@code streamstats} or {@code eventstats} scan over a
 * bulk-loaded index stays running past the DELETE round trip, and node thread-pool plus index stats
 * observe that execution actually stops after cancellation.
 *
 * <p>The complex-worker pool is disabled so both commands run on {@code sql-worker}. The component
 * regression {@code CancelledQueryFallbackTest} covers the fallback guard; this IT cannot
 * independently prove it because the legacy engine does not analyze streamstats / eventstats, so a
 * disabled guard would fail with {@code CalciteUnsupportedException} rather than scan again.
 */
public class AsyncPPLCancellationIT extends PPLIntegTestCase {

  private static final String FALLBACK_ALLOWED = "plugins.calcite.fallback.allowed";
  private static final String COMPLEX_POOL_ENABLED = "plugins.sql.complex_worker_pool.enabled";

  private RestClient owner;
  private RestClient peer;
  private String ownerNodeId;

  @Override
  protected void init() throws Exception {
    super.init();
    enableCalcite();
    setClusterSetting(FALLBACK_ALLOWED, "true");
    setClusterSetting(COMPLEX_POOL_ENABLED, "false");
    RestClient[] nodes = AsyncQueryFixture.twoNodeClients(getClusterHosts(), this::nodeClient);
    owner = nodes[0];
    peer = nodes[1];
    ownerNodeId = localNodeId(owner);
    AsyncQueryFixture.createIndex(client());
  }

  @After
  public void tearDownFixture() throws Exception {
    if (owner != null) {
      awaitPoolsIdle(owner, ownerNodeId);
      owner.close();
    }
    if (peer != null) {
      peer.close();
    }
    setClusterSetting(FALLBACK_ALLOWED, null);
    setClusterSetting(COMPLEX_POOL_ENABLED, null);
  }

  @Test
  public void baselineStreamstatsReadsEveryBatchAndReportsCorrectTotal() throws Exception {
    assertBaselineAggregate(STREAMSTATS_QUERY);
  }

  @Test
  public void baselineEventstatsReadsEveryBatchAndReportsCorrectTotal() throws Exception {
    assertBaselineAggregate(EVENTSTATS_QUERY);
  }

  @Test
  public void deleteOnOwnerCancelsStreamstatsAndStopsExecution() throws Exception {
    assertDeleteCancelsAndStops(STREAMSTATS_QUERY, owner);
  }

  @Test
  public void deleteOnPeerForwardsCancellationOfEventstatsAndStopsExecution() throws Exception {
    assertDeleteCancelsAndStops(EVENTSTATS_QUERY, peer);
  }

  @Test
  public void concurrentDeletesCancelOnceAndRemoveOnce() throws Exception {
    long searchesBefore = indexSearchCount(owner);
    String queryId = submitAsync(STREAMSTATS_QUERY);
    AsyncQueryFixture.RunningSnapshot running = awaitRunning(owner, ownerNodeId, searchesBefore);
    long searchesAtDelete = indexSearchCount(owner);

    CountDownLatch start = new CountDownLatch(1);
    ExecutorService executor = Executors.newFixedThreadPool(2);
    List<Response> responses = new ArrayList<>();
    try {
      Future<Response> onOwner = executor.submit(() -> deleteWhenStarted(start, owner, queryId));
      Future<Response> onPeer = executor.submit(() -> deleteWhenStarted(start, peer, queryId));
      start.countDown();
      responses.add(onOwner.get(10, TimeUnit.SECONDS));
      responses.add(onPeer.get(10, TimeUnit.SECONDS));
    } finally {
      executor.shutdownNow();
    }

    List<Integer> codes = new ArrayList<>();
    for (Response response : responses) {
      int code = response.getStatusLine().getStatusCode();
      codes.add(code);
      if (code == 200) {
        assertAcknowledged(response, "CANCELLED");
      }
    }
    codes.sort(null);
    Assert.assertEquals(List.of(200, 404), codes);
    assertStopped(owner, ownerNodeId, searchesAtDelete, running.pits);
    assertNotFound(() -> getAsyncQuery(owner, queryId));
  }

  @Test
  public void deleteOfFailedJobReturnsFailedAndRemovesIt() throws Exception {
    // Scalar overflow in a post-window eval surfaces as a terminal FAILED status: eventstats drains
    // the full scan, then the +1 overflow on a bigint column fails at projection time. Fallback is
    // off so the failure isn't swallowed into a legacy restart.
    setClusterSetting(FALLBACK_ALLOWED, "false");

    JSONObject body = new JSONObject();
    body.put("query", OVERFLOW_QUERY);
    body.put("wait_for_completion_timeout", "0");
    String queryId = new JSONObject(postPpl(owner, body)).getString("id");

    JSONObject terminal = pollUntilTerminal(owner, queryId, 30_000);
    Assert.assertEquals("FAILED", terminal.getString("status"));
    Assert.assertTrue("FAILED body must carry an error: " + terminal, terminal.has("error"));

    // Preserve forwarding coverage for the terminal-FAILED delete path by sending DELETE through
    // the non-owner node; the service-level removal and acknowledgement contract is the same.
    assertAcknowledged(deleteAsyncQuery(peer, queryId), "FAILED");
    assertNotFound(() -> getAsyncQuery(owner, queryId));
    assertNotFound(() -> deleteAsyncQuery(peer, queryId));
  }

  private void assertBaselineAggregate(String query) throws Exception {
    long before = indexSearchCount(owner);
    JSONObject body = new JSONObject();
    body.put("query", query);
    // Allow enough wait for the inline result; async submit still returns ID + RUNNING for a slow
    // run, so handle both shapes.
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
        query + " must aggregate across the full scan to " + DOCS, DOCS, row.getInt(0));
    long after = indexSearchCount(owner);
    Assert.assertTrue(
        "baseline scan must read at least "
            + BASELINE_MIN_SEARCHES
            + " batches: before="
            + before
            + " after="
            + after,
        after - before >= BASELINE_MIN_SEARCHES);
    awaitPoolsIdle(owner, ownerNodeId);
  }

  private void assertDeleteCancelsAndStops(String query, RestClient deleteNode) throws Exception {
    long searchesBefore = indexSearchCount(owner);
    String queryId = submitAsync(query);
    AsyncQueryFixture.RunningSnapshot running = awaitRunning(owner, ownerNodeId, searchesBefore);
    Assert.assertEquals(
        "RUNNING", new JSONObject(getAsyncQuery(owner, queryId)).getString("status"));
    long searchesAtDelete = indexSearchCount(owner);

    assertAcknowledged(deleteAsyncQuery(deleteNode, queryId), "CANCELLED");
    assertStopped(owner, ownerNodeId, searchesAtDelete, running.pits);

    assertNotFound(() -> getAsyncQuery(owner, queryId));
    assertNotFound(() -> getAsyncQuery(deleteNode, queryId));
    assertNotFound(() -> deleteAsyncQuery(deleteNode, queryId));
  }

  private String submitAsync(String query) throws Exception {
    JSONObject body = new JSONObject();
    body.put("query", query);
    body.put("wait_for_completion_timeout", "0");
    return new JSONObject(postPpl(owner, body)).getString("id");
  }

  private static Response deleteWhenStarted(CountDownLatch start, RestClient node, String queryId)
      throws Exception {
    start.await();
    try {
      return deleteAsyncQuery(node, queryId);
    } catch (ResponseException e) {
      return e.getResponse();
    }
  }

  private static void assertAcknowledged(Response response, String status) throws IOException {
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    Assert.assertEquals(
        status, new JSONObject(getResponseBody(response, true)).getString("status"));
  }

  private RestClient nodeClient(HttpHost host) {
    try {
      return buildClient(Settings.EMPTY, new HttpHost[] {host});
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  private static void setClusterSetting(String name, String value) throws IOException {
    updateClusterSettings(new SQLIntegTestCase.ClusterSetting("persistent", name, value));
  }
}
