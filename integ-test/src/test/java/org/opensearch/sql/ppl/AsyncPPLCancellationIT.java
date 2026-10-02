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
import static org.opensearch.sql.ppl.AsyncQueryFixture.DOCS;
import static org.opensearch.sql.ppl.AsyncQueryFixture.QUERY;
import static org.opensearch.sql.ppl.AsyncQueryFixture.assertPitsClosed;
import static org.opensearch.sql.ppl.AsyncQueryFixture.assertStopped;
import static org.opensearch.sql.ppl.AsyncQueryFixture.awaitHeld;
import static org.opensearch.sql.ppl.AsyncQueryFixture.awaitIdle;
import static org.opensearch.sql.ppl.AsyncQueryFixture.status;
import static org.opensearch.sql.util.MatcherUtils.verifyNumOfRows;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.hc.core5.http.HttpHost;
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
 * Cancels real running Calcite async queries and observes that execution stops. Runs on the
 * two-node {@code asyncMultiNodeIT} cluster, whose async query test plugin holds the query's first
 * search so DELETE always reaches a running job. Fallback to the legacy engine stays enabled so
 * cancellation must not restart the query there.
 */
public class AsyncPPLCancellationIT extends PPLIntegTestCase {

  private static final String FALLBACK_ALLOWED = "plugins.calcite.fallback.allowed";

  private RestClient owner;
  private RestClient peer;

  @Override
  protected void init() throws Exception {
    super.init();
    enableCalcite();
    setFallbackAllowed("true");
    RestClient[] nodes = AsyncQueryFixture.twoNodeClients(getClusterHosts(), this::nodeClient);
    owner = nodes[0];
    peer = nodes[1];
    AsyncQueryFixture.createIndex(client());
  }

  @After
  public void tearDownFixture() throws Exception {
    if (owner != null) {
      AsyncQueryFixture.disarm(owner);
      // Let execution left over from a failed assertion finish before the next test arms.
      awaitIdle(owner);
      owner.close();
    }
    if (peer != null) {
      peer.close();
    }
    setFallbackAllowed(null);
  }

  @Test
  public void uncancelledQueryReadsEveryBatch() throws Exception {
    String queryId = submitHeldQuery();
    AsyncQueryFixture.release(owner, false);

    JSONObject result = pollUntilTerminal(owner, queryId, 30_000);
    Assert.assertEquals("SUCCEEDED", result.getString("status"));
    verifyNumOfRows(result, DOCS);
    JSONObject idle = awaitIdle(owner);
    Assert.assertFalse(idle.getBoolean("task_cancelled"));
    Assert.assertTrue(
        "baseline scan must read every batch: " + idle, idle.getInt("searches") >= DOCS);
    assertPitsClosed(owner, idle);
  }

  @Test
  public void deleteOnOwnerCancelsRunningQueryAndStopsExecution() throws Exception {
    assertDeleteCancelsAndStops(owner);
  }

  @Test
  public void deleteOnPeerForwardsCancellationAndStopsExecution() throws Exception {
    assertDeleteCancelsAndStops(peer);
  }

  @Test
  public void deleteOfFailedQueryReturnsFailedAndRemovesIt() throws Exception {
    // A fetch failure falls back to the legacy engine when fallback is allowed.
    setFallbackAllowed(null);
    String queryId = submitHeldQuery();
    AsyncQueryFixture.release(owner, true);
    Assert.assertEquals("FAILED", pollUntilTerminal(owner, queryId, 30_000).getString("status"));
    awaitIdle(owner);

    assertAcknowledged(deleteAsyncQuery(peer, queryId), "FAILED");
    assertNotFound(() -> getAsyncQuery(owner, queryId));
    assertNotFound(() -> deleteAsyncQuery(peer, queryId));
  }

  @Test
  public void concurrentDeletesCancelOnceAndRemoveOnce() throws Exception {
    String queryId = submitHeldQuery();
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
    AsyncQueryFixture.release(owner, false);
    assertStopped(owner);
  }

  private void assertDeleteCancelsAndStops(RestClient deleteNode) throws Exception {
    String queryId = submitHeldQuery();
    Assert.assertEquals(
        "RUNNING", new JSONObject(getAsyncQuery(owner, queryId)).getString("status"));

    assertAcknowledged(deleteAsyncQuery(deleteNode, queryId), "CANCELLED");
    JSONObject beforeRelease = status(owner);

    AsyncQueryFixture.release(owner, false);
    assertStopped(owner);
    Assert.assertTrue(
        "DELETE must cancel the task before the held search resumes: " + beforeRelease,
        beforeRelease.getBoolean("task_cancelled"));
    assertNotFound(() -> getAsyncQuery(owner, queryId));
    assertNotFound(() -> getAsyncQuery(deleteNode, queryId));
    assertNotFound(() -> deleteAsyncQuery(deleteNode, queryId));
  }

  /** Submits {@link AsyncQueryFixture#QUERY} on the owner and waits until its search is held. */
  private String submitHeldQuery() throws Exception {
    AsyncQueryFixture.arm(owner);
    JSONObject body = new JSONObject();
    body.put("query", QUERY);
    body.put("wait_for_completion_timeout", "0");
    String queryId = new JSONObject(postPpl(owner, body)).getString("id");
    awaitHeld(owner);
    return queryId;
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

  private static void setFallbackAllowed(String value) throws IOException {
    updateClusterSettings(
        new SQLIntegTestCase.ClusterSetting("persistent", FALLBACK_ALLOWED, value));
  }
}
