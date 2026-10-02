/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.security;

import static org.opensearch.sql.legacy.TestUtils.getResponseBody;
import static org.opensearch.sql.ppl.AsyncQueryFixture.INDEX;
import static org.opensearch.sql.ppl.AsyncQueryFixture.QUERY;
import static org.opensearch.sql.ppl.AsyncQueryFixture.assertStopped;
import static org.opensearch.sql.ppl.AsyncQueryFixture.awaitHeld;
import static org.opensearch.sql.ppl.AsyncQueryFixture.status;

import java.io.IOException;
import org.apache.hc.core5.http.HttpHost;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.client.RestClient;
import org.opensearch.common.settings.Settings;
import org.opensearch.sql.job.QueryJobId;
import org.opensearch.sql.legacy.SQLIntegTestCase;
import org.opensearch.sql.ppl.AsyncQueryFixture;

/**
 * Async PPL ownership checks on the two-node secured {@code asyncSecurityMultiNodeIT} cluster. Both
 * users hold every async permission, so a 403 can only come from job ownership, and the requests
 * are sent to the non-owner node to exercise owner forwarding.
 */
public class AsyncPPLSecurityIT extends SecurityTestBase {

  private static final String ALICE = "async_alice";
  private static final String BOB = "async_bob";
  private static final String ASYNC_PATH = "/_plugins/_async_query/";

  private RestClient owner;
  private RestClient peer;

  @Override
  protected void init() throws Exception {
    super.init();
    enableCalcite();
    updateClusterSettings(
        new SQLIntegTestCase.ClusterSetting(
            "persistent", "plugins.calcite.fallback.allowed", "true"));
    AsyncQueryFixture.createIndex(client());
    createAsyncUser(ALICE);
    createAsyncUser(BOB);
    RestClient[] nodes = AsyncQueryFixture.twoNodeClients(getClusterHosts(), this::nodeClient);
    owner = nodes[0];
    peer = nodes[1];
  }

  @After
  public void tearDownFixture() throws Exception {
    if (owner != null) {
      AsyncQueryFixture.disarm(owner);
      AsyncQueryFixture.awaitIdle(owner);
      owner.close();
    }
    if (peer != null) {
      peer.close();
    }
    updateClusterSettings(
        new SQLIntegTestCase.ClusterSetting(
            "persistent", "plugins.calcite.fallback.allowed", null));
  }

  @Test
  public void otherUserCannotDeleteThroughForwardingAndOwnerStillCan() throws Exception {
    // Bob's forwarded GET and DELETE reach the owner's job service: a missing job is a 404, so a
    // later 403 can only come from ownership.
    String unknownId = QueryJobId.create(AsyncQueryFixture.localNodeId(owner)).encode();
    assertNotFound(() -> asUser(peer, "DELETE", ASYNC_PATH + unknownId, BOB, null));
    assertNotFound(() -> asUser(peer, "GET", ASYNC_PATH + unknownId, BOB, null));

    AsyncQueryFixture.arm(owner);
    JSONObject body = new JSONObject();
    body.put("query", QUERY);
    body.put("wait_for_completion_timeout", "0");
    String queryId =
        new JSONObject(asUser(owner, "POST", "/_plugins/_ppl", ALICE, body)).getString("id");
    awaitHeld(owner);

    assertForbidden(() -> asUser(peer, "DELETE", ASYNC_PATH + queryId, BOB, null));
    assertForbidden(() -> asUser(peer, "GET", ASYNC_PATH + queryId, BOB, null));
    JSONObject afterForbidden = status(owner);
    Assert.assertFalse(
        "another user's DELETE must not cancel: " + afterForbidden,
        afterForbidden.getBoolean("task_cancelled"));
    Assert.assertTrue(afterForbidden.getBoolean("held"));
    Assert.assertEquals(
        "RUNNING",
        new JSONObject(asUser(peer, "GET", ASYNC_PATH + queryId, ALICE, null)).getString("status"));

    JSONObject deleted = new JSONObject(asUser(peer, "DELETE", ASYNC_PATH + queryId, ALICE, null));
    Assert.assertEquals("CANCELLED", deleted.getString("status"));
    AsyncQueryFixture.release(owner, false);
    assertStopped(owner);
    assertNotFound(() -> asUser(owner, "GET", ASYNC_PATH + queryId, ALICE, null));
  }

  private void createAsyncUser(String user) throws IOException {
    String role = user + "_role";
    createRoleWithPermissions(
        role,
        INDEX,
        new String[] {
          "cluster:admin/opensearch/ppl",
          "cluster:admin/opensearch/ql/async_query/result",
          "cluster:admin/opensearch/ql/async_query/delete"
        },
        new String[] {
          "indices:data/read/search*",
          "indices:admin/mappings/get",
          "indices:monitor/settings/get",
          "indices:data/read/point_in_time/create",
          "indices:data/read/point_in_time/delete",
          "indices:admin/resolve/index",
          "indices:data/read/field_caps*"
        });
    createUser(user, role);
  }

  private String asUser(RestClient node, String method, String path, String user, JSONObject body)
      throws IOException {
    Request request = new Request(method, path);
    if (body != null) {
      request.setJsonEntity(body.toString());
    }
    RequestOptions.Builder options = RequestOptions.DEFAULT.toBuilder();
    options.addHeader("Authorization", createBasicAuthHeader(user, STRONG_PASSWORD));
    request.setOptions(options);
    Response response = node.performRequest(request);
    return getResponseBody(response, true);
  }

  private static void assertForbidden(org.junit.function.ThrowingRunnable request) {
    ResponseException e = Assert.assertThrows(ResponseException.class, request);
    Assert.assertEquals(403, e.getResponse().getStatusLine().getStatusCode());
  }

  private static void assertNotFound(org.junit.function.ThrowingRunnable request) {
    ResponseException e = Assert.assertThrows(ResponseException.class, request);
    Assert.assertEquals(404, e.getResponse().getStatusLine().getStatusCode());
  }

  private RestClient nodeClient(HttpHost host) {
    try {
      return buildClient(Settings.EMPTY, new HttpHost[] {host});
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }
}
