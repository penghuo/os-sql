/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.security;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.opensearch.sql.legacy.TestUtils.getResponseBody;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;
import static org.opensearch.sql.plugin.transport.PPLAsyncDeleteAction.NAME;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.hc.core5.http.HttpHost;
import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.client.RestClient;
import org.opensearch.sql.plugin.transport.PPLAsyncGetResultAction;
import org.opensearch.sql.util.ClusterPlugins;

/**
 * Multi-node integration coverage for asynchronous job routing and forwarded FGAC context.
 *
 * <p>The dedicated three-node test cluster is intentionally reduced to two nodes at the end of the
 * single test to verify owner loss without affecting other integration suites.
 */
public class PPLAsyncQueryMultiNodeIT extends SecurityTestBase {

  private static final String OWNER_USER = "ppl_async_multinode_owner";
  private static final String OWNER_ROLE = "ppl_async_multinode_owner_role";
  private static final String OTHER_USER = "ppl_async_multinode_other";
  private static final String OTHER_ROLE = "ppl_async_multinode_other_role";
  private static final String ADMIN_USER = "admin";
  private static final String ADMIN_PASSWORD = "admin";
  private static final String SUBMIT_ACTION = "cluster:admin/opensearch/ppl";
  private static final String GET_ACTION = PPLAsyncGetResultAction.NAME;
  private static final String DELETE_ACTION = NAME;
  private static final String[] INDEX_PERMISSIONS = {
    "indices:data/read/search*",
    "indices:admin/mappings/get",
    "indices:monitor/settings/get",
    "indices:data/read/point_in_time/create",
    "indices:data/read/point_in_time/delete",
    "indices:admin/resolve/index",
    "indices:data/read/field_caps*"
  };

  @Override
  protected void init() throws Exception {
    ClusterPlugins.requirePluginOrAssume(
        client(),
        ClusterPlugins.SECURITY_PLUGIN,
        "opensearch-security plugin not installed on test cluster");
    super.init();
    enableCalcite();
    loadIndex(Index.BANK);
    createRoleAndUser(OWNER_ROLE, OWNER_USER);
    createRoleAndUser(OTHER_ROLE, OTHER_USER);
  }

  @Test
  public void routesAcrossNodesPreservesAuthorizationAndHandlesOwnerDeparture() throws Exception {
    List<NodeEndpoint> nodes = directNodeClients();
    ExecutorService executor = Executors.newFixedThreadPool(8);
    try {
      assertEquals("The test requires exactly three nodes", 3, nodes.size());
      String clusterManagerId = clusterManagerId(nodes.getFirst().client);
      NodeEndpoint owner =
          nodes.stream()
              .filter(node -> !node.id.equals(clusterManagerId))
              .findFirst()
              .orElse(nodes.getFirst());
      List<NodeEndpoint> gateways = nodes.stream().filter(node -> node != owner).toList();
      NodeEndpoint getGateway = gateways.get(0);
      NodeEndpoint deleteGateway = gateways.get(1);

      String routedJob = submitForId(owner.client, OWNER_USER);
      assertEquals(200, status(getGateway.client, "GET", jobEndpoint(routedJob), null, OWNER_USER));
      assertEquals(403, status(getGateway.client, "GET", jobEndpoint(routedJob), null, OTHER_USER));
      assertEquals(
          403, status(deleteGateway.client, "DELETE", jobEndpoint(routedJob), null, OTHER_USER));

      assertAuthorizationRace(routedJob, getGateway, deleteGateway, executor);

      JSONObject deleted =
          jsonRequest(deleteGateway.client, "DELETE", jobEndpoint(routedJob), null, OWNER_USER);
      assertTrue(
          deleted.getString("status").equals("CANCELLED")
              || deleted.getString("status").equals("SUCCEEDED"));
      assertEquals(404, status(getGateway.client, "GET", jobEndpoint(routedJob), null, OWNER_USER));

      String ownerLostJob = submitForId(owner.client, OWNER_USER);
      assertEquals(
          200, status(getGateway.client, "GET", jobEndpoint(ownerLostJob), null, OWNER_USER));

      CountDownLatch forwardingStarted = new CountDownLatch(1);
      Future<List<Integer>> forwardingStatuses =
          executor.submit(
              () ->
                  pollWhileOwnerDeparts(
                      deleteGateway.client, ownerLostJob, forwardingStarted, OWNER_USER));
      assertTrue(forwardingStarted.await(10, TimeUnit.SECONDS));
      stopOwnerProcess(owner);

      List<Integer> statuses = forwardingStatuses.get(45, TimeUnit.SECONDS);
      assertFalse(statuses.isEmpty());
      assertTrue(statuses.stream().allMatch(status -> status == 200 || status == 404));
      assertTrue("Forwarding must converge to not found after owner loss", statuses.contains(404));
      assertEquals(2, waitForNodeCount(getGateway.client, 2));
      assertEquals(
          404, status(getGateway.client, "GET", jobEndpoint(ownerLostJob), null, OWNER_USER));
    } finally {
      executor.shutdownNow();
      for (NodeEndpoint node : nodes) {
        node.close();
      }
    }
  }

  private void assertAuthorizationRace(
      String id, NodeEndpoint getGateway, NodeEndpoint deleteGateway, ExecutorService executor)
      throws Exception {
    CountDownLatch start = new CountDownLatch(1);
    List<ExpectedStatus> requests = new ArrayList<>();
    for (int attempt = 0; attempt < 8; attempt++) {
      requests.add(
          new ExpectedStatus(
              200,
              executor.submit(
                  () -> {
                    start.await();
                    return status(getGateway.client, "GET", jobEndpoint(id), null, OWNER_USER);
                  })));
      requests.add(
          new ExpectedStatus(
              403,
              executor.submit(
                  () -> {
                    start.await();
                    return status(
                        deleteGateway.client, "DELETE", jobEndpoint(id), null, OTHER_USER);
                  })));
    }
    start.countDown();
    for (ExpectedStatus request : requests) {
      assertEquals(request.expected, (int) request.result.get(30, TimeUnit.SECONDS));
    }
    assertEquals(200, status(getGateway.client, "GET", jobEndpoint(id), null, OWNER_USER));
  }

  private List<Integer> pollWhileOwnerDeparts(
      RestClient gateway, String id, CountDownLatch started, String user) throws Exception {
    List<Integer> statuses = new ArrayList<>();
    started.countDown();
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(40);
    while (System.nanoTime() < deadline) {
      int status = status(gateway, "GET", jobEndpoint(id), null, user);
      statuses.add(status);
      if (status == 404) {
        return statuses;
      }
      Thread.sleep(25);
    }
    return statuses;
  }

  private void stopOwnerProcess(NodeEndpoint owner) throws Exception {
    ProcessHandle process =
        ProcessHandle.of(owner.pid)
            .orElseThrow(() -> new AssertionError("Owner process is not available: " + owner.id));
    assertTrue("Owner process must be alive before departure", process.isAlive());
    assertTrue("Failed to request owner-node shutdown", process.destroy());
    process.onExit().get(30, TimeUnit.SECONDS);
  }

  private int waitForNodeCount(RestClient client, int expected) throws Exception {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
    int actual = -1;
    while (System.nanoTime() < deadline) {
      JSONObject health =
          jsonRequest(client, "GET", "/_cluster/health", null, ADMIN_USER, ADMIN_PASSWORD);
      actual = health.getInt("number_of_nodes");
      if (actual == expected) {
        return actual;
      }
      Thread.sleep(100);
    }
    return actual;
  }

  private List<NodeEndpoint> directNodeClients() throws IOException {
    String cluster = System.getProperty("tests.rest.cluster");
    assertNotNull("tests.rest.cluster is required", cluster);
    List<NodeEndpoint> nodes = new ArrayList<>();
    try {
      for (String address : cluster.split(",")) {
        RestClient direct = RestClient.builder(parseHost(address)).build();
        JSONObject nodeInfo =
            jsonRequest(direct, "GET", "/_nodes/_local/process", null, ADMIN_USER, ADMIN_PASSWORD);
        JSONObject nodeMap = nodeInfo.getJSONObject("nodes");
        String nodeId = nodeMap.keys().next();
        long pid = nodeMap.getJSONObject(nodeId).getJSONObject("process").getLong("id");
        if (nodes.stream().anyMatch(node -> node.id.equals(nodeId))) {
          direct.close();
        } else {
          nodes.add(new NodeEndpoint(nodeId, pid, direct));
        }
      }
      return nodes;
    } catch (IOException | RuntimeException e) {
      for (NodeEndpoint node : nodes) {
        try {
          node.close();
        } catch (IOException closeFailure) {
          e.addSuppressed(closeFailure);
        }
      }
      throw e;
    }
  }

  private String clusterManagerId(RestClient client) throws IOException {
    return jsonRequest(
            client, "GET", "/_cluster/state/master_node", null, ADMIN_USER, ADMIN_PASSWORD)
        .getString("master_node");
  }

  private String submitForId(RestClient client, String user) throws IOException {
    String query =
        String.format(
            Locale.ROOT,
            "source=%s | sort account_number | fields account_number, firstname",
            TEST_INDEX_BANK);
    JSONObject response =
        jsonRequest(
            client,
            "POST",
            "/_plugins/_ppl",
            new JSONObject()
                .put("query", query)
                .put("wait_for_completion_timeout", "0s")
                .put("keep_alive", "5m")
                .toString(),
            user);
    assertEquals("RUNNING", response.getString("status"));
    assertTrue(response.has("id"));
    return response.getString("id");
  }

  private void createRoleAndUser(String role, String user) throws IOException {
    createRoleWithPermissions(
        role,
        TEST_INDEX_BANK,
        new String[] {SUBMIT_ACTION, GET_ACTION, DELETE_ACTION},
        INDEX_PERMISSIONS);
    createUser(user, role);
  }

  private JSONObject jsonRequest(
      RestClient client, String method, String endpoint, String body, String user)
      throws IOException {
    return jsonRequest(client, method, endpoint, body, user, STRONG_PASSWORD);
  }

  private JSONObject jsonRequest(
      RestClient client, String method, String endpoint, String body, String user, String password)
      throws IOException {
    Response response = perform(client, method, endpoint, body, user, password);
    assertEquals(200, response.getStatusLine().getStatusCode());
    return new JSONObject(getResponseBody(response, true));
  }

  private int status(RestClient client, String method, String endpoint, String body, String user)
      throws IOException {
    try {
      Response response = perform(client, method, endpoint, body, user, STRONG_PASSWORD);
      int status = response.getStatusLine().getStatusCode();
      getResponseBody(response, true);
      return status;
    } catch (ResponseException e) {
      int status = e.getResponse().getStatusLine().getStatusCode();
      getResponseBody(e.getResponse(), true);
      return status;
    }
  }

  private Response perform(
      RestClient client, String method, String endpoint, String body, String user, String password)
      throws IOException {
    Request request = new Request(method, endpoint);
    if (body != null) {
      request.setJsonEntity(body);
    }
    RequestOptions.Builder options = RequestOptions.DEFAULT.toBuilder();
    options.addHeader("Content-Type", "application/json");
    options.addHeader("Authorization", createBasicAuthHeader(user, password));
    request.setOptions(options);
    return client.performRequest(request);
  }

  private static HttpHost parseHost(String address) {
    String value = address.trim();
    int scheme = value.indexOf("://");
    if (scheme >= 0) {
      value = value.substring(scheme + 3);
    }
    int portSeparator = value.lastIndexOf(':');
    if (portSeparator < 0) {
      throw new IllegalArgumentException("Invalid REST endpoint: " + address);
    }
    String host = value.substring(0, portSeparator);
    if (host.startsWith("[") && host.endsWith("]")) {
      host = host.substring(1, host.length() - 1);
    }
    return new HttpHost("http", host, Integer.parseInt(value.substring(portSeparator + 1)));
  }

  private static String jobEndpoint(String id) {
    return "/_plugins/_ppl/jobs/" + id;
  }

  private record NodeEndpoint(String id, long pid, RestClient client) implements AutoCloseable {
    @Override
    public void close() throws IOException {
      client.close();
    }
  }

  private record ExpectedStatus(int expected, Future<Integer> result) {}
}
