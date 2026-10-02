/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.opensearch.sql.legacy.TestUtils.getResponseBody;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;
import org.apache.hc.core5.http.HttpHost;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Assert;
import org.opensearch.client.Request;
import org.opensearch.client.RestClient;

/**
 * Client for the async query test plugin installed in the dedicated async test clusters. The plugin
 * holds the first search of one PPL query on {@link #INDEX} and reports, per node, whether the
 * query's task was cancelled, how many searches it issued, which point-in-time contexts it opened,
 * and how busy the SQL thread pools are.
 */
public final class AsyncQueryFixture {

  /** Six documents read one per search: an uncancelled scan issues at least {@link #DOCS}. */
  public static final String INDEX = "async_query_fixture";

  public static final int DOCS = 6;

  public static final String QUERY = "source=" + INDEX + " | fields n";

  /** The held batch plus the batch prefetched while it is processed. */
  public static final int MAX_SEARCHES_AFTER_CANCEL = 2;

  private static final String BASE = "/_plugins/_async_query_fixture/";
  private static final long WAIT_MILLIS = 10_000;

  private AsyncQueryFixture() {}

  /** Creates {@link #INDEX} with {@code index.max_result_window=1} unless it already exists. */
  public static void createIndex(RestClient admin) throws IOException {
    // The low-level client reports a missing index on HEAD as a 404 response, not an exception.
    if (admin.performRequest(new Request("HEAD", "/" + INDEX)).getStatusLine().getStatusCode()
        == 200) {
      return;
    }
    Request create = new Request("PUT", "/" + INDEX);
    create.setJsonEntity(
        "{\"settings\":{\"number_of_shards\":1,\"number_of_replicas\":0,"
            + "\"max_result_window\":1},"
            + "\"mappings\":{\"properties\":{\"n\":{\"type\":\"integer\"}}}}");
    admin.performRequest(create);
    StringBuilder bulk = new StringBuilder();
    for (int n = 1; n <= DOCS; n++) {
      bulk.append("{\"index\":{}}\n{\"n\":").append(n).append("}\n");
    }
    Request load = new Request("POST", "/" + INDEX + "/_bulk?refresh=true");
    load.setJsonEntity(bulk.toString());
    admin.performRequest(load);
  }

  /** Observes the next PPL query on {@link #INDEX} at {@code node} and holds its first search. */
  public static void arm(RestClient node) throws IOException {
    call(node, "POST", "arm?index=" + INDEX);
  }

  public static void release(RestClient node, boolean fail) throws IOException {
    call(node, "POST", "release?fail=" + fail);
  }

  public static void disarm(RestClient node) throws IOException {
    call(node, "POST", "disarm");
  }

  public static JSONObject status(RestClient node) throws IOException {
    return call(node, "GET", "status");
  }

  /** Waits until the first search is held while the query occupies a SQL worker. */
  public static JSONObject awaitHeld(RestClient node) throws Exception {
    return await(
        node,
        status -> status.getBoolean("held") && active(status, "sql-worker") > 0,
        "first search held on a busy sql-worker");
  }

  /** Waits until nothing is held and every SQL pool on {@code node} is idle. */
  public static JSONObject awaitIdle(RestClient node) throws Exception {
    return await(
        node,
        status -> {
          if (status.getBoolean("held")) {
            return false;
          }
          JSONObject active = status.getJSONObject("active");
          for (String pool : active.keySet()) {
            if (active.getInt(pool) > 0) {
              return false;
            }
          }
          return true;
        },
        "idle SQL pools");
  }

  /**
   * Asserts that the cancelled query on {@code owner} stopped: its task was cancelled, the SQL
   * pools went idle without further searches, and every PIT it opened was closed.
   */
  public static void assertStopped(RestClient owner) throws Exception {
    JSONObject idle = awaitIdle(owner);
    Assert.assertTrue(
        "cancelled query must not keep reading batches: " + idle,
        idle.getInt("searches") <= MAX_SEARCHES_AFTER_CANCEL);
    Assert.assertTrue("query task must be cancelled: " + idle, idle.getBoolean("task_cancelled"));
    assertPitsClosed(owner, idle);
  }

  /** Asserts that none of the PITs recorded in {@code status} is still open in the cluster. */
  public static void assertPitsClosed(RestClient admin, JSONObject status) throws IOException {
    JSONArray tracked = status.getJSONArray("pits");
    Assert.assertTrue("query must have opened a PIT: " + status, tracked.length() > 0);
    List<String> open = new ArrayList<>();
    JSONArray pits =
        new JSONObject(
                getResponseBody(
                    admin.performRequest(new Request("GET", "/_search/point_in_time/_all")), true))
            .getJSONArray("pits");
    for (int i = 0; i < pits.length(); i++) {
      open.add(pits.getJSONObject(i).getString("pit_id"));
    }
    for (int i = 0; i < tracked.length(); i++) {
      Assert.assertFalse(
          "PIT left open by the query: " + tracked.getString(i),
          open.contains(tracked.getString(i)));
    }
  }

  /**
   * Returns clients pinned to two distinct nodes. Test clusters bind each node to several
   * addresses, so hosts are deduplicated by the node id they report.
   */
  public static RestClient[] twoNodeClients(
      List<HttpHost> hosts, Function<HttpHost, RestClient> builder) throws IOException {
    RestClient first = builder.apply(hosts.get(0));
    String firstId = localNodeId(first);
    for (int i = 1; i < hosts.size(); i++) {
      RestClient candidate = builder.apply(hosts.get(i));
      if (!localNodeId(candidate).equals(firstId)) {
        return new RestClient[] {first, candidate};
      }
      candidate.close();
    }
    first.close();
    throw new AssertionError("async fixture tests need two distinct nodes: " + hosts);
  }

  public static String localNodeId(RestClient client) throws IOException {
    JSONObject body =
        new JSONObject(
            getResponseBody(client.performRequest(new Request("GET", "/_nodes/_local")), true));
    return body.getJSONObject("nodes").keys().next();
  }

  private static int active(JSONObject status, String pool) {
    JSONObject active = status.getJSONObject("active");
    return active.has(pool) ? active.getInt(pool) : 0;
  }

  private static JSONObject await(
      RestClient node, Predicate<JSONObject> condition, String description) throws Exception {
    long deadline = System.currentTimeMillis() + WAIT_MILLIS;
    JSONObject last = status(node);
    while (!condition.test(last)) {
      if (System.currentTimeMillis() > deadline) {
        Assert.fail("timed out waiting for " + description + "; last status " + last);
      }
      Thread.sleep(50);
      last = status(node);
    }
    return last;
  }

  private static JSONObject call(RestClient node, String method, String endpoint)
      throws IOException {
    return new JSONObject(
        getResponseBody(node.performRequest(new Request(method, BASE + endpoint)), true));
  }
}
