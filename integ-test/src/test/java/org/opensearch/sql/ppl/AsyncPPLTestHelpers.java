/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.opensearch.sql.legacy.TestUtils.getResponseBody;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.Assert;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.RestClient;

/**
 * Shared helpers for the async PPL integration tests ({@link AsyncPPLQueryLifecycleIT} and {@link
 * AsyncPPLMultiNodeRoutingIT}). Keeps the two IT classes free of duplicated boilerplate for POST /
 * GET / poll-until-terminal.
 */
final class AsyncPPLTestHelpers {

  static final String PPL_ENDPOINT = "/_plugins/_ppl";
  static final String ASYNC_QUERY_ENDPOINT = "/_plugins/_async_query/";

  private AsyncPPLTestHelpers() {}

  /** POST {@code /_plugins/_ppl} with the given JSON body; returns the raw response body. */
  static String postPpl(RestClient client, JSONObject body) throws IOException {
    return postPpl(client, PPL_ENDPOINT, body);
  }

  static String postPpl(RestClient client, String endpoint, JSONObject body) throws IOException {
    Request request = new Request("POST", endpoint);
    request.setJsonEntity(body.toString());
    Response response = client.performRequest(request);
    return getResponseBody(response, true);
  }

  /** GET {@code /_plugins/_async_query/{id}}; returns the raw response body. */
  static String getAsyncQuery(RestClient client, String queryId) throws IOException {
    Request request = new Request("GET", ASYNC_QUERY_ENDPOINT + queryId);
    Response response = client.performRequest(request);
    return getResponseBody(response, true);
  }

  /** DELETE {@code /_plugins/_async_query/{id}}; returns the HTTP status code. */
  static int deleteAsyncQuery(RestClient client, String queryId) throws IOException {
    Request request = new Request("DELETE", ASYNC_QUERY_ENDPOINT + queryId);
    return client.performRequest(request).getStatusLine().getStatusCode();
  }

  /**
   * Submits {@code body} on {@code submitNode}, cancels on {@code cancelNode}, and returns the id
   * of the first job whose cancel landed while it was still running. A fast query can finish
   * between submit and DELETE; such attempts must keep their result and are retried.
   */
  static String submitAndCancel(RestClient submitNode, RestClient cancelNode, JSONObject body)
      throws Exception {
    int attempts = 5;
    for (int i = 0; i < attempts; i++) {
      String queryId = new JSONObject(postPpl(submitNode, body)).getString("id");
      Assert.assertEquals(204, deleteAsyncQuery(cancelNode, queryId));
      String status = new JSONObject(getAsyncQuery(submitNode, queryId)).getString("status");
      if ("CANCELLED".equals(status)) {
        return queryId;
      }
      Assert.assertEquals("cancel that lost the race must keep the result", "SUCCEEDED", status);
    }
    Assert.fail("no DELETE landed on a running job in " + attempts + " attempts");
    return null; // unreachable
  }

  /** Resolves the id of the node that serves {@code client}'s requests. */
  static String localNodeId(RestClient client) throws IOException {
    Response response = client.performRequest(new Request("GET", "/_nodes/_local"));
    return new JSONObject(getResponseBody(response, true)).getJSONObject("nodes").keys().next();
  }

  /** Polls GET until the job reaches a terminal state or the timeout expires. */
  static JSONObject pollUntilTerminal(RestClient client, String queryId, long timeoutMillis)
      throws Exception {
    long deadline = System.currentTimeMillis() + timeoutMillis;
    JSONObject last = null;
    while (System.currentTimeMillis() < deadline) {
      last = new JSONObject(getAsyncQuery(client, queryId));
      String status = last.getString("status");
      if (!"RUNNING".equals(status) && !"PENDING".equals(status)) {
        return last;
      }
      Thread.sleep(200);
    }
    Assert.fail(
        "async job ["
            + queryId
            + "] did not reach terminal state within "
            + timeoutMillis
            + "ms. last="
            + last);
    return last; // unreachable
  }
}
