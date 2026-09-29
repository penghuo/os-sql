/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.rest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;
import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.rest.RestHandler.Route;
import org.opensearch.rest.RestRequest;
import org.opensearch.sql.plugin.transport.TransportPPLQueryRequest;

public class RestPPLQueryActionTest {

  @Test
  public void registersSubmitPollAndDeleteRoutes() {
    List<Route> routes = new RestPPLQueryAction().routes();

    assertTrue(
        routes.stream()
            .anyMatch(
                route ->
                    route.getMethod() == RestRequest.Method.POST
                        && route.getPath().equals(RestPPLQueryAction.QUERY_API_ENDPOINT)));
    assertTrue(
        routes.stream()
            .anyMatch(
                route ->
                    route.getMethod() == RestRequest.Method.GET
                        && route.getPath().equals(RestPPLQueryAction.ASYNC_JOB_API_ENDPOINT)));
    assertTrue(
        routes.stream()
            .anyMatch(
                route ->
                    route.getMethod() == RestRequest.Method.DELETE
                        && route.getPath().equals(RestPPLQueryAction.ASYNC_JOB_API_ENDPOINT)));
    assertEquals(4, routes.size());
  }

  @Test
  public void asyncModeRequiresOneOfTheTwoLifecycleFields() {
    assertTrue(
        request(new JSONObject().put("query", "source=logs").put("keep_alive", "5m"))
            .isAsyncQueryRequest());
    assertTrue(
        request(
                new JSONObject()
                    .put("query", "source=logs")
                    .put("wait_for_completion_timeout", "1s"))
            .isAsyncQueryRequest());
    assertTrue(!request(new JSONObject().put("query", "source=logs")).isAsyncQueryRequest());
  }

  private static TransportPPLQueryRequest request(JSONObject body) {
    return new TransportPPLQueryRequest(
        body.getString("query"), body, RestPPLQueryAction.QUERY_API_ENDPOINT);
  }
}
