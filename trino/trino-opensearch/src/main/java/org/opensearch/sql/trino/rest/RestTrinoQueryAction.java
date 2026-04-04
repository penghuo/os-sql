/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.trino.rest;

import static org.opensearch.core.rest.RestStatus.NOT_FOUND;
import static org.opensearch.core.rest.RestStatus.OK;

import java.util.List;
import java.util.UUID;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.transport.client.node.NodeClient;

/**
 * REST handler implementing the Trino client protocol endpoints. For Task 3, this returns STUB
 * responses (no real query execution).
 *
 * <ul>
 *   <li>POST /_plugins/_trino_sql/v1/statement -- submit query
 *   <li>GET /_plugins/_trino_sql/v1/statement/{queryId}/{token} -- fetch results
 *   <li>DELETE /_plugins/_trino_sql/v1/statement/{queryId} -- cancel query
 * </ul>
 */
public class RestTrinoQueryAction extends BaseRestHandler {

  public static final String STATEMENT_API_ENDPOINT = "/_plugins/_trino_sql/v1/statement";

  private static final Logger LOG = LogManager.getLogger(RestTrinoQueryAction.class);

  public RestTrinoQueryAction() {
    super();
  }

  @Override
  public String getName() {
    return "trino_query_action";
  }

  @Override
  public List<Route> routes() {
    return List.of(
        new Route(RestRequest.Method.POST, STATEMENT_API_ENDPOINT),
        new Route(
            RestRequest.Method.GET, STATEMENT_API_ENDPOINT + "/{queryId}/{token}"),
        new Route(RestRequest.Method.DELETE, STATEMENT_API_ENDPOINT + "/{queryId}"));
  }

  @Override
  protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
    return switch (request.method()) {
      case POST -> handlePost(request);
      case GET -> handleGet(request);
      case DELETE -> handleDelete(request);
      default -> channel ->
          channel.sendResponse(
              new BytesRestResponse(RestStatus.METHOD_NOT_ALLOWED, "Method not allowed"));
    };
  }

  /**
   * POST /_plugins/_trino_sql/v1/statement Submit a query. Returns a stub FINISHED response with
   * empty data.
   */
  private RestChannelConsumer handlePost(RestRequest request) {
    String query = request.content().utf8ToString();
    String queryId = UUID.randomUUID().toString().replace("-", "");
    LOG.debug("Trino stub: received query [{}], assigned id [{}]", query, queryId);

    String responseBody =
        "{"
            + "\"id\":\""
            + queryId
            + "\","
            + "\"infoUri\":\""
            + STATEMENT_API_ENDPOINT
            + "/"
            + queryId
            + "\","
            + "\"stats\":{"
            + "\"state\":\"FINISHED\","
            + "\"queued\":false,"
            + "\"scheduled\":true,"
            + "\"nodes\":1,"
            + "\"totalSplits\":0,"
            + "\"queuedSplits\":0,"
            + "\"runningSplits\":0,"
            + "\"completedSplits\":0,"
            + "\"cpuTimeMillis\":0,"
            + "\"wallTimeMillis\":0,"
            + "\"queuedTimeMillis\":0,"
            + "\"elapsedTimeMillis\":0,"
            + "\"processedRows\":0,"
            + "\"processedBytes\":0,"
            + "\"peakMemoryBytes\":0"
            + "}"
            + "}";

    return channel -> channel.sendResponse(new BytesRestResponse(OK, "application/json", responseBody));
  }

  /** GET /_plugins/_trino_sql/v1/statement/{queryId}/{token} Fetch next results. Stub: 404. */
  private RestChannelConsumer handleGet(RestRequest request) {
    String queryId = request.param("queryId");
    String token = request.param("token");
    LOG.debug("Trino stub: GET results for query [{}], token [{}]", queryId, token);

    String responseBody = "{\"error\":{\"message\":\"Query not found\",\"errorCode\":1}}";
    return channel -> channel.sendResponse(new BytesRestResponse(NOT_FOUND, "application/json", responseBody));
  }

  /** DELETE /_plugins/_trino_sql/v1/statement/{queryId} Cancel query. Stub: 204. */
  private RestChannelConsumer handleDelete(RestRequest request) {
    String queryId = request.param("queryId");
    LOG.debug("Trino stub: DELETE query [{}]", queryId);

    return channel ->
        channel.sendResponse(new BytesRestResponse(RestStatus.NO_CONTENT, "application/json", ""));
  }
}
