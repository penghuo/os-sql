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
import org.opensearch.sql.trino.bootstrap.TrinoEngine;
import org.opensearch.transport.client.node.NodeClient;

/**
 * REST handler implementing the Trino client protocol endpoints.
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

  private final TrinoEngine engine;

  public RestTrinoQueryAction() {
    this(null);
  }

  public RestTrinoQueryAction(TrinoEngine engine) {
    super();
    this.engine = engine;
  }

  @Override
  public String getName() {
    return "trino_query_action";
  }

  @Override
  public List<Route> routes() {
    return List.of(
        new Route(RestRequest.Method.POST, STATEMENT_API_ENDPOINT),
        new Route(RestRequest.Method.GET, STATEMENT_API_ENDPOINT + "/{queryId}/{token}"),
        new Route(RestRequest.Method.DELETE, STATEMENT_API_ENDPOINT + "/{queryId}"));
  }

  @Override
  protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
    return switch (request.method()) {
      case POST -> handlePost(request);
      case GET -> handleGet(request);
      case DELETE -> handleDelete(request);
      default ->
          channel ->
              channel.sendResponse(
                  new BytesRestResponse(RestStatus.METHOD_NOT_ALLOWED, "Method not allowed"));
    };
  }

  /**
   * POST /_plugins/_trino_sql/v1/statement Submit a query. If TrinoEngine is available, execute the
   * query and return real results. Otherwise, return a stub response.
   */
  private RestChannelConsumer handlePost(RestRequest request) {
    String body = request.content().utf8ToString();

    // The Trino client protocol sends the SQL as the raw body (text/plain),
    // but some clients may wrap it in JSON: {"query": "SELECT 1"}
    String query = extractQuery(body);
    LOG.debug("Trino: received query [{}]", query);

    if (engine != null) {
      // Read optional catalog/schema from Trino client headers
      String catalog = request.header("X-Trino-Catalog");
      String schema = request.header("X-Trino-Schema");

      // Real execution via TrinoEngine
      String responseBody = engine.executeAndSerializeJson(query, catalog, schema);
      return channel ->
          channel.sendResponse(new BytesRestResponse(OK, "application/json", responseBody));
    }

    // Fallback: stub response (engine not initialized)
    String queryId = UUID.randomUUID().toString().replace("-", "");
    LOG.debug("Trino stub: no engine available, returning stub for query [{}]", query);

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

    return channel ->
        channel.sendResponse(new BytesRestResponse(OK, "application/json", responseBody));
  }

  /** Extract SQL query from the request body. Handles both raw text and JSON-wrapped formats. */
  private String extractQuery(String body) {
    if (body == null || body.isBlank()) {
      return "";
    }
    String trimmed = body.trim();
    // If body looks like JSON, try to extract "query" field
    if (trimmed.startsWith("{")) {
      // Simple JSON extraction without pulling in a JSON library
      int queryIdx = trimmed.indexOf("\"query\"");
      if (queryIdx >= 0) {
        int colonIdx = trimmed.indexOf(':', queryIdx + 7);
        if (colonIdx >= 0) {
          // Find the string value after the colon
          int startQuote = trimmed.indexOf('"', colonIdx + 1);
          if (startQuote >= 0) {
            int endQuote = findClosingQuote(trimmed, startQuote + 1);
            if (endQuote >= 0) {
              return trimmed.substring(startQuote + 1, endQuote);
            }
          }
        }
      }
      // Could not parse JSON, return as-is
      return trimmed;
    }
    // Raw text body (standard Trino client protocol)
    return trimmed;
  }

  /** Find the closing double-quote, handling escaped quotes. */
  private int findClosingQuote(String s, int start) {
    for (int i = start; i < s.length(); i++) {
      if (s.charAt(i) == '\\') {
        i++; // skip escaped char
      } else if (s.charAt(i) == '"') {
        return i;
      }
    }
    return -1;
  }

  /** GET /_plugins/_trino_sql/v1/statement/{queryId}/{token} Fetch next results. Stub: 404. */
  private RestChannelConsumer handleGet(RestRequest request) {
    String queryId = request.param("queryId");
    String token = request.param("token");
    LOG.debug("Trino: GET results for query [{}], token [{}]", queryId, token);

    String responseBody = "{\"error\":{\"message\":\"Query not found\",\"errorCode\":1}}";
    return channel ->
        channel.sendResponse(
            new BytesRestResponse(NOT_FOUND, "application/json", responseBody));
  }

  /** DELETE /_plugins/_trino_sql/v1/statement/{queryId} Cancel query. Stub: 204. */
  private RestChannelConsumer handleDelete(RestRequest request) {
    String queryId = request.param("queryId");
    LOG.debug("Trino: DELETE query [{}]", queryId);

    return channel ->
        channel.sendResponse(
            new BytesRestResponse(RestStatus.NO_CONTENT, "application/json", ""));
  }
}
