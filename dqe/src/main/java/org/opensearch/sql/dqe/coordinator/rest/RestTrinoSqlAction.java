/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.coordinator.rest;

import static org.opensearch.core.rest.RestStatus.INTERNAL_SERVER_ERROR;
import static org.opensearch.core.rest.RestStatus.OK;

import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.core.action.ActionListener;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.sql.dqe.coordinator.transport.TransportTrinoSqlAction;
import org.opensearch.sql.dqe.coordinator.transport.TrinoSqlAction;
import org.opensearch.sql.dqe.coordinator.transport.TrinoSqlRequest;
import org.opensearch.sql.dqe.coordinator.transport.TrinoSqlResponse;
import org.opensearch.transport.client.node.NodeClient;

/**
 * REST handler for the DQE Trino SQL query endpoint. Accepts POST requests at {@code
 * /_plugins/_trino_sql} for query execution and {@code /_plugins/_trino_sql/_explain} for explain
 * mode.
 *
 * <p>When the {@link TransportTrinoSqlAction} singleton is available, query execution bypasses the
 * transport action framework (action filter chain, task registration, action map lookup) to reduce
 * per-query overhead by ~1-2ms. Falls back to the standard {@code nodeClient.execute()} path if the
 * singleton is not yet initialized.
 */
public class RestTrinoSqlAction extends BaseRestHandler {

  public static final String QUERY_ENDPOINT = "/_plugins/_trino_sql";
  public static final String EXPLAIN_ENDPOINT = "/_plugins/_trino_sql/_explain";

  private static final Logger LOG = LogManager.getLogger();

  @Override
  public List<Route> routes() {
    return List.of(
        new Route(RestRequest.Method.POST, QUERY_ENDPOINT),
        new Route(RestRequest.Method.POST, EXPLAIN_ENDPOINT));
  }

  @Override
  public String getName() {
    return "trino_sql_query_action";
  }

  @Override
  protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient nodeClient) {
    String content = request.content().utf8ToString();
    String query = extractQuery(content);
    boolean isExplain = request.path().endsWith("/_explain");

    // Use direct execution if the transport action singleton is available.
    // This bypasses the action filter chain, task registration, action map lookup,
    // and TrinoSqlRequest object allocation, reducing per-query overhead.
    TransportTrinoSqlAction direct = TransportTrinoSqlAction.getInstance();
    if (direct != null) {
      return channel ->
          direct.executeDirect(
              query,
              isExplain,
              new ActionListener<>() {
                @Override
                public void onResponse(TrinoSqlResponse response) {
                  channel.sendResponse(
                      new BytesRestResponse(OK, response.getContentType(), response.getResult()));
                }

                @Override
                public void onFailure(Exception e) {
                  LOG.error("Error executing Trino SQL query", e);
                  channel.sendResponse(
                      new BytesRestResponse(
                          INTERNAL_SERVER_ERROR,
                          "application/json; charset=UTF-8",
                          "{\"error\":\"" + escapeJson(e.getMessage()) + "\"}"));
                }
              });
    }

    // Fallback: standard transport action dispatch via NodeClient
    TrinoSqlRequest trinoRequest = new TrinoSqlRequest(query, isExplain);
    return channel ->
        nodeClient.execute(
            TrinoSqlAction.INSTANCE,
            trinoRequest,
            new ActionListener<>() {
              @Override
              public void onResponse(TrinoSqlResponse response) {
                channel.sendResponse(
                    new BytesRestResponse(OK, response.getContentType(), response.getResult()));
              }

              @Override
              public void onFailure(Exception e) {
                LOG.error("Error executing Trino SQL query", e);
                channel.sendResponse(
                    new BytesRestResponse(
                        INTERNAL_SERVER_ERROR,
                        "application/json; charset=UTF-8",
                        "{\"error\":\"" + escapeJson(e.getMessage()) + "\"}"));
              }
            });
  }

  /**
   * Extract the "query" field from a JSON request body. Minimal parsing for MVP: looks for the
   * "query" key and extracts its string value.
   */
  static String extractQuery(String json) {
    // Simple JSON extraction without a full parser dependency.
    // Expects: {"query": "SELECT ..."}
    int keyIdx = json.indexOf("\"query\"");
    if (keyIdx < 0) {
      throw new IllegalArgumentException("Request body must contain a \"query\" field");
    }
    int colonIdx = json.indexOf(':', keyIdx + 7);
    if (colonIdx < 0) {
      throw new IllegalArgumentException("Malformed JSON: no colon after \"query\" key");
    }
    // Find the opening quote of the value
    int openQuote = json.indexOf('"', colonIdx + 1);
    if (openQuote < 0) {
      throw new IllegalArgumentException("Malformed JSON: no string value for \"query\"");
    }
    // Find the closing quote, handling escaped quotes
    int closeQuote = findClosingQuote(json, openQuote + 1);
    return unescapeJson(json.substring(openQuote + 1, closeQuote));
  }

  /** Find the position of the closing (unescaped) double-quote starting at the given offset. */
  private static int findClosingQuote(String json, int startIdx) {
    for (int i = startIdx; i < json.length(); i++) {
      char c = json.charAt(i);
      if (c == '\\') {
        i++; // skip escaped char
      } else if (c == '"') {
        return i;
      }
    }
    throw new IllegalArgumentException("Malformed JSON: unterminated string value");
  }

  /** Unescape common JSON escape sequences. */
  private static String unescapeJson(String s) {
    return s.replace("\\\"", "\"")
        .replace("\\\\", "\\")
        .replace("\\n", "\n")
        .replace("\\r", "\r")
        .replace("\\t", "\t");
  }

  /** Escape special characters for inclusion in a JSON string value. */
  static String escapeJson(String s) {
    if (s == null) {
      return "null";
    }
    return s.replace("\\", "\\\\")
        .replace("\"", "\\\"")
        .replace("\n", "\\n")
        .replace("\r", "\\r")
        .replace("\t", "\\t");
  }
}
