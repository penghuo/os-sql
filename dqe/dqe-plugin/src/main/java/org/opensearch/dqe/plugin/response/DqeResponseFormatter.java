/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.plugin.response;

import java.util.List;
import org.opensearch.dqe.parser.DqeException;

/**
 * Formats DQE query results and errors into JSON per design doc Section 6.4.
 *
 * <p>Success response format:
 *
 * <pre>{@code
 * {
 *   "engine": "dqe",
 *   "schema": [{"name": "col1", "type": "integer"}, ...],
 *   "data": [[val1, val2, ...], ...],
 *   "stats": {
 *     "state": "COMPLETED",
 *     "query_id": "...",
 *     "elapsed_ms": 42,
 *     "rows_processed": 100,
 *     "bytes_processed": 4096,
 *     "stages": 2,
 *     "shards_queried": 5
 *   }
 * }
 * }</pre>
 *
 * <p>Error response format:
 *
 * <pre>{@code
 * {
 *   "engine": "dqe",
 *   "error": {
 *     "type": "DqeException",
 *     "reason": "...",
 *     "error_code": "EXECUTION_ERROR"
 *   },
 *   "status": 400,
 *   "query_id": "..."
 * }
 * }</pre>
 */
public class DqeResponseFormatter {

  public DqeResponseFormatter() {}

  /**
   * Format a successful query response as a JSON string.
   *
   * @param response the query response
   * @return JSON string
   */
  public String formatSuccess(DqeQueryResponse response) {
    StringBuilder sb = new StringBuilder();
    sb.append("{");
    sb.append("\"engine\":").append(jsonString(response.getEngine()));

    // Schema
    sb.append(",\"schema\":[");
    List<DqeQueryResponse.ColumnSchema> schema = response.getSchema();
    for (int i = 0; i < schema.size(); i++) {
      if (i > 0) {
        sb.append(",");
      }
      DqeQueryResponse.ColumnSchema col = schema.get(i);
      sb.append("{\"name\":").append(jsonString(col.getName()));
      sb.append(",\"type\":").append(jsonString(col.getType())).append("}");
    }
    sb.append("]");

    // Data
    sb.append(",\"data\":[");
    List<List<Object>> data = response.getData();
    for (int i = 0; i < data.size(); i++) {
      if (i > 0) {
        sb.append(",");
      }
      sb.append("[");
      List<Object> row = data.get(i);
      for (int j = 0; j < row.size(); j++) {
        if (j > 0) {
          sb.append(",");
        }
        sb.append(jsonValue(row.get(j)));
      }
      sb.append("]");
    }
    sb.append("]");

    // Stats
    if (response.getStats() != null) {
      sb.append(",\"stats\":").append(formatStats(response.getStats()));
    }

    sb.append("}");
    return sb.toString();
  }

  /**
   * Format an error response as a JSON string.
   *
   * @param exception the DQE exception
   * @param queryId the query ID (may be null)
   * @return JSON string
   */
  public String formatError(DqeException exception, String queryId) {
    StringBuilder sb = new StringBuilder();
    sb.append("{");
    sb.append("\"engine\":\"dqe\"");
    sb.append(",\"error\":{");
    sb.append("\"type\":\"DqeException\"");
    sb.append(",\"reason\":").append(jsonString(exception.getMessage()));
    sb.append(",\"error_code\":").append(jsonString(exception.getErrorCode().name()));
    sb.append("}");
    sb.append(",\"status\":400");
    if (queryId != null) {
      sb.append(",\"query_id\":").append(jsonString(queryId));
    }
    sb.append("}");
    return sb.toString();
  }

  private String formatStats(DqeQueryResponse.QueryStats stats) {
    StringBuilder sb = new StringBuilder();
    sb.append("{");
    sb.append("\"state\":").append(jsonString(stats.getState()));
    if (stats.getQueryId() != null) {
      sb.append(",\"query_id\":").append(jsonString(stats.getQueryId()));
    }
    sb.append(",\"elapsed_ms\":").append(stats.getElapsedMs());
    sb.append(",\"rows_processed\":").append(stats.getRowsProcessed());
    sb.append(",\"bytes_processed\":").append(stats.getBytesProcessed());
    sb.append(",\"stages\":").append(stats.getStages());
    sb.append(",\"shards_queried\":").append(stats.getShardsQueried());
    sb.append("}");
    return sb.toString();
  }

  private static String jsonString(String value) {
    if (value == null) {
      return "null";
    }
    StringBuilder sb = new StringBuilder("\"");
    for (int i = 0; i < value.length(); i++) {
      char c = value.charAt(i);
      switch (c) {
        case '"':
          sb.append("\\\"");
          break;
        case '\\':
          sb.append("\\\\");
          break;
        case '\n':
          sb.append("\\n");
          break;
        case '\r':
          sb.append("\\r");
          break;
        case '\t':
          sb.append("\\t");
          break;
        default:
          if (c < 0x20) {
            sb.append(String.format("\\u%04x", (int) c));
          } else {
            sb.append(c);
          }
      }
    }
    sb.append("\"");
    return sb.toString();
  }

  private static String jsonValue(Object value) {
    if (value == null) {
      return "null";
    }
    if (value instanceof Number) {
      return value.toString();
    }
    if (value instanceof Boolean) {
      return value.toString();
    }
    return jsonString(value.toString());
  }
}
