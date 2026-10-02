/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.asyncquery.model;

import java.util.List;
import java.util.Map;
import lombok.Data;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.executor.progress.QueryProgress;

/** AsyncQueryExecutionResponse to store the response form spark job execution. */
@Data
public class AsyncQueryExecutionResponse {
  private final String status;
  private final ExecutionEngine.Schema schema;
  private final List<ExprValue> results;
  private final String error;
  private final String sessionId;

  /**
   * Statement-level {@code explain} result. Populated only when the underlying job produced a
   * {@link org.opensearch.sql.job.QueryResult.Explain}; {@code null} for ordinary row-shaped
   * responses. Transport layers that see a non-null value bypass the standard schema-and-datarows
   * renderer and format this with the sync explain formatter.
   */
  private final ExecutionEngine.ExplainResponse explain;

  /** Structured synchronous error details, populated on the PPL failure path. */
  private final Map<String, Object> errorDetails;

  /** Completion estimate, or {@code null} when the underlying path reports none. */
  private final QueryProgress progress;

  public AsyncQueryExecutionResponse(
      String status,
      ExecutionEngine.Schema schema,
      List<ExprValue> results,
      String error,
      String sessionId,
      ExecutionEngine.ExplainResponse explain) {
    this(status, schema, results, error, sessionId, explain, null, null);
  }

  public AsyncQueryExecutionResponse(
      String status,
      ExecutionEngine.Schema schema,
      List<ExprValue> results,
      String error,
      String sessionId,
      ExecutionEngine.ExplainResponse explain,
      Map<String, Object> errorDetails) {
    this(status, schema, results, error, sessionId, explain, errorDetails, null);
  }

  public AsyncQueryExecutionResponse(
      String status,
      ExecutionEngine.Schema schema,
      List<ExprValue> results,
      String error,
      String sessionId,
      ExecutionEngine.ExplainResponse explain,
      Map<String, Object> errorDetails,
      QueryProgress progress) {
    this.status = status;
    this.schema = schema;
    this.results = results;
    this.error = error;
    this.sessionId = sessionId;
    this.explain = explain;
    this.errorDetails = errorDetails;
    this.progress = progress;
  }
}
