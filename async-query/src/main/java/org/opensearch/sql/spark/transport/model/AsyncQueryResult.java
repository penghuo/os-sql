/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.transport.model;

import java.util.Collection;
import java.util.Map;
import javax.annotation.Nullable;
import lombok.Getter;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.executor.pagination.Cursor;
import org.opensearch.sql.executor.progress.QueryProgress;
import org.opensearch.sql.protocol.response.QueryResult;

/** AsyncQueryResult for async query APIs. */
public class AsyncQueryResult extends QueryResult {

  @Getter private final String status;
  @Getter private final String error;

  /** Structured synchronous error details, populated on the PPL failure path. */
  @Getter private final Map<String, Object> errorDetails;

  /** Completion estimate, or {@code null} when the underlying path reports none. */
  @Getter @Nullable private final QueryProgress progress;

  public AsyncQueryResult(
      String status,
      ExecutionEngine.Schema schema,
      Collection<ExprValue> exprValues,
      Cursor cursor,
      String error) {
    this(status, schema, exprValues, cursor, error, null, null);
  }

  public AsyncQueryResult(
      String status,
      ExecutionEngine.Schema schema,
      Collection<ExprValue> exprValues,
      Cursor cursor,
      String error,
      Map<String, Object> errorDetails) {
    this(status, schema, exprValues, cursor, error, errorDetails, null);
  }

  public AsyncQueryResult(
      String status,
      ExecutionEngine.Schema schema,
      Collection<ExprValue> exprValues,
      Cursor cursor,
      String error,
      Map<String, Object> errorDetails,
      @Nullable QueryProgress progress) {
    super(schema, exprValues, cursor);
    this.status = status;
    this.error = error;
    this.errorDetails = errorDetails;
    this.progress = progress;
  }

  public AsyncQueryResult(
      String status,
      ExecutionEngine.Schema schema,
      Collection<ExprValue> exprValues,
      String error) {
    this(status, schema, exprValues, error, null);
  }

  public AsyncQueryResult(
      String status,
      ExecutionEngine.Schema schema,
      Collection<ExprValue> exprValues,
      String error,
      Map<String, Object> errorDetails) {
    super(schema, exprValues);
    this.status = status;
    this.error = error;
    this.errorDetails = errorDetails;
    this.progress = null;
  }
}
