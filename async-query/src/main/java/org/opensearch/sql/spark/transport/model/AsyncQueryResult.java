/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.transport.model;

import java.util.Collection;
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

  /**
   * Completion estimate, or {@code null} when the underlying path reports none — the Spark async-query
   * path. A null value omits the {@code progress} object from the response rather than reporting a
   * fabricated zero.
   */
  @Getter @Nullable private final QueryProgress progress;

  public AsyncQueryResult(
      String status,
      ExecutionEngine.Schema schema,
      Collection<ExprValue> exprValues,
      Cursor cursor,
      String error) {
    this(status, schema, exprValues, cursor, error, null);
  }

  public AsyncQueryResult(
      String status,
      ExecutionEngine.Schema schema,
      Collection<ExprValue> exprValues,
      Cursor cursor,
      String error,
      @Nullable QueryProgress progress) {
    super(schema, exprValues, cursor);
    this.status = status;
    this.error = error;
    this.progress = progress;
  }

  public AsyncQueryResult(
      String status,
      ExecutionEngine.Schema schema,
      Collection<ExprValue> exprValues,
      String error) {
    super(schema, exprValues);
    this.status = status;
    this.error = error;
    this.progress = null;
  }
}
