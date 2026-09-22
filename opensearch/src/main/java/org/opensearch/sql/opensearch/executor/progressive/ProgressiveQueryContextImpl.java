/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.progressive;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.executor.ExecutionEngine.QueryResponse;
import org.opensearch.sql.executor.ProgressiveQueryContext;

/** OpenSearch execution implementation of a job-scoped progressive query context. */
public final class ProgressiveQueryContextImpl implements ProgressiveQueryContext {
  private final QueryStateStore store;
  private final CalciteResultSetMaterializer materializer;
  private final Consumer<ExprValue> rowConsumer;
  private final SearchExecutionObserver searchObserver;
  private final PreparedStatement previewStatement;
  private final ReentrantReadWriteLock lifecycleLock = new ReentrantReadWriteLock();
  private boolean closed;

  ProgressiveQueryContextImpl(
      QueryStateStore store,
      CalciteResultSetMaterializer materializer,
      Consumer<ExprValue> rowConsumer,
      SearchExecutionObserver searchObserver,
      PreparedStatement previewStatement) {
    this.store = Objects.requireNonNull(store);
    this.materializer = Objects.requireNonNull(materializer);
    this.rowConsumer = Objects.requireNonNull(rowConsumer);
    this.searchObserver = Objects.requireNonNull(searchObserver);
    this.previewStatement = previewStatement;
  }

  public void consume(ResultSet resultSet, Integer querySizeLimit) throws SQLException {
    materializer.drain(resultSet, querySizeLimit, rowConsumer);
  }

  public SearchExecutionObserver searchObserver() {
    return searchObserver;
  }

  @Override
  public QueryResponse currentResult() {
    lifecycleLock.readLock().lock();
    try {
      if (closed) {
        return materializer.materialize(List.of());
      }
      if (previewStatement == null) {
        return materializer.materialize(store.rows());
      }
      synchronized (previewStatement) {
        try (ResultSet resultSet = previewStatement.executeQuery()) {
          return materializer.materialize(resultSet, null);
        } catch (SQLException e) {
          throw new IllegalStateException("Failed to materialize the current query result", e);
        }
      }
    } finally {
      lifecycleLock.readLock().unlock();
    }
  }

  @Override
  public void close() {
    lifecycleLock.writeLock().lock();
    try {
      if (closed) {
        return;
      }
      closed = true;
      store.close();
      if (previewStatement != null) {
        try {
          previewStatement.close();
        } catch (SQLException ignored) {
          // Best-effort cleanup after the job has already reached a terminal lifecycle transition.
        }
      }
    } finally {
      lifecycleLock.writeLock().unlock();
    }
  }
}
