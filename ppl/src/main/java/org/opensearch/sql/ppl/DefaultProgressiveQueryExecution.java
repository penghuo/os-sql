/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.opensearch.sql.executor.ExecutionEngine.QueryResponse;
import org.opensearch.sql.executor.ProgressiveQueryContext;
import org.opensearch.sql.executor.ProgressiveQueryExecution;
import org.opensearch.sql.executor.ProgressiveQueryResponseListener;

/**
 * Bridges the execution engine callback model to the lifecycle-facing execution handle.
 *
 * <p>This class is internal to the execution module. Callers receive only {@link
 * ProgressiveQueryExecution}.
 */
final class DefaultProgressiveQueryExecution
    implements ProgressiveQueryExecution, ProgressiveQueryResponseListener {
  private final CompletableFuture<Void> completion = new CompletableFuture<>();

  private ProgressiveQueryContext context;
  private boolean closed;

  @Override
  public void onContextReady(ProgressiveQueryContext context) {
    boolean reject;
    synchronized (this) {
      reject = closed || this.context != null;
      if (!reject) {
        this.context = context;
      }
    }
    if (reject) {
      context.close();
    }
  }

  @Override
  public void onResponse(QueryResponse ignored) {
    completion.complete(null);
  }

  @Override
  public void onFailure(Exception failure) {
    completion.completeExceptionally(failure);
  }

  @Override
  public Optional<QueryResponse> currentResult() {
    ProgressiveQueryContext current;
    synchronized (this) {
      current = closed ? null : context;
    }
    return current == null ? Optional.empty() : Optional.of(current.currentResult());
  }

  @Override
  public CompletionStage<Void> completion() {
    return completion;
  }

  @Override
  public void close() {
    ProgressiveQueryContext current;
    synchronized (this) {
      if (closed) {
        return;
      }
      closed = true;
      current = context;
      context = null;
    }
    if (current != null) {
      current.close();
    }
  }
}
