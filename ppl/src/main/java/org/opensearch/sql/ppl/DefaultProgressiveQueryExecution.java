/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.executor.ExecutionEngine.QueryResponse;
import org.opensearch.sql.executor.ProgressiveQueryExecution;

/**
 * Final-result implementation of the progressive execution contract.
 *
 * <p>This adapter keeps the existing callback-based query execution unchanged. Until progressive
 * result producers are added, {@link #currentResult()} is empty while execution is running and
 * exposes the final response immediately before successful completion is published.
 */
final class DefaultProgressiveQueryExecution
    implements ProgressiveQueryExecution, ResponseListener<QueryResponse> {
  private final CompletableFuture<Void> completion = new CompletableFuture<>();
  private volatile QueryResponse finalResult;

  @Override
  public void onResponse(QueryResponse response) {
    finalResult = Objects.requireNonNull(response);
    // Publish completion only after currentResult() can observe the authoritative final response.
    completion.complete(null);
  }

  @Override
  public void onFailure(Exception failure) {
    completion.completeExceptionally(Objects.requireNonNull(failure));
  }

  @Override
  public Optional<QueryResponse> currentResult() {
    return Optional.ofNullable(finalResult);
  }

  @Override
  public CompletionStage<Void> completion() {
    return completion;
  }

  @Override
  public void close() {
    // This final-only adapter owns no execution resources.
  }
}
