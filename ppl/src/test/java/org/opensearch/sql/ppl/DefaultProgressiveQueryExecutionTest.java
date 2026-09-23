/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.Test;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.executor.ExecutionEngine.QueryResponse;
import org.opensearch.sql.executor.ExecutionEngine.Schema;
import org.opensearch.sql.executor.ExecutionEngine.Schema.Column;

public class DefaultProgressiveQueryExecutionTest {

  @Test
  public void finalResultIsVisibleBeforeSuccessfulCompletionNotification() {
    DefaultProgressiveQueryExecution execution = new DefaultProgressiveQueryExecution();
    AtomicBoolean visibleFromCompletion = new AtomicBoolean();
    execution
        .completion()
        .whenComplete(
            (ignored, failure) ->
                visibleFromCompletion.set(
                    failure == null
                        && execution.currentResult().orElseThrow().getResults().size() == 1));

    execution.onResponse(response("final"));

    assertTrue(execution.completion().toCompletableFuture().isDone());
    assertFalse(execution.completion().toCompletableFuture().isCompletedExceptionally());
    assertTrue(visibleFromCompletion.get());
  }

  @Test
  public void failureCompletesExceptionallyWithoutPublishingRows() {
    DefaultProgressiveQueryExecution execution = new DefaultProgressiveQueryExecution();

    execution.onFailure(new IllegalStateException("boom"));

    CompletionException failure =
        assertThrows(
            CompletionException.class, () -> execution.completion().toCompletableFuture().join());
    assertTrue(failure.getCause() instanceof IllegalStateException);
    assertTrue(execution.currentResult().isEmpty());
  }

  @Test
  public void firstTerminalCallbackWinsAndCloseIsIdempotent() {
    DefaultProgressiveQueryExecution execution = new DefaultProgressiveQueryExecution();

    execution.onResponse(response("first"));
    execution.onResponse(response("second"));
    execution.onFailure(new IllegalStateException("late"));
    execution.close();
    execution.close();

    assertEquals(
        "first", execution.currentResult().orElseThrow().getResults().getFirst().stringValue());
    assertFalse(execution.completion().toCompletableFuture().isCompletedExceptionally());
  }

  private static QueryResponse response(String value) {
    return new QueryResponse(
        new Schema(List.of(new Column("state", null, ExprCoreType.STRING))),
        List.of(ExprValueUtils.stringValue(value)),
        null);
  }
}
