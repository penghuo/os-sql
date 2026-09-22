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
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.executor.ExecutionEngine.QueryResponse;
import org.opensearch.sql.executor.ExecutionEngine.Schema;
import org.opensearch.sql.executor.ExecutionEngine.Schema.Column;
import org.opensearch.sql.executor.ProgressiveQueryContext;

public class DefaultProgressiveQueryExecutionTest {

  @Test
  public void exposesBoundContextAndCompletion() {
    DefaultProgressiveQueryExecution execution = new DefaultProgressiveQueryExecution();
    TrackingContext context = new TrackingContext(response());

    assertTrue(execution.currentResult().isEmpty());

    execution.onContextReady(context);
    execution.onResponse(response());

    assertEquals(1, execution.currentResult().orElseThrow().getResults().size());
    assertFalse(execution.completion().toCompletableFuture().isCompletedExceptionally());
    assertTrue(execution.completion().toCompletableFuture().isDone());
  }

  @Test
  public void exposesFailureThroughCompletion() {
    DefaultProgressiveQueryExecution execution = new DefaultProgressiveQueryExecution();

    execution.onFailure(new IllegalStateException("boom"));

    CompletionException failure =
        assertThrows(
            CompletionException.class, () -> execution.completion().toCompletableFuture().join());
    assertTrue(failure.getCause() instanceof IllegalStateException);
  }

  @Test
  public void closesContextThatArrivesAfterHandleWasClosed() {
    DefaultProgressiveQueryExecution execution = new DefaultProgressiveQueryExecution();
    TrackingContext context = new TrackingContext(response());

    execution.close();
    execution.onContextReady(context);

    assertEquals(1, context.closes.get());
    assertTrue(execution.currentResult().isEmpty());
  }

  private static QueryResponse response() {
    return new QueryResponse(
        new Schema(List.of(new Column("state", null, ExprCoreType.STRING))),
        List.of(ExprValueUtils.stringValue("ready")),
        null);
  }

  private static final class TrackingContext implements ProgressiveQueryContext {
    private final QueryResponse response;
    private final AtomicInteger closes = new AtomicInteger();

    private TrackingContext(QueryResponse response) {
      this.response = response;
    }

    @Override
    public QueryResponse currentResult() {
      return response;
    }

    @Override
    public void close() {
      closes.incrementAndGet();
    }
  }
}
