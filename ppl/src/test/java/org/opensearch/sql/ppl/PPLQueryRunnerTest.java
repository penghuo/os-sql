/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import org.junit.Test;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.executor.ExecutionEngine.QueryResponse;
import org.opensearch.sql.executor.ExecutionEngine.Schema;
import org.opensearch.sql.executor.pagination.Cursor;
import org.opensearch.sql.job.QueryResult;
import org.opensearch.sql.job.QueryRunner.ResultListeners;
import org.opensearch.sql.ppl.domain.PPLQueryRequest;

public class PPLQueryRunnerTest {

  @Test
  public void run_deliversQueryResponseThroughFuture()
      throws ExecutionException, InterruptedException {
    PPLService service = mock(PPLService.class);
    QueryResponse response = new QueryResponse(new Schema(List.of()), List.of(), Cursor.None);
    doAnswer(
            invocation -> {
              ResponseListener<QueryResponse> listener = invocation.getArgument(1);
              listener.onResponse(response);
              return null;
            })
        .when(service)
        .execute(any(PPLQueryRequest.class), any(), any(), any());

    QueryResult result = newRunner(service).run(ResultListeners.PLAIN).toCompletableFuture().get();
    assertTrue(result instanceof QueryResult.Rows);
    assertEquals(response.getSchema(), ((QueryResult.Rows) result).schema());
    assertTrue(result.tookMillis() >= 0);
  }

  @Test
  public void run_propagatesQueryFailure() {
    PPLService service = mock(PPLService.class);
    doAnswer(
            invocation -> {
              ResponseListener<QueryResponse> listener = invocation.getArgument(1);
              listener.onFailure(new IllegalStateException("boom"));
              return null;
            })
        .when(service)
        .execute(any(PPLQueryRequest.class), any(), any(), any());

    ExecutionException e =
        assertThrows(
            ExecutionException.class,
            () -> newRunner(service).run(ResultListeners.PLAIN).toCompletableFuture().get());
    assertEquals("boom", e.getCause().getMessage());
  }

  @Test
  public void run_isSingleUse() {
    PPLQueryRunner runner = newRunner(mock(PPLService.class));
    runner.run(ResultListeners.PLAIN);
    assertThrows(IllegalStateException.class, () -> runner.run(ResultListeners.PLAIN));
  }

  /**
   * The runner must hand {@link PPLService} the very objects the factory produced, for both the row
   * and the explain listener. Building its own listener — or wrapping the supplied one — is how
   * whatever instrumentation the job attached would be silently lost.
   */
  @Test
  public void run_passesFactoryProducedListenersToServiceUnchanged() {
    PPLService service = mock(PPLService.class);
    List<ResponseListener<?>> handedToService = new ArrayList<>();
    doAnswer(
            invocation -> {
              handedToService.add(invocation.getArgument(1));
              handedToService.add(invocation.getArgument(2));
              return null;
            })
        .when(service)
        .execute(any(PPLQueryRequest.class), any(), any(), any());

    RecordingListeners listeners = new RecordingListeners();
    newRunner(service).run(listeners);

    assertEquals("expected a row and an explain listener", 2, listeners.produced.size());
    assertEquals(2, handedToService.size());
    assertSame(listeners.produced.get(0), handedToService.get(0));
    assertSame(listeners.produced.get(1), handedToService.get(1));
  }

  /**
   * Both listeners the factory produced complete the same future, so either engine outcome resolves
   * the runner. Exercised through the factory rather than through {@code PLAIN} so the delivery
   * path is the one production uses.
   */
  @Test
  public void run_explainResponseFromFactoryListenerCompletesTheFuture()
      throws ExecutionException, InterruptedException {
    PPLService service = mock(PPLService.class);
    doAnswer(
            invocation -> {
              ResponseListener<ExecutionEngine.ExplainResponse> explainListener =
                  invocation.getArgument(2);
              explainListener.onResponse(
                  new ExecutionEngine.ExplainResponse((ExecutionEngine.ExplainResponseNode) null));
              return null;
            })
        .when(service)
        .execute(any(PPLQueryRequest.class), any(), any(), any());

    QueryResult result =
        newRunner(service).run(new RecordingListeners()).toCompletableFuture().get();
    assertTrue(result instanceof QueryResult.Explain);
  }

  @Test
  public void run_explainFailureFromFactoryListenerFailsTheFuture() {
    PPLService service = mock(PPLService.class);
    doAnswer(
            invocation -> {
              ResponseListener<ExecutionEngine.ExplainResponse> explainListener =
                  invocation.getArgument(2);
              explainListener.onFailure(new IllegalStateException("explain boom"));
              return null;
            })
        .when(service)
        .execute(any(PPLQueryRequest.class), any(), any(), any());

    ExecutionException e =
        assertThrows(
            ExecutionException.class,
            () -> newRunner(service).run(new RecordingListeners()).toCompletableFuture().get());
    assertEquals("explain boom", e.getCause().getMessage());
  }

  @Test
  public void run_rejectsNullListeners() {
    assertThrows(NullPointerException.class, () -> newRunner(mock(PPLService.class)).run(null));
  }

  private static PPLQueryRunner newRunner(PPLService service) {
    return new PPLQueryRunner(
        service,
        new PPLQueryRequest("source=logs", null, "/_plugins/_ppl", "jdbc"),
        s -> {},
        Clock.systemUTC());
  }

  /** Factory that records every listener it produced, in the order the runner asked for them. */
  private static final class RecordingListeners implements ResultListeners {

    private final List<ResponseListener<?>> produced = new ArrayList<>();

    @Override
    public <T> ResponseListener<T> listenerFor(
        Consumer<T> onResponse, Consumer<Exception> onFailure) {
      ResponseListener<T> listener =
          new ResponseListener<T>() {
            @Override
            public void onResponse(T response) {
              onResponse.accept(response);
            }

            @Override
            public void onFailure(Exception e) {
              onFailure.accept(e);
            }
          };
      produced.add(listener);
      return listener;
    }
  }
}
