/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import java.time.Clock;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import org.opensearch.sql.executor.ExecutionEngine.ExplainResponse;
import org.opensearch.sql.executor.ExecutionEngine.QueryResponse;
import org.opensearch.sql.job.QueryResult;
import org.opensearch.sql.job.QueryRunner;
import org.opensearch.sql.ppl.domain.PPLQueryRequest;

/**
 * PPL adapter for the neutral {@link QueryRunner} SPI.
 *
 * <p>Wraps a single {@link PPLService#execute} call. Result production runs on the PPL query
 * manager's worker threads exactly as it does today; this class only bridges the callback-based
 * response into a {@link CompletionStage}.
 *
 * <p>Cancellation is best-effort: {@link PPLService} does not surface an interrupt hook for
 * synchronous execution, so {@link #cancel()} marks the future as cancelled and lets a late
 * response drop on the floor.
 *
 * <p>This class observes nothing about the query's progress. It supplies result-conversion
 * callbacks to the {@link ResultListeners} the job layer passes in, and hands the listeners it gets
 * back to {@link PPLService} unchanged — whatever instrumentation they carry belongs to the job.
 */
public final class PPLQueryRunner implements QueryRunner {

  private final PPLService pplService;
  private final PPLQueryRequest request;
  private final Consumer<String> anonymizedQuerySink;
  private final Clock clock;
  private final AtomicBoolean started = new AtomicBoolean();
  private final CompletableFuture<QueryResult> future = new CompletableFuture<>();

  /**
   * @param pplService live PPL service; not owned by the runner
   * @param request rich PPL request; carries include_metadata, time_bounds, format, etc.
   * @param anonymizedQuerySink receives the PII-scrubbed query text; supply {@link
   *     PPLService#NO_ANONYMIZED_QUERY_SINK} when no telemetry is wanted
   * @param clock time source; used to measure {@code tookMillis}
   * @throws NullPointerException if any argument is {@code null}
   */
  public PPLQueryRunner(
      PPLService pplService,
      PPLQueryRequest request,
      Consumer<String> anonymizedQuerySink,
      Clock clock) {
    this.pplService = Objects.requireNonNull(pplService, "pplService must not be null");
    this.request = Objects.requireNonNull(request, "request must not be null");
    this.anonymizedQuerySink =
        Objects.requireNonNull(anonymizedQuerySink, "anonymizedQuerySink must not be null");
    this.clock = Objects.requireNonNull(clock, "clock must not be null");
  }

  @Override
  public CompletionStage<QueryResult> run(ResultListeners listeners) {
    if (!started.compareAndSet(false, true)) {
      throw new IllegalStateException("PPLQueryRunner is single-use");
    }
    Objects.requireNonNull(listeners, "listeners must not be null");
    long startMillis = clock.millis();
    pplService.execute(
        request,
        listeners.listenerFor(
            (QueryResponse response) ->
                future.complete(QueryResult.of(response, clock.millis() - startMillis)),
            future::completeExceptionally),
        listeners.listenerFor(
            (ExplainResponse response) ->
                future.complete(QueryResult.of(response, clock.millis() - startMillis)),
            future::completeExceptionally),
        anonymizedQuerySink);
    return future;
  }

  @Override
  public void cancel() {
    future.cancel(false);
  }
}
