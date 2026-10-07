/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.job;

import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;
import org.opensearch.sql.common.response.ResponseListener;

/**
 * Engine adapter that produces a {@link QueryResult} for one submitted query.
 *
 * <p>Implementations live in the engine modules (PPL, SQL, analytics-engine). They translate the
 * neutral {@link SubmitRequest} into the engine-specific execution plan, own any threading, and
 * report completion or failure through the returned {@link CompletionStage}.
 *
 * <p>A runner is single-use. {@link #run(ResultListeners)} must be invoked exactly once; subsequent
 * invocations throw {@link IllegalStateException}. {@link #cancel()} is idempotent and safe to
 * invoke before {@link #run(ResultListeners)} or after completion.
 *
 * <p>A runner observes nothing about its own progress. The job layer owns any per-query
 * instrumentation and supplies it through the {@link ResultListeners} handed to {@link
 * #run(ResultListeners)}.
 */
public interface QueryRunner {

  /**
   * Starts execution and returns the future that carries the final result.
   *
   * @param listeners factory for the listeners this runner hands to its engine; bound to the
   *     submitting job
   */
  CompletionStage<QueryResult> run(ResultListeners listeners);

  /** Requests cooperative cancellation. Safe to call from any state. */
  void cancel();

  /**
   * Supplies the response listeners a {@link QueryRunner} hands to its engine, for one execution.
   *
   * <p>A runner describes what to do with a result and with a failure; the job layer decides what
   * the engine actually receives. Whatever instrumentation the returned listener carries is the job
   * layer's business — a runner must pass it on unchanged and must not inspect, unwrap, or re-wrap
   * it.
   *
   * <p>An implementation is bound to exactly one job, so every listener it produces reports to that
   * job and to no other.
   */
  interface ResultListeners {

    /**
     * Builds the listener the engine should receive for a result of type {@code T}.
     *
     * <p>Forwards whichever outcome the engine reports to the matching callback, on the engine
     * thread that reports it. This adds no arrival guarantee of its own: a repeated or late outcome
     * is absorbed downstream, by the runner's {@link java.util.concurrent.CompletableFuture} and by
     * the job state machine, both of which already ignore a second terminal outcome.
     *
     * @param onResponse invoked with the engine's successful result
     * @param onFailure invoked with the engine's exception
     */
    <T> ResponseListener<T> listenerFor(Consumer<T> onResponse, Consumer<Exception> onFailure);

    /**
     * Factory that adds nothing, for tests and for any caller that drives a runner without a job.
     *
     * <p>Not a lambda because {@link #listenerFor} is generic.
     */
    ResultListeners PLAIN =
        new ResultListeners() {
          @Override
          public <T> ResponseListener<T> listenerFor(
              Consumer<T> onResponse, Consumer<Exception> onFailure) {
            return new ResponseListener<T>() {
              @Override
              public void onResponse(T response) {
                onResponse.accept(response);
              }

              @Override
              public void onFailure(Exception e) {
                onFailure.accept(e);
              }
            };
          }
        };
  }
}
