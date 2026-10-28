/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.job;

import java.time.Clock;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * Active object that carries one query through the lifecycle state machine.
 *
 * <p>Instances are constructed only by {@link QueryJobService} implementations. The public surface
 * is deliberately small — five accessors and one mutator — so that engines cannot observe or drive
 * lifecycle transitions except through the runner they were given.
 *
 * <p>State transitions:
 *
 * <pre>
 *   PENDING --startRunner()--&gt; RUNNING --runner success--&gt; SUCCEEDED
 *                                     \--runner failure--&gt; FAILED
 *                                     \--cancel()------&gt; CANCELLED
 *   PENDING --cancel()------&gt; CANCELLED
 *   PENDING --failQueued()--&gt; FAILED    (waited too long to start)
 * </pre>
 *
 * <h2>Thread safety</h2>
 *
 * All mutable state is guarded by {@code this}. Side effects that may block or reenter — invoking
 * the runner, cancelling it, completing the future — are performed after the monitor is released.
 * The {@link CompletableFuture} that backs completion is never leaked to callers; observation goes
 * through {@link #await(Duration)} and {@link #onTerminal(Runnable)}.
 */
public final class QueryJob {

  private final QueryJobId id;
  private final Principal owner;
  private final QueryRunner runner;
  private final Clock clock;
  private final Function<Throwable, Map<String, Object>> failureRenderer;
  private final long submittedAtMillis;
  private final CompletableFuture<QueryResult> completion = new CompletableFuture<>();

  private QueryJobState state = QueryJobState.PENDING;
  private OptionalLong startedAtMillis = OptionalLong.empty();
  private OptionalLong completedAtMillis = OptionalLong.empty();
  private Optional<QueryFailure> failure = Optional.empty();
  private Optional<QueryResult> result = Optional.empty();

  /**
   * Creates a job in {@link QueryJobState#PENDING}. Package-private: only {@link QueryJobService}
   * implementations, which share this package, may construct a job. The submission time is captured
   * from {@code clock} at construction; the runner is not started here.
   *
   * @param id opaque, node-routable identifier for this job
   * @param owner caller identity retained for later authorization on {@code get} / {@code cancel}
   * @param runner engine adapter that will produce the result; started later via {@link
   *     #startRunner()}
   * @param clock time source used for submission, start, and completion timestamps
   * @throws NullPointerException if any of {@code id}, {@code owner}, {@code runner}, or {@code
   *     clock} is {@code null}
   */
  QueryJob(QueryJobId id, Principal owner, QueryRunner runner, Clock clock) {
    this(id, owner, runner, clock, null);
  }

  /**
   * Creates a job in {@link QueryJobState#PENDING} with a failure renderer. Package-private: only
   * {@link QueryJobService} implementations may construct a job.
   *
   * @param failureRenderer supplies the structured {@code details} payload on failure; nullable
   *     (empty details). Invoked on the runner-completing thread exactly once if the job reaches
   *     {@code FAILED}. Exceptions from the renderer are swallowed — see {@link
   *     QueryFailure#of(Throwable, Function)}.
   */
  QueryJob(
      QueryJobId id,
      Principal owner,
      QueryRunner runner,
      Clock clock,
      Function<Throwable, Map<String, Object>> failureRenderer) {
    this.id = Objects.requireNonNull(id, "id must not be null");
    this.owner = Objects.requireNonNull(owner, "owner must not be null");
    this.runner = Objects.requireNonNull(runner, "runner must not be null");
    this.clock = Objects.requireNonNull(clock, "clock must not be null");
    this.failureRenderer = failureRenderer;
    this.submittedAtMillis = clock.millis();
  }

  /** Returns the opaque, node-routable job identifier. */
  public QueryJobId id() {
    return id;
  }

  /** Returns the caller identity captured at submission. */
  public Principal owner() {
    return owner;
  }

  /** Returns an immutable snapshot of the job's current state. */
  public synchronized QueryJobStatus status() {
    return new QueryJobStatus(
        id, state, submittedAtMillis, startedAtMillis, completedAtMillis, failure, result);
  }

  /**
   * Bounded, non-blocking wait. The returned stage fires exactly once — with the runner's {@link
   * QueryResult} on success, {@link QueryResult.Running} when the budget expires, or exceptionally
   * with the unwrapped runner cause on failure or cancellation. The callback fires on the
   * runner-completing thread (success/failure) or the JDK {@code Delayer} daemon (timeout); callers
   * needing {@code ThreadContext} preserved must wrap their listener with {@code
   * ContextPreservingActionListener} before registering.
   *
   * <p>Non-{@link Exception} throwables re-throw so JVM-level errors (e.g. {@link
   * OutOfMemoryError}) are not silently downgraded to an application-level failure.
   */
  public CompletionStage<QueryResult> await(Duration budget) {
    CompletableFuture<QueryResult> out = outcome();
    long millis = budget == null ? 0L : budget.toMillis();
    if (millis <= 0L) {
      out.complete(null);
    } else {
      out.completeOnTimeout(null, millis, TimeUnit.MILLISECONDS);
    }
    // null marks an expired wait.
    return out.thenCompose(value -> value != null ? out : onWaitExpired()).minimalCompletionStage();
  }

  /** The job's outcome: its result on success, its unwrapped cause on failure or cancellation. */
  private CompletableFuture<QueryResult> outcome() {
    CompletableFuture<QueryResult> out = new CompletableFuture<>();
    completion.whenComplete(
        (value, throwable) -> {
          if (throwable == null) {
            out.complete(value);
            return;
          }
          Throwable cause = unwrap(throwable);
          if (cause instanceof Error error) {
            throw error;
          }
          out.completeExceptionally(cause);
        });
    return out;
  }

  /** Reports an unfinished job by its state; reports a job that just finished inline instead. */
  private CompletionStage<QueryResult> onWaitExpired() {
    synchronized (this) {
      if (!state.isTerminal()) {
        return CompletableFuture.completedFuture(new QueryResult.Running(id, state));
      }
    }
    return outcome();
  }

  /** One-shot hook that fires exactly once when the job reaches any terminal state. */
  public void onTerminal(Runnable action) {
    Objects.requireNonNull(action, "action must not be null");
    completion.whenComplete((result, err) -> action.run());
  }

  /**
   * Requests cancellation. Terminal states are unaffected. Cancellation from {@code PENDING} or
   * {@code RUNNING} moves the job to {@code CANCELLED}, cancels the runner (best effort), and
   * completes the internal future exceptionally.
   */
  public void cancel() {
    boolean shouldCancelRunner;
    synchronized (this) {
      if (state.isTerminal()) {
        return;
      }
      state = QueryJobState.CANCELLED;
      completedAtMillis = OptionalLong.of(clock.millis());
      shouldCancelRunner = true;
    }
    if (shouldCancelRunner) {
      safeCancelRunner();
    }
    completion.cancel(false);
  }

  /**
   * Fails a job that waited too long to start. Package-private: called by the scheduler that queued
   * it.
   *
   * @param cause reported as the job's failure
   * @return {@code true} if the job was {@code PENDING} and is now {@code FAILED}
   */
  boolean failQueued(Throwable cause) {
    synchronized (this) {
      if (state != QueryJobState.PENDING) {
        return false;
      }
      state = QueryJobState.FAILED;
      completedAtMillis = OptionalLong.of(clock.millis());
      failure = Optional.of(QueryFailure.of(cause, failureRenderer));
    }
    completion.completeExceptionally(cause);
    return true;
  }

  /**
   * Transitions the job from {@link QueryJobState#PENDING} to {@link QueryJobState#RUNNING}, calls
   * {@link QueryRunner#run()}, and wires the returned stage into this job's state machine.
   *
   * <p>Package-private. The service invokes this exactly once, immediately after publishing the job
   * to the {@link QueryJobStore}. Any of the following short-circuit the call:
   *
   * <ul>
   *   <li>the job has already been cancelled while pending — the runner is not started;
   *   <li>{@code runner.run()} throws — the job moves to {@link QueryJobState#FAILED};
   *   <li>{@code runner.run()} returns {@code null} — treated as a runner failure.
   * </ul>
   */
  void startRunner() {
    synchronized (this) {
      if (state != QueryJobState.PENDING) {
        return;
      }
      state = QueryJobState.RUNNING;
      startedAtMillis = OptionalLong.of(clock.millis());
    }
    CompletionStage<QueryResult> stage;
    try {
      stage = Objects.requireNonNull(runner.run(), "runner must not return null");
    } catch (RuntimeException e) {
      onRunnerFailure(e);
      return;
    }
    stage.whenComplete(
        (value, throwable) -> {
          if (throwable != null) {
            onRunnerFailure(unwrap(throwable));
          } else if (value == null) {
            onRunnerFailure(new IllegalStateException("runner completed with null result"));
          } else {
            onRunnerSuccess(value);
          }
        });
  }

  /**
   * Terminal transition from {@link QueryJobState#RUNNING} to {@link QueryJobState#SUCCEEDED}.
   * Ignored if the job has already left {@code RUNNING} (e.g. a concurrent {@link #cancel()} beat
   * the runner). Records the result inside the monitor; completes the future outside it.
   *
   * @param value final result produced by the runner; never {@code null}
   */
  private void onRunnerSuccess(QueryResult value) {
    synchronized (this) {
      if (state != QueryJobState.RUNNING) {
        return;
      }
      state = QueryJobState.SUCCEEDED;
      completedAtMillis = OptionalLong.of(clock.millis());
      result = Optional.of(value);
    }
    completion.complete(value);
  }

  /**
   * Terminal transition to {@link QueryJobState#FAILED}. Accepts the transition from either {@code
   * RUNNING} (normal failure path) or {@code PENDING} (synchronous throw from {@link
   * QueryRunner#run()}). Ignored once the job is already terminal.
   *
   * @param throwable exception raised by the runner; may be a raw cause or a {@link
   *     CompletionException} wrapper (already unwrapped in {@link #startRunner()})
   */
  private void onRunnerFailure(Throwable throwable) {
    synchronized (this) {
      if (state != QueryJobState.RUNNING && state != QueryJobState.PENDING) {
        return;
      }
      state = QueryJobState.FAILED;
      completedAtMillis = OptionalLong.of(clock.millis());
      failure = Optional.of(QueryFailure.of(throwable, failureRenderer));
    }
    completion.completeExceptionally(throwable);
  }

  /**
   * Best-effort cancel of the runner. Swallows {@link RuntimeException} — a misbehaving runner must
   * not block the state machine, and the job has already been marked {@code CANCELLED} before this
   * is called.
   */
  private void safeCancelRunner() {
    try {
      runner.cancel();
    } catch (RuntimeException ignored) {
      // Cancellation is best-effort; a misbehaving runner must not block the state machine.
    }
  }

  /**
   * Peels a single {@link CompletionException} wrapper so downstream reporting sees the original
   * runner exception. Non-wrapper throwables and wrappers with no cause pass through unchanged.
   *
   * @param throwable throwable observed on the runner's completion stage
   * @return the underlying cause when {@code throwable} is a {@link CompletionException} carrying a
   *     non-{@code null} cause; otherwise the original throwable
   */
  public static Throwable unwrap(Throwable throwable) {
    return throwable instanceof CompletionException && throwable.getCause() != null
        ? throwable.getCause()
        : throwable;
  }
}
