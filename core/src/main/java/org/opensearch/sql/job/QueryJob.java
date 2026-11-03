/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.job;

import com.google.common.annotations.VisibleForTesting;
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
 *                                     \--result over budget--&gt; FAILED
 *                                     \--cancel()------&gt; CANCELLED
 *   PENDING --cancel()------&gt; CANCELLED
 * </pre>
 *
 * <h2>Result retention</h2>
 *
 * A job still unfinished when the submit wait expires is retained for polling. A retained job
 * charges its result against its {@link QueryJobStore} when the runner succeeds; when the store
 * refuses the charge, the job ends {@code FAILED} with the refusal as its cause and the rows are
 * dropped. Results returned inline are never charged. The store {@linkplain #detach() detaches} a
 * job when it removes it, which returns the charge. The job releases its runner on every terminal
 * transition, so a retained job does not keep the runner or its copy of the result reachable.
 *
 * <h2>Thread safety</h2>
 *
 * All mutable state is guarded by {@code this}. Side effects that may block or reenter — invoking
 * the runner, cancelling it, completing the future — are performed after the monitor is released.
 * The {@link CompletableFuture} that backs completion is never leaked to callers; observation goes
 * through {@link #await(Duration)} and {@link #onTerminal(Runnable)}.
 */
public final class QueryJob {

  static final double MAX_RUNNING_FRACTION = 0.95D;

  private final QueryJobId id;
  private final Principal owner;
  private QueryRunner runner;
  private final Clock clock;
  private final Function<Throwable, Map<String, Object>> failureRenderer;
  private final QueryJobStore store;
  private final long submittedAtMillis;
  private final CompletableFuture<QueryResult> completion = new CompletableFuture<>();

  private QueryJobState state = QueryJobState.PENDING;
  private OptionalLong startedAtMillis = OptionalLong.empty();
  private OptionalLong completedAtMillis = OptionalLong.empty();
  private Optional<QueryFailure> failure = Optional.empty();
  private Optional<QueryResult> result = Optional.empty();
  private double runningFraction;
  private boolean retained;
  private boolean detached;
  private long chargedBytes;

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
    this(id, owner, runner, clock, failureRenderer, null);
  }

  /**
   * Creates a job in {@link QueryJobState#PENDING} whose retained result is charged against {@code
   * store}. Package-private: only {@link QueryJobService} implementations may construct a job.
   *
   * @param store store the job is registered in; charged when the job succeeds while retained.
   *     Nullable (no charge).
   */
  QueryJob(
      QueryJobId id,
      Principal owner,
      QueryRunner runner,
      Clock clock,
      Function<Throwable, Map<String, Object>> failureRenderer,
      QueryJobStore store) {
    this.id = Objects.requireNonNull(id, "id must not be null");
    this.owner = Objects.requireNonNull(owner, "owner must not be null");
    this.runner = Objects.requireNonNull(runner, "runner must not be null");
    this.clock = Objects.requireNonNull(clock, "clock must not be null");
    this.failureRenderer = failureRenderer;
    this.store = store;
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
  public QueryJobStatus status() {
    double sample = sampleRunnerProgress();
    synchronized (this) {
      return new QueryJobStatus(
          id,
          state,
          submittedAtMillis,
          startedAtMillis,
          completedAtMillis,
          failure,
          result,
          progress(sample));
    }
  }

  /**
   * Bounded, non-blocking wait. The returned stage fires exactly once — with the runner's {@link
   * QueryResult} on success, {@link QueryResult.Running} when the budget expires first, or
   * exceptionally with the unwrapped runner cause on failure or cancellation. The callback fires on
   * the runner-completing thread (success/failure) or the JDK {@code Delayer} daemon (timeout);
   * callers needing {@code ThreadContext} preserved must wrap their listener with {@code
   * ContextPreservingActionListener} before registering.
   *
   * <p>Expiry retains the job for polling in the same step that checks it has not succeeded, so a
   * result is either returned inline or retained and charged, never both.
   *
   * <p>Non-{@link Exception} throwables re-throw so JVM-level errors (e.g. {@link
   * OutOfMemoryError}) are not silently downgraded to an application-level failure.
   */
  public CompletionStage<QueryResult> await(Duration budget) {
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
    long millis = budget == null ? 0L : budget.toMillis();
    if (millis <= 0L) {
      out.complete(retainUnlessSucceeded());
    } else {
      // Cancellable, so an early outcome does not keep the job reachable until the timeout.
      CompletableFuture<Void> timer = new CompletableFuture<>();
      timer
          .completeOnTimeout(null, millis, TimeUnit.MILLISECONDS)
          .thenRun(
              () -> {
                if (!out.isDone()) {
                  out.complete(retainUnlessSucceeded());
                }
              });
      out.whenComplete((value, throwable) -> timer.cancel(false));
    }
    return out.minimalCompletionStage();
  }

  /**
   * Returns the result of a job that already succeeded. Otherwise retains the job for polling and
   * returns {@link QueryResult.Running}, so a result is never retained without being charged.
   */
  private QueryResult retainUnlessSucceeded() {
    synchronized (this) {
      if (state == QueryJobState.SUCCEEDED) {
        return result.orElseThrow();
      }
      retained = true;
    }
    return running();
  }

  private QueryResult.Running running() {
    double sample = sampleRunnerProgress();
    synchronized (this) {
      double fraction = Math.min(MAX_RUNNING_FRACTION, progress(sample).fractionDone());
      return new QueryResult.Running(id, new QueryProgress(fraction));
    }
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
    QueryRunner toCancel;
    double sample = sampleRunnerProgress();
    synchronized (this) {
      if (state.isTerminal()) {
        return;
      }
      progress(sample);
      state = QueryJobState.CANCELLED;
      completedAtMillis = OptionalLong.of(clock.millis());
      toCancel = runner;
      runner = null;
    }
    safeCancelRunner(toCancel);
    completion.cancel(false);
  }

  /**
   * Detaches the job from the store and returns its result charge to the store. Package-private:
   * the store calls this once, after removing the job. A result that arrives later is not charged.
   */
  void detach() {
    long released;
    synchronized (this) {
      detached = true;
      released = chargedBytes;
      chargedBytes = 0;
    }
    if (released > 0) {
      store.release(released);
    }
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
    QueryRunner toRun;
    synchronized (this) {
      if (state != QueryJobState.PENDING) {
        return;
      }
      state = QueryJobState.RUNNING;
      startedAtMillis = OptionalLong.of(clock.millis());
      toRun = runner;
    }
    CompletionStage<QueryResult> stage;
    try {
      stage = Objects.requireNonNull(toRun.run(), "runner must not return null");
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
   * the runner). Records the result inside the monitor; completes the future outside it. A retained
   * job charges the result first and ends {@code FAILED} if the store refuses it.
   *
   * @param value final result produced by the runner; never {@code null}
   */
  private void onRunnerSuccess(QueryResult value) {
    RuntimeException refused = null;
    synchronized (this) {
      if (state != QueryJobState.RUNNING) {
        return;
      }
      completedAtMillis = OptionalLong.of(clock.millis());
      runner = null;
      if (retained && !detached && store != null) {
        long bytes = QueryResultSizeEstimator.estimate(value);
        try {
          store.reserve(bytes);
          chargedBytes = bytes;
        } catch (RuntimeException e) {
          refused = e;
        }
      }
      if (refused == null) {
        state = QueryJobState.SUCCEEDED;
        result = Optional.of(value);
      } else {
        state = QueryJobState.FAILED;
        failure = Optional.of(QueryFailure.of(refused, failureRenderer));
      }
    }
    if (refused == null) {
      completion.complete(value);
    } else {
      completion.completeExceptionally(refused);
    }
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
    // Not sampled: a failing engine has closed its scans, which reads as done.
    synchronized (this) {
      if (state != QueryJobState.RUNNING && state != QueryJobState.PENDING) {
        return;
      }
      state = QueryJobState.FAILED;
      completedAtMillis = OptionalLong.of(clock.millis());
      failure = Optional.of(QueryFailure.of(throwable, failureRenderer));
      runner = null;
    }
    completion.completeExceptionally(throwable);
  }

  /** Never decreases, and stays below 1.0 until SUCCEEDED. Caller holds the monitor. */
  private QueryProgress progress(double sample) {
    if (state == QueryJobState.SUCCEEDED) {
      return QueryProgress.DONE;
    }
    if (state == QueryJobState.RUNNING) {
      runningFraction = Math.max(runningFraction, Math.min(MAX_RUNNING_FRACTION, sample));
    }
    return new QueryProgress(runningFraction);
  }

  /** Called outside the monitor; the runner is foreign code. */
  private double sampleRunnerProgress() {
    QueryRunner sampled;
    synchronized (this) {
      if (state != QueryJobState.RUNNING) {
        return 0D;
      }
      sampled = runner;
    }
    try {
      QueryProgress progress = sampled.progress();
      return progress == null ? 0D : progress.fractionDone();
    } catch (RuntimeException e) {
      return 0D;
    }
  }

  /**
   * Best-effort cancel of the runner. Swallows {@link RuntimeException} — a misbehaving runner must
   * not block the state machine, and the job has already been marked {@code CANCELLED} before this
   * is called.
   */
  private static void safeCancelRunner(QueryRunner toCancel) {
    if (toCancel == null) {
      return;
    }
    try {
      toCancel.cancel();
    } catch (RuntimeException ignored) {
      // Cancellation is best-effort; a misbehaving runner must not block the state machine.
    }
  }

  /** Whether the job still references its runner. */
  @VisibleForTesting
  synchronized boolean holdsRunner() {
    return runner != null;
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
