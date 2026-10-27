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
 * <p>State transitions. {@code PENDING} means accepted but not yet executing: queued, backing off
 * after a refusal, or being prepared. A job never returns to an earlier state, and a terminal state
 * never changes.
 *
 * <pre>
 *   PENDING --execution starts--------------&gt; RUNNING
 *   PENDING --refused for capacity----------&gt; PENDING   (fresh runner, requeued)
 *   PENDING --error before execution--------&gt; FAILED
 *   PENDING --failQueued()------------------&gt; FAILED    (waited too long)
 *   PENDING --cancel()----------------------&gt; CANCELLED
 *   RUNNING --result------------------------&gt; SUCCEEDED
 *   RUNNING --error, or result over budget--&gt; FAILED
 *   RUNNING --cancel()----------------------&gt; CANCELLED
 * </pre>
 *
 * See {@code docs/dev/ppl-async-query-lifecycle.md} for the full lifecycle, including the store and
 * the scheduler.
 *
 * <h2>Scheduling</h2>
 *
 * A {@link QueryJobScheduler} decides when a {@code PENDING} job is handed to its runner. The job
 * stays {@code PENDING} until the runner reports that execution began. When the runner is refused
 * for lack of capacity before that, and it can supply a {@linkplain QueryRunner#newAttempt() fresh
 * attempt}, the job stays {@code PENDING} and the scheduler queues it again.
 *
 * <h2>Result retention</h2>
 *
 * A job still unfinished when the submit wait expires is retained for polling: its id is returned
 * instead of a result. A retained job's result is charged against its {@link ResultBudget} when it
 * arrives; when the budget refuses the charge the job ends {@code FAILED} with the refusal as its
 * cause and the rows are dropped. The store {@linkplain #detach() detaches} the job when it removes
 * it, which returns the charge. Results that are returned inline are never retained and never
 * charged.
 *
 * <p>The job releases its runner on every terminal transition, so a retained job does not keep the
 * engine's request, task, or its own copy of the result reachable.
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
  private QueryRunner runner;
  private final Clock clock;
  private final Function<Throwable, Map<String, Object>> failureRenderer;
  private final ResultBudget resultBudget;
  private final QueryJobScheduler scheduler;
  private final long submittedAtMillis;
  // Signals the terminal transition without holding the result: a retained job's rows must be
  // reachable only through result, so dropping them there frees them.
  private final CompletableFuture<Void> completion = new CompletableFuture<>();

  private QueryJobState state = QueryJobState.PENDING;
  private OptionalLong startedAtMillis = OptionalLong.empty();
  private OptionalLong completedAtMillis = OptionalLong.empty();
  private Optional<QueryFailure> failure = Optional.empty();
  private Optional<QueryResult> result = Optional.empty();
  private boolean dispatched;
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
    this(id, owner, runner, clock, failureRenderer, ResultBudget.UNLIMITED, null);
  }

  /**
   * Creates a job in {@link QueryJobState#PENDING} that {@code scheduler} starts and requeues.
   * Package-private: only {@link QueryJobService} implementations may construct a job.
   *
   * @param resultBudget budget a retained {@code SUCCEEDED} result is charged against
   * @param scheduler requeues the job when its runner is refused for capacity; {@code null} fails
   *     the job instead
   * @throws NullPointerException if {@code resultBudget} is {@code null}
   */
  QueryJob(
      QueryJobId id,
      Principal owner,
      QueryRunner runner,
      Clock clock,
      Function<Throwable, Map<String, Object>> failureRenderer,
      ResultBudget resultBudget,
      QueryJobScheduler scheduler) {
    this.id = Objects.requireNonNull(id, "id must not be null");
    this.owner = Objects.requireNonNull(owner, "owner must not be null");
    this.runner = Objects.requireNonNull(runner, "runner must not be null");
    this.clock = Objects.requireNonNull(clock, "clock must not be null");
    this.failureRenderer = failureRenderer;
    this.resultBudget = Objects.requireNonNull(resultBudget, "resultBudget must not be null");
    this.scheduler = scheduler;
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
   * QueryResult} on success, exceptionally with the unwrapped runner cause on failure or
   * cancellation, or with {@link QueryResult.Running} when the budget expires first. Expiry marks
   * the job retained for polling in the same step that checks it has not finished, so a job is
   * either reported inline or retained, never both. The callback fires on the runner-completing
   * thread (success/failure) or the JDK {@code Delayer} daemon (timeout); callers needing {@code
   * ThreadContext} preserved must wrap their listener with {@code ContextPreservingActionListener}
   * before registering.
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
        (ignored, throwable) -> {
          if (throwable == null) {
            QueryResult value = currentResult();
            if (value != null) {
              out.complete(value);
            } else {
              out.completeExceptionally(
                  new IllegalStateException("query result is no longer available"));
            }
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

  /** Retains an unfinished job for polling; reports a job that just finished inline instead. */
  private CompletionStage<QueryResult> onWaitExpired() {
    synchronized (this) {
      if (!state.isTerminal()) {
        retained = true;
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
    QueryRunner toCancel;
    synchronized (this) {
      if (state.isTerminal()) {
        return;
      }
      state = QueryJobState.CANCELLED;
      completedAtMillis = OptionalLong.of(clock.millis());
      toCancel = runner;
      runner = null;
    }
    safeCancelRunner(toCancel);
    completion.cancel(false);
  }

  /**
   * Fails a job that waited in the queue too long. Package-private: called by the {@link
   * QueryJobScheduler} that queued it.
   *
   * @param cause reported as the job's failure
   * @return {@code true} if the job was {@code PENDING} and is now {@code FAILED}
   */
  boolean failQueued(Throwable cause) {
    synchronized (this) {
      if (state != QueryJobState.PENDING || dispatched) {
        return false;
      }
      state = QueryJobState.FAILED;
      completedAtMillis = OptionalLong.of(clock.millis());
      failure = Optional.of(QueryFailure.of(cause, failureRenderer));
      runner = null;
    }
    completion.completeExceptionally(cause);
    return true;
  }

  /** Whether the job is waiting to be handed to its runner. */
  synchronized boolean isPending() {
    return state == QueryJobState.PENDING && !dispatched;
  }

  /**
   * Detaches the job from the store and returns its result charge to the budget. Package-private:
   * the store calls this exactly once, after removing the job. Later results are never charged.
   */
  void detach() {
    long released;
    synchronized (this) {
      detached = true;
      released = chargedBytes;
      chargedBytes = 0;
    }
    if (released > 0) {
      resultBudget.release(released);
    }
  }

  /**
   * Hands a {@link QueryJobState#PENDING} job to its runner and wires the returned stage into this
   * job's state machine. The job becomes {@link QueryJobState#RUNNING} when the runner reports that
   * execution began.
   *
   * <p>Package-private. The {@link QueryJobScheduler}, or the service when there is none, invokes
   * this when the job may start; again after each capacity rejection. Any of the following
   * short-circuit the call:
   *
   * <ul>
   *   <li>the job is not pending or already handed to its runner — the call is ignored;
   *   <li>{@code runner.run()} throws — the job moves to {@link QueryJobState#FAILED};
   *   <li>{@code runner.run()} returns {@code null} — treated as a runner failure.
   * </ul>
   */
  void startRunner() {
    QueryRunner toRun;
    synchronized (this) {
      if (state != QueryJobState.PENDING || dispatched) {
        return;
      }
      dispatched = true;
      toRun = runner;
    }
    CompletionStage<QueryResult> stage;
    try {
      stage =
          Objects.requireNonNull(
              toRun.run(this::onExecutionStarted), "runner must not return null");
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

  /** Transition from {@link QueryJobState#PENDING} to {@link QueryJobState#RUNNING}. */
  private synchronized void onExecutionStarted() {
    if (state == QueryJobState.PENDING && dispatched) {
      state = QueryJobState.RUNNING;
      startedAtMillis = OptionalLong.of(clock.millis());
    }
  }

  /**
   * Terminal transition from {@link QueryJobState#RUNNING} to {@link QueryJobState#SUCCEEDED}. A
   * result from a runner that never reported its start counts as starting and finishing at once.
   * Ignored once the job is terminal (e.g. a concurrent {@link #cancel()} beat the runner). Records
   * the result inside the monitor; completes the future outside it. A retained job charges the
   * result first and ends {@code FAILED} if the budget refuses it.
   *
   * @param value final result produced by the runner; never {@code null}
   */
  private void onRunnerSuccess(QueryResult value) {
    RuntimeException refused;
    synchronized (this) {
      if (state == QueryJobState.PENDING && dispatched) {
        onExecutionStarted();
      }
      if (state != QueryJobState.RUNNING) {
        return;
      }
      state = QueryJobState.SUCCEEDED;
      completedAtMillis = OptionalLong.of(clock.millis());
      result = Optional.of(value);
      runner = null;
      refused = retained && !detached ? chargeOrFail(value) : null;
    }
    if (refused == null) {
      completion.complete(null);
    } else {
      completion.completeExceptionally(refused);
    }
  }

  /**
   * Charges a {@code SUCCEEDED} result against the budget. On refusal, moves the job to {@code
   * FAILED} with the refusal as its cause and drops the result. Must hold the monitor.
   *
   * @param value result to charge
   * @return the refusal, or {@code null} when the charge was accepted
   */
  private RuntimeException chargeOrFail(QueryResult value) {
    long bytes = QueryResultSizeEstimator.estimate(value);
    try {
      resultBudget.reserve(bytes);
      chargedBytes = bytes;
      return null;
    } catch (RuntimeException e) {
      state = QueryJobState.FAILED;
      result = Optional.empty();
      failure = Optional.of(QueryFailure.of(e, failureRenderer));
      return e;
    }
  }

  /**
   * Terminal transition to {@link QueryJobState#FAILED}, from {@code RUNNING} or from {@code
   * PENDING} (an error before execution began). Ignored once the job is already terminal. A job
   * refused for lack of capacity before execution began stays {@code PENDING} and is requeued
   * instead, when its runner can supply a fresh attempt.
   *
   * @param throwable exception raised by the runner; may be a raw cause or a {@link
   *     CompletionException} wrapper (already unwrapped in {@link #startRunner()})
   */
  private void onRunnerFailure(Throwable throwable) {
    boolean requeued;
    synchronized (this) {
      if (state != QueryJobState.RUNNING && state != QueryJobState.PENDING) {
        return;
      }
      requeued = state == QueryJobState.PENDING && dispatched && prepareNextAttempt(throwable);
      if (!requeued) {
        state = QueryJobState.FAILED;
        completedAtMillis = OptionalLong.of(clock.millis());
        failure = Optional.of(QueryFailure.of(throwable, failureRenderer));
        runner = null;
      }
    }
    if (requeued) {
      scheduler.requeue(this);
    } else {
      completion.completeExceptionally(throwable);
    }
  }

  /**
   * Gives a refused job a fresh runner, to be handed out again, when {@code throwable} is a
   * capacity rejection the scheduler can retry. Must hold the monitor.
   *
   * @return {@code true} if the job will be requeued
   */
  private boolean prepareNextAttempt(Throwable throwable) {
    if (scheduler == null || runner == null || !scheduler.isCapacityRejection(throwable)) {
      return false;
    }
    QueryRunner nextAttempt = runner.newAttempt();
    if (nextAttempt == null) {
      return false;
    }
    dispatched = false;
    runner = nextAttempt;
    return true;
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

  private synchronized QueryResult currentResult() {
    return result.orElse(null);
  }

  /** Whether the job still references its runner. Exposed for tests of the release on terminal. */
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
