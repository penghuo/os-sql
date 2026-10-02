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
import org.opensearch.sql.executor.progress.QueryProgress;

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
   * Highest fraction this job has ever handed to a caller, and the value a non-successful terminal
   * state freezes at. Guarded by {@code this}, like every other mutable field.
   *
   * <p>Sampling the engine's observer and committing the terminal transition have to happen in the
   * same critical section as the reads. If a terminal thread sampled outside the monitor, a poll
   * could publish a higher running value in the gap and the terminal state would then freeze the
   * older, lower one — a client would watch progress go backwards at exactly the moment the query
   * stopped. Keeping the maximum here and only ever raising it makes that unrepresentable: a frozen
   * value is by construction at least as high as anything already published.
   */
  private QueryProgress publishedProgress = QueryProgress.ZERO;

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
        id,
        state,
        submittedAtMillis,
        startedAtMillis,
        completedAtMillis,
        failure,
        result,
        currentProgress());
  }

  /**
   * Resolves the fraction to publish for the current state. Caller must hold {@code this}.
   *
   * <p>The lifecycle, not the engine, decides what {@code 1.0} means. {@code SUCCEEDED} is the only
   * state whose result is ready, so it is the only state that reports {@code 1.0}; a job that
   * failed or was cancelled keeps the frozen maximum, which is what lets a client tell a query that
   * died early from one that died near the end.
   */
  private QueryProgress currentProgress() {
    return switch (state) {
      case SUCCEEDED -> QueryProgress.COMPLETE;
      case PENDING -> QueryProgress.ZERO;
      case RUNNING -> raisePublished(runnerProgress());
      case FAILED, CANCELLED -> publishedProgress;
    };
  }

  /**
   * Raises the published maximum to {@code candidate} and returns the maximum. Caller must hold
   * {@code this}.
   */
  private QueryProgress raisePublished(QueryProgress candidate) {
    if (candidate.fractionDone() > publishedProgress.fractionDone()) {
      publishedProgress = candidate;
    }
    return publishedProgress;
  }

  /**
   * Reads the runner's observer defensively. Caller must hold {@code this}, so that sampling cannot
   * interleave with a state transition.
   *
   * <p>{@link QueryRunner#progress()} is engine-supplied, so a runner that returns {@code null} or
   * throws degrades to "nothing observed" rather than failing the poll a client is using to
   * discover the job's state. It is contractually cheap and non-blocking, which is what makes it
   * safe to call from inside this monitor; the observer's own lock is a leaf — producers never
   * reach back into the job.
   */
  private QueryProgress runnerProgress() {
    try {
      QueryProgress snapshot = runner.progress();
      if (snapshot == null) {
        return QueryProgress.ZERO;
      }
      // Source accounting must never claim completion while the job is still running. Clamping to
      // the
      // public ceiling beats letting QueryJobStatus reject the snapshot, which would turn a
      // progress
      // bug into a failed poll — and clamping rather than zeroing keeps the value monotonic.
      return snapshot.fractionDone() >= 1.0 ? QueryProgress.PUBLIC_CEILING : snapshot;
    } catch (RuntimeException e) {
      return QueryProgress.ZERO;
    }
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
      out.complete(runningSnapshot());
    } else {
      // Supply the timeout value lazily: completeOnTimeout takes a fixed value, so the fraction has
      // to be read when the budget actually expires rather than at registration time, when the
      // query
      // has not started yet. Cancelling the trigger once `out` settles releases the JDK Delayer
      // task
      // instead of leaving it queued for the remainder of the budget.
      CompletableFuture<Void> trigger = new CompletableFuture<>();
      trigger.completeOnTimeout(null, millis, TimeUnit.MILLISECONDS);
      trigger.thenRun(() -> out.complete(runningSnapshot()));
      out.whenComplete((value, throwable) -> trigger.cancel(false));
    }
    return out.minimalCompletionStage();
  }

  /**
   * Builds the submit-time {@code RUNNING} payload.
   *
   * <p>Reads through the same critical section as {@link #status()} rather than sampling the engine
   * directly. The submit timeout races every terminal transition: a job can commit {@code
   * CANCELLED} at a frozen fraction and have late source callbacks advance the observer before the
   * timeout fires. Sampling the observer here would publish that higher value in the submit
   * response and the client's first GET would then report a lower one.
   *
   * <p>Also guards the {@code RUNNING} payload against {@code 1.0}. If success won the race the
   * caller gets the terminal result instead of this, so a {@code RUNNING} body claiming completion
   * could only ever mislead.
   */
  private QueryResult.Running runningSnapshot() {
    QueryProgress progress;
    synchronized (this) {
      progress = currentProgress();
    }
    if (progress.fractionDone() >= 1.0) {
      progress = QueryProgress.PUBLIC_CEILING;
    }
    return new QueryResult.Running(id, progress);
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
      // Sample and freeze inside the monitor, before the state flips. Sampling outside would let a
      // concurrent poll publish a higher running value in the gap, which this transition would then
      // overwrite with the older one.
      raisePublished(runnerProgress());
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
      // Same ordering as cancel(): freeze under the monitor so the frozen value cannot be lower
      // than a
      // concurrently published running one.
      raisePublished(runnerProgress());
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
