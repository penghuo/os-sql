/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.job;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.executor.ExecutionEngine.Schema;
import org.opensearch.sql.executor.pagination.Cursor;
import org.opensearch.sql.executor.progress.ProgressObserver;
import org.opensearch.sql.executor.progress.QueryProgress;
import org.opensearch.sql.executor.progress.SourceProgressEvent;

/**
 * Lifecycle half of the progress contract: which fraction each job state publishes, and that
 * nothing a client can observe ever moves backwards.
 *
 * <p>The races here are driven deterministically with latches rather than by sleeping, so a
 * regression shows up as a failure on the first run instead of flaking.
 */
class QueryJobProgressTest {

  private static final QueryJobId ID = new QueryJobId("node-1", "ctx-1");
  private static final Principal OWNER = new Principal("alice", null, List.of());
  private static final Schema SCHEMA = new Schema(List.of());
  private static final QueryResult RESULT =
      new QueryResult.Rows(SCHEMA, List.of(), Cursor.None, List.of(), 0);
  private static final double CEILING = QueryProgress.PUBLIC_CEILING.fractionDone();

  @Test
  @DisplayName("PENDING reports zero regardless of what the observer would say")
  void pendingReportsZero() {
    ProgressRunner runner = new ProgressRunner();
    runner.set(0.5);
    QueryJob job = newJob(runner);
    assertEquals(0.0, job.status().progress().fractionDone());
  }

  @Test
  @DisplayName("RUNNING reports the observed fraction")
  void runningReportsObservedFraction() {
    ProgressRunner runner = new ProgressRunner();
    QueryJob job = newJob(runner);
    job.startRunner();
    runner.set(0.4);
    assertEquals(0.4, job.status().progress().fractionDone());
  }

  @Test
  @DisplayName("SUCCEEDED reports exactly 1.0 even if the source accounting lagged")
  void succeededReportsOne() {
    ProgressRunner runner = new ProgressRunner();
    QueryJob job = newJob(runner);
    job.startRunner();
    runner.set(0.2);
    runner.complete(RESULT);
    assertEquals(1.0, job.status().progress().fractionDone());
  }

  @Test
  @DisplayName("FAILED freezes the last running fraction")
  void failedFreezesLastRunningFraction() {
    ProgressRunner runner = new ProgressRunner();
    QueryJob job = newJob(runner);
    job.startRunner();
    runner.set(0.6);
    runner.fail(new IllegalStateException("boom"));

    assertEquals(0.6, job.status().progress().fractionDone());
    // Late source activity after the terminal transition must not move a frozen value.
    runner.set(0.8);
    assertEquals(0.6, job.status().progress().fractionDone());
  }

  @Test
  @DisplayName("CANCELLED freezes the last running fraction")
  void cancelledFreezesLastRunningFraction() {
    ProgressRunner runner = new ProgressRunner();
    QueryJob job = newJob(runner);
    job.startRunner();
    runner.set(0.3);
    job.cancel();

    assertEquals(0.3, job.status().progress().fractionDone());
    runner.set(0.8);
    assertEquals(0.3, job.status().progress().fractionDone());
  }

  @Test
  @DisplayName("an observer claiming completion while running is clamped, never published as 1.0")
  void runningNeverPublishesOne() {
    ProgressRunner runner = new ProgressRunner();
    QueryJob job = newJob(runner);
    job.startRunner();
    runner.set(1.0);
    assertEquals(CEILING, job.status().progress().fractionDone());
  }

  @Test
  @DisplayName("a throwing or null observer degrades to zero instead of failing the poll")
  void misbehavingObserverDegradesToZero() {
    ProgressRunner thrower = new ProgressRunner();
    thrower.setThrowing(true);
    QueryJob throwingJob = newJob(thrower);
    throwingJob.startRunner();
    assertEquals(0.0, throwingJob.status().progress().fractionDone());

    ProgressRunner nuller = new ProgressRunner();
    nuller.setReturningNull(true);
    QueryJob nullJob = newJob(nuller);
    nullJob.startRunner();
    assertEquals(0.0, nullJob.status().progress().fractionDone());
  }

  @Test
  @DisplayName("repeated polls never decrease even when the observer regresses")
  void pollsAreMonotonicAgainstRegressingObserver() {
    ProgressRunner runner = new ProgressRunner();
    QueryJob job = newJob(runner);
    job.startRunner();

    runner.set(0.5);
    assertEquals(0.5, job.status().progress().fractionDone());
    runner.set(0.1);
    assertEquals(0.5, job.status().progress().fractionDone());
  }

  // ------------------------------------------------------------------ P1-TERMINAL-MONOTONIC

  @RepeatedTest(20)
  @DisplayName("cancel cannot freeze a value below one already published to a poller")
  void cancelFreezesAtLeastThePublishedMaximum() throws Exception {
    assertTerminalNeverRegresses(QueryJob::cancel);
  }

  @RepeatedTest(20)
  @DisplayName("failure cannot freeze a value below one already published to a poller")
  void failureFreezesAtLeastThePublishedMaximum() throws Exception {
    assertTerminalNeverRegresses(job -> {});
  }

  /**
   * Drives the race the reviewer reproduced: a terminal transition and a concurrent poll, with the
   * observer advancing in between. Whatever the interleaving, the frozen terminal fraction must be
   * at least the highest value any poll returned.
   */
  private void assertTerminalNeverRegresses(java.util.function.Consumer<QueryJob> cancelAction)
      throws Exception {
    ProgressRunner runner = new ProgressRunner();
    QueryJob job = newJob(runner);
    job.startRunner();
    runner.set(0.4);

    ExecutorService pool = Executors.newFixedThreadPool(2);
    try {
      CountDownLatch start = new CountDownLatch(1);
      AtomicReference<Double> highestPublished = new AtomicReference<>(0.0);

      var poller =
          pool.submit(
              () -> {
                awaitQuietly(start);
                for (int i = 0; i < 2_000; i++) {
                  runner.set(Math.min(0.8, 0.4 + i * 0.0002));
                  double seen = job.status().progress().fractionDone();
                  highestPublished.updateAndGet(previous -> Math.max(previous, seen));
                }
              });
      var terminator =
          pool.submit(
              () -> {
                awaitQuietly(start);
                cancelAction.accept(job);
                runner.fail(new IllegalStateException("boom"));
              });

      start.countDown();
      poller.get(30, TimeUnit.SECONDS);
      terminator.get(30, TimeUnit.SECONDS);

      QueryJobStatus terminal = job.status();
      assertTrue(terminal.state().isTerminal(), "job should be terminal");
      assertTrue(
          terminal.progress().fractionDone() >= highestPublished.get() - 1e-12,
          "terminal fraction "
              + terminal.progress().fractionDone()
              + " regressed below already-published "
              + highestPublished.get());
      assertTrue(terminal.progress().fractionDone() < 1.0, "terminal failure must not report 1.0");
    } finally {
      pool.shutdownNow();
    }
  }

  // ------------------------------------------------------------------ P1-TIMEOUT-FREEZE

  @Test
  @DisplayName(
      "a submit timeout landing mid-cancellation reports the frozen value, not a later one")
  void timeoutDuringCancellationUsesFrozenProgress() throws Exception {
    ProgressRunner runner = new ProgressRunner();
    QueryJob job = newJob(runner);
    job.startRunner();
    runner.set(0.4);

    // Park inside runner.cancel(), which QueryJob calls after committing CANCELLED but before
    // completing the
    // internal future. That is the window where the submit timeout can still produce a RUNNING
    // payload while
    // the job is already terminal — the exact interleaving that used to publish a value the next
    // GET would
    // contradict.
    runner.blockCancellation();
    ExecutorService pool = Executors.newSingleThreadExecutor();
    try {
      var canceller = pool.submit(() -> job.cancel());
      runner.awaitInsideCancel();

      // Late source callbacks advance the observer while the job is already CANCELLED at 0.4.
      runner.set(0.8);

      QueryResult settled = job.await(Duration.ZERO).toCompletableFuture().get(5, TimeUnit.SECONDS);
      QueryResult.Running running = assertRunning(settled);
      assertEquals(
          0.4,
          running.progress().fractionDone(),
          "submit payload must use the frozen value, not the advanced observer");

      runner.releaseCancellation();
      canceller.get(5, TimeUnit.SECONDS);
    } finally {
      pool.shutdownNow();
    }

    assertSame(QueryJobState.CANCELLED, job.status().state());
    assertEquals(0.4, job.status().progress().fractionDone());
  }

  @Test
  @DisplayName(
      "a wait on an already-cancelled job surfaces the cancellation and leaves progress frozen")
  void waitAfterCancellationSurfacesCancellation() {
    ProgressRunner runner = new ProgressRunner();
    QueryJob job = newJob(runner);
    job.startRunner();
    runner.set(0.4);
    job.cancel();
    runner.set(0.8);

    CompletionStage<QueryResult> awaited = job.await(Duration.ZERO);
    assertThrows(
        java.util.concurrent.ExecutionException.class,
        () -> awaited.toCompletableFuture().get(5, TimeUnit.SECONDS));
    assertSame(QueryJobState.CANCELLED, job.status().state());
    assertEquals(0.4, job.status().progress().fractionDone());
  }

  @Test
  @DisplayName("the RUNNING payload from a submit timeout carries the published fraction")
  void timeoutPayloadCarriesPublishedFraction() throws Exception {
    ProgressRunner runner = new ProgressRunner();
    QueryJob job = newJob(runner);
    job.startRunner();
    runner.set(0.55);

    QueryResult settled = job.await(Duration.ZERO).toCompletableFuture().get(5, TimeUnit.SECONDS);
    QueryResult.Running running = assertRunning(settled);
    assertEquals(0.55, running.progress().fractionDone());
  }

  @Test
  @DisplayName("a RUNNING payload never claims completion")
  void timeoutPayloadNeverReportsOne() throws Exception {
    ProgressRunner runner = new ProgressRunner();
    QueryJob job = newJob(runner);
    job.startRunner();
    runner.set(1.0);

    QueryResult.Running running =
        assertRunning(job.await(Duration.ZERO).toCompletableFuture().get(5, TimeUnit.SECONDS));
    assertTrue(running.progress().fractionDone() < 1.0);
    assertEquals(CEILING, running.progress().fractionDone());
  }

  @RepeatedTest(20)
  @DisplayName("a timed wait racing cancellation never publishes above the frozen value")
  void timedWaitRacingCancellationStaysConsistent() throws Exception {
    ProgressRunner runner = new ProgressRunner();
    QueryJob job = newJob(runner);
    job.startRunner();
    runner.set(0.4);

    CompletionStage<QueryResult> awaited = job.await(Duration.ofMillis(30));
    ExecutorService pool = Executors.newSingleThreadExecutor();
    try {
      CountDownLatch start = new CountDownLatch(1);
      var terminator =
          pool.submit(
              () -> {
                awaitQuietly(start);
                job.cancel();
                runner.set(0.8);
              });
      start.countDown();
      terminator.get(30, TimeUnit.SECONDS);

      QueryProgress statusProgress = job.status().progress();
      try {
        QueryResult settled = awaited.toCompletableFuture().get(5, TimeUnit.SECONDS);
        if (settled instanceof QueryResult.Running running) {
          assertTrue(
              running.progress().fractionDone() <= statusProgress.fractionDone() + 1e-12,
              "submit payload "
                  + running.progress().fractionDone()
                  + " exceeded the frozen status value "
                  + statusProgress.fractionDone());
        }
      } catch (java.util.concurrent.ExecutionException expected) {
        // Cancellation won the race and the wait completed exceptionally. Nothing to compare.
      }
      assertEquals(0.4, job.status().progress().fractionDone());
    } finally {
      pool.shutdownNow();
    }
  }

  // ------------------------------------------------------------------ helpers

  /** Builds a job over the runner's own observer, so the job samples what the test sets. */
  private static QueryJob newJob(ProgressRunner runner) {
    return new QueryJob(
        ID,
        OWNER,
        runner,
        Clock.fixed(Instant.ofEpochMilli(1_000), ZoneOffset.UTC),
        null,
        runner.observer());
  }

  private static QueryResult.Running assertRunning(QueryResult result) {
    assertTrue(result instanceof QueryResult.Running, "expected a Running marker, got " + result);
    return (QueryResult.Running) result;
  }

  private static void awaitQuietly(CountDownLatch latch) {
    try {
      latch.await();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(e);
    }
  }

  /**
   * Drives one job's lifecycle. Completion and cancellation come from here; the fraction does not —
   * the job samples the {@link FakeObserver} it was constructed with. The observer is held here
   * only so each test needs a single handle.
   */
  private static final class ProgressRunner implements QueryRunner {

    private final CompletableFuture<QueryResult> future = new CompletableFuture<>();
    private final FakeObserver observer = new FakeObserver();

    /** Gates {@link #cancel()} so a test can hold the job inside its cancellation path. */
    private volatile CountDownLatch cancelGate;

    private final CountDownLatch insideCancel = new CountDownLatch(1);

    @Override
    public CompletionStage<QueryResult> run(ResultListeners listeners) {
      return future;
    }

    ProgressObserver observer() {
      return observer;
    }

    @Override
    public void cancel() {
      CountDownLatch gate = cancelGate;
      if (gate != null) {
        insideCancel.countDown();
        awaitQuietly(gate);
      }
      future.cancel(false);
    }

    void blockCancellation() {
      cancelGate = new CountDownLatch(1);
    }

    void awaitInsideCancel() {
      awaitQuietly(insideCancel);
    }

    void releaseCancellation() {
      CountDownLatch gate = cancelGate;
      if (gate != null) {
        gate.countDown();
      }
    }

    void set(double fraction) {
      observer.set(fraction);
    }

    void setThrowing(boolean value) {
      observer.setThrowing(value);
    }

    void setReturningNull(boolean value) {
      observer.setReturningNull(value);
    }

    void complete(QueryResult result) {
      future.complete(result);
    }

    void fail(Throwable throwable) {
      future.completeExceptionally(throwable);
    }
  }

  /**
   * Observer whose reported fraction a test sets directly, including the two ways a real
   * engine-side implementation could misbehave: returning {@code null} and throwing.
   */
  private static final class FakeObserver implements ProgressObserver {

    private final AtomicReference<QueryProgress> progress =
        new AtomicReference<>(QueryProgress.ZERO);
    private final AtomicBoolean throwing = new AtomicBoolean();
    private final AtomicBoolean returningNull = new AtomicBoolean();

    @Override
    public void register(long sourceId, OptionalLong estimatedDocs) {}

    @Override
    public void seal() {}

    @Override
    public void accept(SourceProgressEvent event) {}

    @Override
    public QueryProgress current() {
      if (throwing.get()) {
        throw new IllegalStateException("observer blew up");
      }
      return returningNull.get() ? null : progress.get();
    }

    void set(double fraction) {
      progress.set(new QueryProgress(fraction));
    }

    void setThrowing(boolean value) {
      throwing.set(value);
    }

    void setReturningNull(boolean value) {
      returningNull.set(value);
    }
  }
}
