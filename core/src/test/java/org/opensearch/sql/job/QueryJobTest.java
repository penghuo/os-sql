/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.job;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.executor.ExecutionEngine.Schema;
import org.opensearch.sql.executor.pagination.Cursor;

class QueryJobTest {

  private static final QueryJobId ID = new QueryJobId("node-1", "ctx-1");
  private static final Principal OWNER = new Principal("alice", null, List.of());
  private static final Schema SCHEMA = new Schema(List.of());
  private static final QueryResult RESULT =
      new QueryResult.Rows(SCHEMA, List.of(), Cursor.None, List.of(), 0);
  private static final QueryResult ROWS =
      new QueryResult.Rows(
          SCHEMA,
          List.of(ExprValueUtils.tupleValue(java.util.Map.of("host", "host-1"))),
          Cursor.None,
          List.of(),
          0);

  @Test
  void status_startsInPending() {
    RecordingRunner runner = new RecordingRunner();
    QueryJob job =
        new QueryJob(ID, OWNER, runner, Clock.fixed(Instant.ofEpochMilli(1_000), ZoneOffset.UTC));
    QueryJobStatus status = job.status();
    assertEquals(QueryJobState.PENDING, status.state());
    assertEquals(1_000L, status.submittedAtMillis());
    assertTrue(status.startedAtMillis().isEmpty());
    assertTrue(status.completedAtMillis().isEmpty());
  }

  @Test
  void success_transitionsToSucceededAndFiresOnTerminal() {
    RecordingRunner runner = new RecordingRunner();
    QueryJob job = newJob(runner, 10L);
    AtomicInteger fired = new AtomicInteger();
    job.onTerminal(fired::incrementAndGet);
    job.startRunner();
    runner.complete(RESULT);
    assertEquals(1, fired.get());
    QueryJobStatus status = job.status();
    assertEquals(QueryJobState.SUCCEEDED, status.state());
    assertTrue(status.result().isPresent());
    assertTrue(status.completedAtMillis().isPresent());
    assertTrue(status.startedAtMillis().isPresent());
  }

  @Test
  void failure_transitionsToFailedAndCarriesTypedFailure() {
    RecordingRunner runner = new RecordingRunner();
    QueryJob job = newJob(runner, 20L);
    job.startRunner();
    IllegalStateException cause = new IllegalStateException("boom");
    runner.fail(cause);
    QueryJobStatus status = job.status();
    assertEquals(QueryJobState.FAILED, status.state());
    assertEquals("IllegalStateException", status.failure().orElseThrow().type());
    assertEquals(java.util.Map.of(), status.failure().orElseThrow().details());
  }

  @Test
  void failure_appliesRendererForStructuredDetails() {
    RecordingRunner runner = new RecordingRunner();
    QueryJob job =
        new QueryJob(
            ID,
            OWNER,
            runner,
            Clock.fixed(Instant.ofEpochMilli(25L), ZoneOffset.UTC),
            t -> java.util.Map.of("code", "FIELD_NOT_FOUND", "reason", t.getMessage()));
    job.startRunner();
    runner.fail(new IllegalArgumentException("Field [x] not found."));
    QueryFailure failure = job.status().failure().orElseThrow();
    assertEquals("FIELD_NOT_FOUND", failure.details().get("code"));
    assertEquals("Field [x] not found.", failure.details().get("reason"));
  }

  @Test
  void failure_rendererBugDoesNotBlockTerminalTransition() {
    RecordingRunner runner = new RecordingRunner();
    QueryJob job =
        new QueryJob(
            ID,
            OWNER,
            runner,
            Clock.fixed(Instant.ofEpochMilli(26L), ZoneOffset.UTC),
            t -> {
              throw new RuntimeException("renderer bug");
            });
    job.startRunner();
    runner.fail(new IllegalStateException("boom"));
    assertEquals(QueryJobState.FAILED, job.status().state());
    assertEquals(java.util.Map.of(), job.status().failure().orElseThrow().details());
  }

  @Test
  void cancel_beforeStart_skipsRunnerAndFiresOnTerminal() {
    RecordingRunner runner = new RecordingRunner();
    QueryJob job = newJob(runner, 30L);
    AtomicInteger fired = new AtomicInteger();
    job.onTerminal(fired::incrementAndGet);
    job.cancel();
    job.startRunner();
    assertEquals(QueryJobState.CANCELLED, job.status().state());
    assertFalse(runner.wasRun());
    assertTrue(runner.wasCancelled());
    assertEquals(1, fired.get());
  }

  @Test
  void cancel_afterStart_movesToCancelledAndCancelsRunner() {
    RecordingRunner runner = new RecordingRunner();
    QueryJob job = newJob(runner, 40L);
    job.startRunner();
    job.cancel();
    assertEquals(QueryJobState.CANCELLED, job.status().state());
    assertTrue(runner.wasCancelled());
  }

  @Test
  void cancel_isIdempotentInTerminalState() {
    RecordingRunner runner = new RecordingRunner();
    QueryJob job = newJob(runner, 50L);
    job.startRunner();
    runner.complete(RESULT);
    QueryJobStatus terminal = job.status();
    job.cancel();
    assertEquals(terminal, job.status());
  }

  @Test
  void startRunner_secondInvocation_isNoOp() {
    RecordingRunner runner = new RecordingRunner();
    QueryJob job = newJob(runner, 70L);
    job.startRunner();
    job.startRunner();
    assertEquals(1, runner.runInvocations());
  }

  @Test
  void nullResultFromRunner_transitionsToFailed() {
    RecordingRunner runner = new RecordingRunner();
    QueryJob job = newJob(runner, 80L);
    job.startRunner();
    runner.complete(null);
    assertEquals(QueryJobState.FAILED, job.status().state());
  }

  @Test
  void await_returnsRunnerResultWhenCompletesBeforeBudget() throws Exception {
    RecordingRunner runner = new RecordingRunner();
    QueryJob job = newJob(runner, 90L);
    job.startRunner();
    CompletionStage<QueryResult> stage = job.await(Duration.ofSeconds(30));
    runner.complete(RESULT);
    assertSame(RESULT, stage.toCompletableFuture().get(1, TimeUnit.SECONDS));
  }

  @Test
  void await_returnsRunningWhenBudgetExpiresFirst() throws Exception {
    RecordingRunner runner = new RecordingRunner();
    QueryJob job = newJob(runner, 100L);
    job.startRunner();
    QueryResult result =
        job.await(Duration.ofMillis(20)).toCompletableFuture().get(1, TimeUnit.SECONDS);
    QueryResult.Running running = assertInstanceOf(QueryResult.Running.class, result);
    assertEquals(ID, running.id());
    runner.complete(RESULT);
    assertEquals(QueryJobState.SUCCEEDED, job.status().state());
  }

  @Test
  void await_completesExceptionallyWithUnwrappedCause() {
    RecordingRunner runner = new RecordingRunner();
    QueryJob job = newJob(runner, 110L);
    job.startRunner();
    CompletionStage<QueryResult> stage = job.await(Duration.ofSeconds(30));
    IllegalStateException cause = new IllegalStateException("boom");
    runner.fail(cause);
    ExecutionException ex =
        assertThrows(
            ExecutionException.class, () -> stage.toCompletableFuture().get(1, TimeUnit.SECONDS));
    assertSame(cause, ex.getCause());
  }

  @Test
  void await_alreadyTerminal_returnsSynchronously() {
    RecordingRunner runner = new RecordingRunner();
    QueryJob job = newJob(runner, 120L);
    job.startRunner();
    runner.complete(RESULT);
    QueryResult result = job.await(Duration.ofSeconds(1)).toCompletableFuture().getNow(null);
    assertNotNull(result, "already-terminal job must complete synchronously");
    assertSame(RESULT, result);
  }

  @Test
  void await_zeroBudget_returnsRunningImmediately() {
    RecordingRunner runner = new RecordingRunner();
    QueryJob job = newJob(runner, 130L);
    job.startRunner();
    QueryResult result = job.await(Duration.ZERO).toCompletableFuture().getNow(null);
    assertNotNull(result, "zero budget must not block");
    assertInstanceOf(QueryResult.Running.class, result);
  }

  @Test
  void await_cancellationSurfacesAsExceptionalCompletion() {
    RecordingRunner runner = new RecordingRunner();
    QueryJob job = newJob(runner, 140L);
    job.startRunner();
    CompletionStage<QueryResult> stage = job.await(Duration.ofSeconds(30));
    job.cancel();
    ExecutionException ex =
        assertThrows(
            ExecutionException.class, () -> stage.toCompletableFuture().get(1, TimeUnit.SECONDS));
    assertInstanceOf(CancellationException.class, ex.getCause());
  }

  @Test
  void await_lateTimerAfterEarlyCompletionDoesNotOverride() throws Exception {
    RecordingRunner runner = new RecordingRunner();
    QueryJob job = newJob(runner, 150L);
    job.startRunner();
    CompletionStage<QueryResult> stage = job.await(Duration.ofMillis(50));
    runner.complete(RESULT);
    QueryResult result = stage.toCompletableFuture().get(1, TimeUnit.SECONDS);
    assertSame(RESULT, result);
    Thread.sleep(120);
    assertSame(result, stage.toCompletableFuture().getNow(null));
  }

  @Test
  void onTerminal_rejectsNull() {
    RecordingRunner runner = new RecordingRunner();
    QueryJob job = newJob(runner, 160L);
    assertThrows(NullPointerException.class, () -> job.onTerminal(null));
  }

  @Test
  void retain_chargesResultThatArrivesLater() {
    RecordingRunner runner = new RecordingRunner();
    CountingBudget budget = new CountingBudget(Long.MAX_VALUE);
    QueryJob job = newJob(runner, budget);
    job.startRunner();
    job.retain();
    runner.complete(ROWS);
    assertEquals(QueryJobState.SUCCEEDED, job.status().state());
    assertEquals(QueryResultSizeEstimator.estimate(ROWS), budget.charged);
  }

  @Test
  void retain_chargesResultThatArrivedBeforeRetention() {
    RecordingRunner runner = new RecordingRunner();
    CountingBudget budget = new CountingBudget(Long.MAX_VALUE);
    QueryJob job = newJob(runner, budget);
    job.startRunner();
    runner.complete(ROWS);
    assertEquals(0L, budget.charged);
    job.retain();
    job.retain();
    assertEquals(QueryResultSizeEstimator.estimate(ROWS), budget.charged);
  }

  @Test
  void success_withoutRetentionIsNeverCharged() {
    RecordingRunner runner = new RecordingRunner();
    CountingBudget budget = new CountingBudget(0L);
    QueryJob job = newJob(runner, budget);
    job.startRunner();
    runner.complete(ROWS);
    job.detach();
    assertEquals(QueryJobState.SUCCEEDED, job.status().state());
    assertEquals(0L, budget.charged);
  }

  @Test
  void refusedCharge_failsJobAndDropsRows() throws Exception {
    RecordingRunner runner = new RecordingRunner();
    CountingBudget budget = new CountingBudget(1L);
    QueryJob job = newJob(runner, budget);
    AtomicInteger fired = new AtomicInteger();
    job.onTerminal(fired::incrementAndGet);
    job.startRunner();
    CompletionStage<QueryResult> stage = job.await(Duration.ofSeconds(5));
    job.retain();
    runner.complete(ROWS);
    QueryJobStatus status = job.status();
    assertEquals(QueryJobState.FAILED, status.state());
    assertTrue(status.result().isEmpty());
    assertEquals("IllegalStateException", status.failure().orElseThrow().type());
    assertEquals(1, fired.get());
    assertEquals(0L, budget.charged);
    ExecutionException ex =
        assertThrows(
            ExecutionException.class, () -> stage.toCompletableFuture().get(1, TimeUnit.SECONDS));
    assertSame(budget.refusal, ex.getCause());
  }

  @Test
  void refusedCharge_onRetentionReplacesUnobservedSuccess() {
    RecordingRunner runner = new RecordingRunner();
    CountingBudget budget = new CountingBudget(1L);
    QueryJob job = newJob(runner, budget);
    job.startRunner();
    runner.complete(ROWS);
    job.retain();
    QueryJobStatus status = job.status();
    assertEquals(QueryJobState.FAILED, status.state());
    assertTrue(status.result().isEmpty());
    assertTrue(status.failure().isPresent());
  }

  @Test
  void detach_releasesChargeOnceAndStopsLaterCharges() {
    RecordingRunner runner = new RecordingRunner();
    CountingBudget budget = new CountingBudget(Long.MAX_VALUE);
    QueryJob job = newJob(runner, budget);
    job.startRunner();
    job.retain();
    runner.complete(ROWS);
    job.detach();
    job.detach();
    assertEquals(0L, budget.charged);
    assertEquals(1, budget.releases);

    RecordingRunner lateRunner = new RecordingRunner();
    QueryJob detachedFirst = newJob(lateRunner, budget);
    detachedFirst.startRunner();
    detachedFirst.retain();
    detachedFirst.detach();
    lateRunner.complete(ROWS);
    assertEquals(QueryJobState.SUCCEEDED, detachedFirst.status().state());
    assertEquals(0L, budget.charged);
  }

  @Test
  void charge_racingDetachAlwaysBalances() throws Exception {
    CountingBudget budget = new CountingBudget(Long.MAX_VALUE);
    for (int i = 0; i < 200; i++) {
      RecordingRunner runner = new RecordingRunner();
      QueryJob job = newJob(runner, budget);
      job.startRunner();
      job.retain();
      Thread completer = new Thread(() -> runner.complete(ROWS));
      completer.start();
      job.detach();
      completer.join();
    }
    assertEquals(0L, budget.charged);
  }

  @Test
  void terminalTransitionsReleaseTheRunner() {
    RecordingRunner succeeded = new RecordingRunner();
    QueryJob success = newJob(succeeded, 170L);
    success.startRunner();
    assertTrue(success.holdsRunner());
    succeeded.complete(RESULT);
    assertFalse(success.holdsRunner());

    RecordingRunner failed = new RecordingRunner();
    QueryJob failure = newJob(failed, 171L);
    failure.startRunner();
    failed.fail(new IllegalStateException("boom"));
    assertFalse(failure.holdsRunner());

    RecordingRunner cancelled = new RecordingRunner();
    QueryJob cancel = newJob(cancelled, 172L);
    cancel.startRunner();
    cancel.cancel();
    assertTrue(cancelled.cancelled);
    assertFalse(cancel.holdsRunner());

    QueryJob refused = newJob(new RecordingRunner(), new CountingBudget(1L));
    refused.startRunner();
    refused.retain();
    assertTrue(refused.holdsRunner());
  }

  @Test
  void capacityRejectionRequeuesWithAFreshAttempt() {
    RecordingRunner retry = new RecordingRunner();
    RetryingRunner refused = new RetryingRunner(retry);
    RecordingScheduler scheduler = new RecordingScheduler(true);
    QueryJob job = newJob(refused, scheduler);
    job.startRunner();

    refused.fail(new IllegalStateException("pool full"));

    assertEquals(QueryJobState.PENDING, job.status().state());
    assertTrue(job.status().startedAtMillis().isEmpty());
    assertEquals(List.of(job), scheduler.requeued);
    job.startRunner();
    retry.complete(RESULT);
    assertEquals(1, retry.runInvocations);
    assertEquals(QueryJobState.SUCCEEDED, job.status().state());
  }

  @Test
  void failureThatIsNotACapacityRejectionFailsTheJob() {
    RetryingRunner runner = new RetryingRunner(new RecordingRunner());
    RecordingScheduler scheduler = new RecordingScheduler(false);
    QueryJob job = newJob(runner, scheduler);
    job.startRunner();

    runner.fail(new IllegalStateException("boom"));

    assertEquals(QueryJobState.FAILED, job.status().state());
    assertTrue(scheduler.requeued.isEmpty());
  }

  @Test
  void failQueued_failsOnlyAPendingJob() {
    QueryJob pending = newJob(new RecordingRunner(), 180L);
    assertTrue(pending.failQueued(new IllegalStateException("waited too long")));
    assertEquals(QueryJobState.FAILED, pending.status().state());
    assertEquals("waited too long", pending.status().failure().orElseThrow().reason());

    RecordingRunner started = new RecordingRunner();
    QueryJob running = newJob(started, 181L);
    running.startRunner();
    assertFalse(running.failQueued(new IllegalStateException("late")));
    assertEquals(QueryJobState.RUNNING, running.status().state());
  }

  @Test
  void await_reportsPendingForAQueuedJob() throws Exception {
    QueryJob job = newJob(new RecordingRunner(), 182L);
    QueryResult.Running pending =
        assertInstanceOf(
            QueryResult.Running.class,
            job.await(Duration.ZERO).toCompletableFuture().get(1, TimeUnit.SECONDS));
    assertEquals(QueryJobState.PENDING, pending.state());

    job.startRunner();
    QueryResult.Running running =
        assertInstanceOf(
            QueryResult.Running.class,
            job.await(Duration.ofMillis(20)).toCompletableFuture().get(1, TimeUnit.SECONDS));
    assertEquals(QueryJobState.RUNNING, running.state());
  }

  private static QueryJob newJob(QueryRunner runner, QueryJobScheduler scheduler) {
    return new QueryJob(
        ID,
        OWNER,
        runner,
        Clock.fixed(Instant.ofEpochMilli(0L), ZoneOffset.UTC),
        null,
        ResultBudget.UNLIMITED,
        scheduler);
  }

  /** Scheduler that only records requeues and answers capacity checks with a fixed verdict. */
  private static final class RecordingScheduler implements QueryJobScheduler {
    private final boolean capacityRejection;
    private final List<QueryJob> requeued = new java.util.ArrayList<>();

    private RecordingScheduler(boolean capacityRejection) {
      this.capacityRejection = capacityRejection;
    }

    @Override
    public void submit(QueryJob job) {}

    @Override
    public boolean isCapacityRejection(Throwable failure) {
      return capacityRejection;
    }

    @Override
    public void requeue(QueryJob job) {
      requeued.add(job);
    }
  }

  /** Runner that can be failed by the test and supplies one fresh attempt. */
  private static final class RetryingRunner implements QueryRunner {
    private final CompletableFuture<QueryResult> future = new CompletableFuture<>();
    private QueryRunner nextAttempt;

    private RetryingRunner(QueryRunner nextAttempt) {
      this.nextAttempt = nextAttempt;
    }

    void fail(Throwable cause) {
      future.completeExceptionally(cause);
    }

    @Override
    public CompletionStage<QueryResult> run() {
      return future;
    }

    @Override
    public void cancel() {}

    @Override
    public QueryRunner newAttempt() {
      QueryRunner next = nextAttempt;
      nextAttempt = null;
      return next;
    }
  }

  private static QueryJob newJob(QueryRunner runner, ResultBudget budget) {
    return new QueryJob(
        ID, OWNER, runner, Clock.fixed(Instant.ofEpochMilli(0L), ZoneOffset.UTC), null, budget);
  }

  /** Budget with a fixed capacity that records the outstanding charge. */
  private static final class CountingBudget implements ResultBudget {
    private final long capacity;
    private final IllegalStateException refusal = new IllegalStateException("budget exhausted");
    private long charged;
    private int releases;

    private CountingBudget(long capacity) {
      this.capacity = capacity;
    }

    @Override
    public synchronized void reserve(long bytes) {
      if (bytes > capacity - charged) {
        throw refusal;
      }
      charged += bytes;
    }

    @Override
    public synchronized void release(long bytes) {
      charged -= bytes;
      releases++;
    }
  }

  private static QueryJob newJob(QueryRunner runner, long submittedMillis) {
    return new QueryJob(
        ID, OWNER, runner, Clock.fixed(Instant.ofEpochMilli(submittedMillis), ZoneOffset.UTC));
  }

  private static final class RecordingRunner implements QueryRunner {
    private final CompletableFuture<QueryResult> future = new CompletableFuture<>();
    private int runInvocations;
    private boolean cancelled;

    @Override
    public CompletionStage<QueryResult> run() {
      runInvocations++;
      return future;
    }

    @Override
    public void cancel() {
      cancelled = true;
    }

    void complete(QueryResult result) {
      future.complete(result);
    }

    void fail(Throwable throwable) {
      future.completeExceptionally(throwable);
    }

    boolean wasRun() {
      return runInvocations > 0;
    }

    boolean wasCancelled() {
      return cancelled;
    }

    int runInvocations() {
      return runInvocations;
    }
  }
}
