/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.job;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.executor.pagination.Cursor;
import org.opensearch.sql.opensearch.executor.WorkerPoolSaturatedException;
import org.opensearch.threadpool.Scheduler.ScheduledCancellable;
import org.opensearch.threadpool.ThreadPool;

class OpenSearchQueryJobSchedulerTest {

  private static final Principal ALICE = new Principal("alice", null, List.of());
  private static final QueryResult ROWS =
      new QueryResult.Rows(
          new ExecutionEngine.Schema(List.of()), List.of(), Cursor.None, List.of(), 0);

  private QueryJobQuotaTest.MutableSettings settings;
  private ThreadPool threadPool;
  private ScheduledCancellable timer;
  private OpenSearchQueryJobScheduler scheduler;
  private int nextId;

  @BeforeEach
  void setUp() {
    settings = QueryJobQuotaTest.settings(1, 100, 1_000);
    threadPool = mock(ThreadPool.class);
    timer = mock(ScheduledCancellable.class);
    when(threadPool.schedule(any(Runnable.class), any(TimeValue.class), any())).thenReturn(timer);
    scheduler = new OpenSearchQueryJobScheduler(settings, threadPool);
  }

  @Test
  void startsUpToTheLimitAndQueuesTheRestInOrder() {
    settings.put(Settings.Key.PPL_ASYNC_NODE_CONCURRENT_RUNNING_QUERIES, 2);
    Runner a = new Runner();
    Runner b = new Runner();
    Runner c = new Runner();
    Runner d = new Runner();
    QueryJob jobA = submit(a);
    submit(b);
    QueryJob jobC = submit(c);
    QueryJob jobD = submit(d);

    assertTrue(a.ran && b.ran);
    assertFalse(c.ran || d.ran);
    assertEquals(QueryJobState.PENDING, jobC.status().state());
    assertEquals(2, scheduler.running());
    assertEquals(2, scheduler.queued());

    a.future.complete(ROWS);

    assertEquals(QueryJobState.SUCCEEDED, jobA.status().state());
    assertTrue(c.ran);
    assertFalse(d.ran);
    assertEquals(QueryJobState.PENDING, jobD.status().state());
  }

  @Test
  void finishedJobCancelsItsWaitDeadline() {
    Runner first = new Runner();
    submit(first);
    Runner queued = new Runner();
    submit(queued);
    first.future.complete(ROWS);
    assertTrue(queued.ran);
    verify(timer, never()).cancel();

    queued.future.complete(ROWS);

    verify(timer).cancel();
  }

  @Test
  void cancelledQueuedJobLeavesTheQueueAndNeverStarts() {
    Runner first = new Runner();
    submit(first);
    Runner queued = new Runner();
    QueryJob job = submit(queued);

    job.cancel();
    first.future.complete(ROWS);

    assertEquals(QueryJobState.CANCELLED, job.status().state());
    assertFalse(queued.ran);
    assertEquals(0, scheduler.queued());
    assertEquals(0, scheduler.running());
    verify(timer).cancel();
  }

  @Test
  void jobThatWaitsTooLongFailsWithTheQueueTimeout() {
    settings.put(Settings.Key.PPL_ASYNC_MAX_QUEUED_TIME, TimeValue.timeValueSeconds(30));
    submit(new Runner());
    Runner queued = new Runner();
    QueryJob job = submit(queued);
    ArgumentCaptor<Runnable> timeout = ArgumentCaptor.forClass(Runnable.class);
    verify(threadPool)
        .schedule(
            timeout.capture(), eq(TimeValue.timeValueSeconds(30)), eq(ThreadPool.Names.GENERIC));

    timeout.getValue().run();

    QueryJobStatus status = job.status();
    assertEquals(QueryJobState.FAILED, status.state());
    assertEquals(
        "Async query waited longer than [30s] for capacity"
            + " [plugins.ppl.async.max_queued_time]; retry later",
        status.failure().orElseThrow().reason());
    assertFalse(queued.ran);
    assertEquals(0, scheduler.queued());
  }

  @Test
  void queueTimeoutAfterTheJobStartedIsIgnored() {
    Runner first = new Runner();
    submit(first);
    QueryJob job = submit(new Runner());
    ArgumentCaptor<Runnable> timeout = ArgumentCaptor.forClass(Runnable.class);
    verify(threadPool).schedule(timeout.capture(), any(TimeValue.class), any());
    first.future.complete(ROWS);

    timeout.getValue().run();

    assertEquals(QueryJobState.RUNNING, job.status().state());
  }

  @Test
  void refusedJobBacksOffAndThenRunsAgain() {
    Runner retry = new Runner();
    Runner refused = new Runner(retry);
    QueryJob job = submit(refused);

    refused.future.completeExceptionally(new WorkerPoolSaturatedException("sql-worker", false));

    assertEquals(QueryJobState.PENDING, job.status().state());
    assertTrue(job.status().startedAtMillis().isEmpty());
    assertEquals(0, scheduler.running());
    assertEquals(1, scheduler.queued());
    Runnable backoff = captureSchedule(OpenSearchQueryJobScheduler.INITIAL_BACKOFF);
    backoff.run();
    retry.future.complete(ROWS);

    assertTrue(retry.ran);
    assertEquals(QueryJobState.SUCCEEDED, job.status().state());
    assertEquals(0, scheduler.running());
  }

  @Test
  void otherJobsUseTheSlotWhileARefusedJobBacksOff() {
    Runner retry = new Runner();
    Runner refused = new Runner(retry);
    submit(refused);
    Runner waiting = new Runner();
    submit(waiting);

    refused.future.completeExceptionally(new WorkerPoolSaturatedException("sql-worker", false));

    assertTrue(waiting.ran);
    Runner later = new Runner();
    submit(later);
    captureSchedule(OpenSearchQueryJobScheduler.INITIAL_BACKOFF).run();
    waiting.future.complete(ROWS);
    assertTrue(retry.ran);
    assertFalse(later.ran);
  }

  @Test
  void newSubmitsStartWhileARefusedJobBacksOff() {
    settings.put(Settings.Key.PPL_ASYNC_NODE_CONCURRENT_RUNNING_QUERIES, 2);
    Runner refused = new Runner(new Runner());
    submit(refused);
    refused.future.completeExceptionally(new WorkerPoolSaturatedException("sql-worker", false));

    Runner fresh = new Runner();
    submit(fresh);

    assertTrue(fresh.ran);
  }

  @Test
  void backoffDoublesFromTheInitialDelayUpToTheCap() {
    assertEquals(TimeValue.timeValueMillis(500), OpenSearchQueryJobScheduler.backoff(1));
    assertEquals(TimeValue.timeValueSeconds(1), OpenSearchQueryJobScheduler.backoff(2));
    assertEquals(TimeValue.timeValueSeconds(2), OpenSearchQueryJobScheduler.backoff(3));
    assertEquals(TimeValue.timeValueSeconds(4), OpenSearchQueryJobScheduler.backoff(4));
    assertEquals(TimeValue.timeValueSeconds(8), OpenSearchQueryJobScheduler.backoff(5));
    assertEquals(TimeValue.timeValueSeconds(8), OpenSearchQueryJobScheduler.backoff(6));
    assertEquals(TimeValue.timeValueSeconds(8), OpenSearchQueryJobScheduler.backoff(100));
  }

  @Test
  void eachRefusalOfTheSameJobBacksOffLonger() {
    RefusingRunner refusing = new RefusingRunner();
    submit(refusing);
    captureSchedule(OpenSearchQueryJobScheduler.backoff(1)).run();

    captureSchedule(OpenSearchQueryJobScheduler.backoff(2)).run();

    captureSchedule(OpenSearchQueryJobScheduler.backoff(3));
    assertEquals(3, refusing.attempts);
  }

  @Test
  void waitDeadlineFailsAJobThatIsBackingOff() {
    settings.put(Settings.Key.PPL_ASYNC_MAX_QUEUED_TIME, TimeValue.timeValueSeconds(30));
    RefusingRunner refusing = new RefusingRunner();
    QueryJob job = submit(refusing);
    Runnable deadline = captureSchedule(TimeValue.timeValueSeconds(30));

    deadline.run();

    QueryJobStatus status = job.status();
    assertEquals(QueryJobState.FAILED, status.state());
    assertEquals(
        "Async query waited longer than [30s] for capacity"
            + " [plugins.ppl.async.max_queued_time]; retry later",
        status.failure().orElseThrow().reason());
    assertEquals(0, scheduler.queued());
  }

  @Test
  void waitDeadlineCountsFromTheFirstWaitAcrossRefusals() {
    settings.put(Settings.Key.PPL_ASYNC_MAX_QUEUED_TIME, TimeValue.timeValueSeconds(30));
    RefusingRunner refusing = new RefusingRunner();
    submit(refusing);
    captureSchedule(OpenSearchQueryJobScheduler.backoff(1)).run();

    verify(threadPool, times(1))
        .schedule(any(Runnable.class), eq(TimeValue.timeValueSeconds(30)), any());
  }

  @Test
  void refusalAfterTheWaitDeadlineFailsTheJob() {
    settings.put(Settings.Key.PPL_ASYNC_MAX_QUEUED_TIME, TimeValue.timeValueSeconds(30));
    Runner first = new Runner();
    submit(first);
    Runner refused = new Runner(new Runner());
    QueryJob job = submit(refused);
    Runnable deadline = captureSchedule(TimeValue.timeValueSeconds(30));
    first.future.complete(ROWS);
    deadline.run();
    assertEquals(QueryJobState.RUNNING, job.status().state());

    refused.future.completeExceptionally(new WorkerPoolSaturatedException("sql-worker", false));

    assertEquals(QueryJobState.FAILED, job.status().state());
    assertEquals(0, scheduler.queued());
  }

  /** Runner refused by a saturated pool on every attempt, synchronously like sql-worker. */
  private static final class RefusingRunner implements QueryRunner {
    private int attempts;

    @Override
    public CompletionStage<QueryResult> run() {
      attempts++;
      return CompletableFuture.failedFuture(new WorkerPoolSaturatedException("sql-worker", false));
    }

    @Override
    public void cancel() {}

    @Override
    public QueryRunner newAttempt() {
      return this;
    }
  }

  /** Returns the last task scheduled on the generic pool with {@code delay}. */
  private Runnable captureSchedule(TimeValue delay) {
    ArgumentCaptor<Runnable> task = ArgumentCaptor.forClass(Runnable.class);
    verify(threadPool, atLeastOnce())
        .schedule(task.capture(), eq(delay), eq(ThreadPool.Names.GENERIC));
    return task.getValue();
  }

  @Test
  void fullQueueRejectsTheSubmit() {
    settings.put(Settings.Key.PPL_ASYNC_MAX_QUEUED_QUERIES, 1);
    submit(new Runner());
    submit(new Runner());
    Runner rejected = new Runner();

    OpenSearchStatusException e =
        assertThrows(OpenSearchStatusException.class, () -> submit(rejected));

    assertEquals(RestStatus.TOO_MANY_REQUESTS, e.status());
    assertFalse(rejected.ran);
    assertEquals(1, scheduler.queued());
  }

  @Test
  void zeroQueueRejectsWhenEverySlotIsBusy() {
    settings.put(Settings.Key.PPL_ASYNC_MAX_QUEUED_QUERIES, 0);
    submit(new Runner());

    assertThrows(OpenSearchStatusException.class, () -> submit(new Runner()));
    assertEquals(0, scheduler.queued());
  }

  @Test
  void refusedJobReentersTheQueueEvenWhenItIsFull() {
    settings.put(Settings.Key.PPL_ASYNC_MAX_QUEUED_QUERIES, 1);
    Runner refused = new Runner(new Runner());
    QueryJob job = submit(refused);
    Runner second = new Runner();
    submit(second);
    refused.future.completeExceptionally(new WorkerPoolSaturatedException("sql-worker", false));
    assertTrue(second.ran);
    submit(new Runner());
    assertThrows(OpenSearchStatusException.class, () -> submit(new Runner()));

    captureSchedule(OpenSearchQueryJobScheduler.INITIAL_BACKOFF).run();

    assertEquals(QueryJobState.PENDING, job.status().state());
    assertEquals(2, scheduler.queued());
  }

  @Test
  void rejectionWithoutAFreshAttemptFailsTheJob() {
    Runner refused = new Runner();
    QueryJob job = submit(refused);

    refused.future.completeExceptionally(new WorkerPoolSaturatedException("sql-worker", false));

    assertEquals(QueryJobState.FAILED, job.status().state());
    assertEquals(0, scheduler.running());
  }

  @Test
  void otherFailuresFailTheJob() {
    Runner failing = new Runner(new Runner());
    QueryJob job = submit(failing);

    failing.future.completeExceptionally(new IllegalStateException("boom"));

    assertEquals(QueryJobState.FAILED, job.status().state());
    assertEquals(0, scheduler.queued());
  }

  @Test
  void raisedLimitStartsMoreJobsOnTheNextRelease() {
    Runner first = new Runner();
    submit(first);
    Runner second = new Runner();
    Runner third = new Runner();
    submit(second);
    submit(third);
    settings.put(Settings.Key.PPL_ASYNC_NODE_CONCURRENT_RUNNING_QUERIES, 3);

    first.future.complete(ROWS);

    assertTrue(second.ran && third.ran);
    assertEquals(2, scheduler.running());
  }

  @Test
  void recognizesWrappedCapacityRejections() {
    assertTrue(
        scheduler.isCapacityRejection(
            new RuntimeException(new WorkerPoolSaturatedException("sql-complex-worker", false))));
    assertFalse(scheduler.isCapacityRejection(new IllegalStateException("boom")));
  }

  private QueryJob submit(QueryRunner runner) {
    QueryJob job =
        new QueryJob(
            new QueryJobId("node-a", "ctx-" + nextId++),
            ALICE,
            runner,
            Clock.systemUTC(),
            null,
            ResultBudget.UNLIMITED,
            scheduler);
    scheduler.submit(job);
    return job;
  }

  /** Runner whose completion the test drives; optionally supplies one fresh attempt. */
  private static final class Runner implements QueryRunner {
    private final CompletableFuture<QueryResult> future = new CompletableFuture<>();
    private final List<Runner> attempts = new ArrayList<>();
    private boolean ran;

    Runner() {}

    Runner(Runner nextAttempt) {
      attempts.add(nextAttempt);
    }

    @Override
    public CompletionStage<QueryResult> run() {
      ran = true;
      return future;
    }

    @Override
    public void cancel() {
      future.cancel(false);
    }

    @Override
    public QueryRunner newAttempt() {
      return attempts.isEmpty() ? null : attempts.remove(0);
    }
  }
}
