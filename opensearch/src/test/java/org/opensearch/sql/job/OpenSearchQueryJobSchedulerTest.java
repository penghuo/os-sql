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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.executor.pagination.Cursor;
import org.opensearch.sql.opensearch.executor.OpenSearchQueryManager;
import org.opensearch.sql.opensearch.executor.WorkerPoolSaturatedException;
import org.opensearch.tasks.CancellableTask;
import org.opensearch.threadpool.Scheduler.ScheduledCancellable;
import org.opensearch.threadpool.ThreadPool;

class OpenSearchQueryJobSchedulerTest {

  private static final Principal ALICE = new Principal("alice", null, List.of());
  private static final QueryResult ROWS =
      new QueryResult.Rows(
          new ExecutionEngine.Schema(List.of()), List.of(), Cursor.None, List.of(), 0);

  private MutableSettings settings;
  private final ThreadContext threadContext =
      new ThreadContext(org.opensearch.common.settings.Settings.EMPTY);
  private ThreadPool threadPool;
  private ScheduledCancellable timer;
  private OpenSearchQueryJobScheduler scheduler;
  private int nextId;

  @BeforeEach
  void setUp() {
    settings = settings(1);
    threadPool = mock(ThreadPool.class);
    timer = mock(ScheduledCancellable.class);
    when(threadPool.schedule(any(Runnable.class), any(TimeValue.class), any())).thenReturn(timer);
    when(threadPool.getThreadContext()).thenReturn(threadContext);
    scheduler = new OpenSearchQueryJobScheduler(settings, threadPool);
  }

  @AfterEach
  void tearDown() {
    OpenSearchQueryManager.clearCancellableTask();
    org.apache.logging.log4j.ThreadContext.clearMap();
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
    assertEquals(QueryJobState.RUNNING, jobC.status().state());
    assertFalse(d.ran);
    assertEquals(QueryJobState.PENDING, jobD.status().state());
  }

  @Test
  void startedJobCancelsItsWaitDeadline() {
    Runner first = new Runner();
    submit(first);
    Runner queued = new Runner();
    submit(queued);
    verify(timer, never()).cancel();

    first.future.complete(ROWS);

    assertTrue(queued.ran);
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

    captureDeadline(TimeValue.timeValueSeconds(30)).run();

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
  void waitDeadlineAfterTheJobStartedIsIgnored() {
    Runner first = new Runner();
    submit(first);
    QueryJob job = submit(new Runner());
    Runnable deadline = captureDeadline(TimeValue.timeValueMinutes(5));
    first.future.complete(ROWS);

    deadline.run();

    assertEquals(QueryJobState.RUNNING, job.status().state());
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
  void workerPoolRefusalFailsTheJobAndStartsTheNext() {
    Runner refused = new Runner();
    QueryJob job = submit(refused);
    Runner queued = new Runner();
    submit(queued);

    refused.future.completeExceptionally(new WorkerPoolSaturatedException("sql-worker"));

    QueryJobStatus status = job.status();
    assertEquals(QueryJobState.FAILED, status.state());
    assertEquals(
        "[sql-worker] thread pool is saturated; retry later",
        status.failure().orElseThrow().reason());
    assertTrue(queued.ran);
    assertEquals(1, scheduler.running());
    assertEquals(0, scheduler.queued());
  }

  @Test
  void synchronousRefusalOnStartFailsTheJobAndStartsTheNext() {
    QueryJob first = submit(new RefusingRunner());
    assertEquals(QueryJobState.FAILED, first.status().state());
    assertEquals(0, scheduler.running());

    Runner next = new Runner();
    submit(next);

    assertTrue(next.ran);
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
  void queuedJobStartsInItsSubmittersContextAndPutsBackTheStartingThreads() throws Exception {
    Runner first = new Runner();
    submit(first);
    CancellableTask submitterTask = mock(CancellableTask.class);
    ContextCapturingRunner queued = new ContextCapturingRunner(threadContext);
    try (ThreadContext.StoredContext ignored = threadContext.stashContext()) {
      threadContext.putHeader("submitter", "alice");
      org.apache.logging.log4j.ThreadContext.put("request_id", "req-1");
      OpenSearchQueryManager.setCancellableTask(submitterTask);
      submit(queued);
    }
    OpenSearchQueryManager.clearCancellableTask();
    org.apache.logging.log4j.ThreadContext.clearMap();

    CancellableTask starterTask = mock(CancellableTask.class);
    AtomicReference<String> headerAfter = new AtomicReference<>();
    AtomicReference<String> requestIdAfter = new AtomicReference<>();
    AtomicReference<CancellableTask> taskAfter = new AtomicReference<>();
    // The thread that finishes the running job starts the queued one.
    Thread starter =
        new Thread(
            () -> {
              threadContext.putHeader("submitter", "bob");
              org.apache.logging.log4j.ThreadContext.put("request_id", "req-2");
              OpenSearchQueryManager.setCancellableTask(starterTask);
              first.future.complete(ROWS);
              headerAfter.set(threadContext.getHeader("submitter"));
              requestIdAfter.set(org.apache.logging.log4j.ThreadContext.get("request_id"));
              taskAfter.set(OpenSearchQueryManager.getCancellableTask());
            });
    starter.start();
    starter.join();

    assertEquals("alice", queued.header.get());
    assertEquals("req-1", queued.requestId.get());
    assertEquals(submitterTask, queued.task.get());
    assertEquals("bob", headerAfter.get());
    assertEquals("req-2", requestIdAfter.get());
    assertEquals(starterTask, taskAfter.get());
  }

  /** Returns the wait deadline scheduled on the generic pool with {@code delay}. */
  private Runnable captureDeadline(TimeValue delay) {
    ArgumentCaptor<Runnable> task = ArgumentCaptor.forClass(Runnable.class);
    verify(threadPool).schedule(task.capture(), eq(delay), eq(ThreadPool.Names.GENERIC));
    return task.getValue();
  }

  private QueryJob submit(QueryRunner runner) {
    QueryJob job =
        new QueryJob(new QueryJobId("node-a", "ctx-" + nextId++), ALICE, runner, Clock.systemUTC());
    scheduler.submit(job);
    return job;
  }

  static MutableSettings settings(int maxRunning) {
    MutableSettings settings = new MutableSettings();
    settings.put(Settings.Key.PPL_ASYNC_NODE_CONCURRENT_RUNNING_QUERIES, maxRunning);
    settings.put(Settings.Key.PPL_ASYNC_MAX_QUEUED_QUERIES, 100);
    settings.put(Settings.Key.PPL_ASYNC_MAX_QUEUED_TIME, TimeValue.timeValueMinutes(5));
    return settings;
  }

  /** {@link Settings} backed by a map the test can change between calls. */
  static final class MutableSettings extends Settings {
    private final Map<Key, Object> values = new HashMap<>();

    void put(Key key, Object value) {
      values.put(key, value);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T getSettingValue(Key key) {
      return (T) values.get(key);
    }

    @Override
    public List<?> getSettings() {
      return List.of();
    }
  }

  /** Runner whose completion the test drives. */
  private static final class Runner implements QueryRunner {
    private final CompletableFuture<QueryResult> future = new CompletableFuture<>();
    private boolean ran;

    @Override
    public CompletionStage<QueryResult> run() {
      ran = true;
      return future;
    }

    @Override
    public void cancel() {
      future.cancel(false);
    }
  }

  /** Runner that records the context it starts in. */
  private static final class ContextCapturingRunner implements QueryRunner {
    private final ThreadContext threadContext;
    private final AtomicReference<String> header = new AtomicReference<>();
    private final AtomicReference<String> requestId = new AtomicReference<>();
    private final AtomicReference<CancellableTask> task = new AtomicReference<>();

    ContextCapturingRunner(ThreadContext threadContext) {
      this.threadContext = threadContext;
    }

    @Override
    public CompletionStage<QueryResult> run() {
      header.set(threadContext.getHeader("submitter"));
      requestId.set(org.apache.logging.log4j.ThreadContext.get("request_id"));
      task.set(OpenSearchQueryManager.getCancellableTask());
      return new CompletableFuture<>();
    }

    @Override
    public void cancel() {}
  }

  /** Runner refused by a saturated pool as it starts, synchronously like sql-worker. */
  private static final class RefusingRunner implements QueryRunner {
    @Override
    public CompletionStage<QueryResult> run() {
      throw new WorkerPoolSaturatedException("sql-worker");
    }

    @Override
    public void cancel() {}
  }
}
