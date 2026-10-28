/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.job;

import static org.opensearch.sql.opensearch.executor.OpenSearchQueryManager.SQL_WORKER_THREAD_POOL_NAME;

import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.IdentityHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.opensearch.executor.OpenSearchQueryManager;
import org.opensearch.tasks.CancellableTask;
import org.opensearch.threadpool.Scheduler.Cancellable;
import org.opensearch.threadpool.ThreadPool;

/**
 * Runs at most as many async jobs at once on this node as {@code sql-worker} has threads, and
 * queues up to {@code plugins.ppl.async.max_queued_queries} more in submission order; a submit
 * beyond both is rejected with {@code 429}, like a thread pool executor with a bounded work queue.
 * Queued jobs wait here as {@code PENDING} rather than in the worker pool's queue.
 *
 * <p>A queued job may wait for at most {@code plugins.ppl.async.max_queued_time}. After that it
 * ends {@code FAILED}.
 *
 * <p>A queued job starts later on whichever thread frees a slot. It starts in the context of the
 * thread that submitted it: the OpenSearch thread context (security user, headers, trace span), the
 * logging context, and the query task that cancellation reaches. The starting thread's own context
 * is put back afterwards.
 *
 * <p>A running slot does not reserve a thread: synchronous queries share the SQL worker pools, so a
 * running job may still wait in a pool's queue, and a job whose worker pool refuses it fails.
 *
 * <h2>Thread safety</h2>
 *
 * All state is guarded by {@code lock}. Jobs are started and failed after the lock is released, so
 * the lock order is always scheduler, then job.
 */
public final class OpenSearchQueryJobScheduler {

  private final Settings settings;
  private final ThreadPool threadPool;
  private final int maxRunning;
  private final Object lock = new Object();
  private final Set<QueryJob> running = Collections.newSetFromMap(new IdentityHashMap<>());
  private final Deque<Queued> queue = new ArrayDeque<>();
  // Fails a queued job that waited too long; armed when the job is queued.
  private final Map<QueryJob, Cancellable> waitDeadlines = new IdentityHashMap<>();
  // Marks a thread that is draining, so a drain triggered while starting a job does not nest.
  private final ThreadLocal<Boolean> draining = ThreadLocal.withInitial(() -> false);

  /**
   * @param settings plugin settings the limits are read from on every decision
   * @param threadPool sizes the running slots after {@code sql-worker} and runs the wait deadline
   *     timers on the generic pool
   * @throws NullPointerException if either argument is {@code null}
   */
  public OpenSearchQueryJobScheduler(Settings settings, ThreadPool threadPool) {
    this.settings = Objects.requireNonNull(settings, "settings must not be null");
    this.threadPool = Objects.requireNonNull(threadPool, "threadPool must not be null");
    this.maxRunning = threadPool.info(SQL_WORKER_THREAD_POOL_NAME).getMax();
  }

  /**
   * Starts {@code job} now, or queues it until a running slot frees. The job is {@code PENDING} and
   * already registered in the {@link QueryJobStore}.
   *
   * @param job the newly published job
   * @throws OpenSearchStatusException with status {@code 429} when no running slot is free and the
   *     queue already holds {@code plugins.ppl.async.max_queued_queries} jobs; the job is neither
   *     started nor queued
   */
  public void submit(QueryJob job) {
    Objects.requireNonNull(job, "job must not be null");
    int maxQueued = settings.getSettingValue(Settings.Key.PPL_ASYNC_MAX_QUEUED_QUERIES);
    boolean startNow;
    synchronized (lock) {
      startNow = queue.isEmpty() && running.size() < maxRunning;
      if (startNow) {
        running.add(job);
      } else if (queue.size() >= maxQueued) {
        throw new OpenSearchStatusException(
            String.format(
                Locale.ROOT,
                "Async query queue on this node is full [%s]; retry later",
                Settings.Key.PPL_ASYNC_MAX_QUEUED_QUERIES.getKeyValue()),
            RestStatus.TOO_MANY_REQUESTS);
      } else {
        armWaitDeadline(job);
        queue.addLast(Queued.capture(job, threadPool.getThreadContext()));
      }
    }
    job.onTerminal(() -> onTerminal(job));
    if (startNow) {
      job.startRunner();
    } else {
      // A slot may have freed between the check above and the enqueue.
      drain();
    }
  }

  /** Jobs started and not yet terminal. */
  int running() {
    synchronized (lock) {
      return running.size();
    }
  }

  /** Jobs waiting to start. */
  int queued() {
    synchronized (lock) {
      return queue.size();
    }
  }

  /** Starts queued jobs in order while running slots are free. */
  private void drain() {
    if (draining.get()) {
      // Called back while this thread starts a job; the loop below picks up the free slot.
      return;
    }
    draining.set(true);
    try {
      while (true) {
        Queued next;
        Cancellable deadline;
        synchronized (lock) {
          if (running.size() >= maxRunning || queue.isEmpty()) {
            return;
          }
          next = queue.pollFirst();
          deadline = waitDeadlines.remove(next.job());
          running.add(next.job());
        }
        cancel(deadline);
        next.startAsSubmitter();
      }
    } finally {
      draining.set(false);
    }
  }

  private void onTerminal(QueryJob job) {
    boolean freedSlot;
    Cancellable deadline;
    synchronized (lock) {
      freedSlot = running.remove(job);
      queue.removeIf(queued -> queued.job() == job);
      deadline = waitDeadlines.remove(job);
    }
    cancel(deadline);
    if (freedSlot) {
      drain();
    }
  }

  /** Arms the deadline of a job that was just queued. Must hold {@code lock}. */
  private void armWaitDeadline(QueryJob job) {
    TimeValue maxQueuedTime = settings.getSettingValue(Settings.Key.PPL_ASYNC_MAX_QUEUED_TIME);
    waitDeadlines.put(
        job,
        threadPool.schedule(() -> onWaitDeadline(job), maxQueuedTime, ThreadPool.Names.GENERIC));
  }

  private void onWaitDeadline(QueryJob job) {
    synchronized (lock) {
      waitDeadlines.remove(job);
      if (!queue.removeIf(queued -> queued.job() == job)) {
        // Started meanwhile.
        return;
      }
    }
    TimeValue maxQueuedTime = settings.getSettingValue(Settings.Key.PPL_ASYNC_MAX_QUEUED_TIME);
    job.failQueued(
        new OpenSearchStatusException(
            String.format(
                Locale.ROOT,
                "Async query waited longer than [%s] for capacity [%s]; retry later",
                maxQueuedTime,
                Settings.Key.PPL_ASYNC_MAX_QUEUED_TIME.getKeyValue()),
            RestStatus.TOO_MANY_REQUESTS));
  }

  private static void cancel(Cancellable timer) {
    if (timer != null) {
      timer.cancel();
    }
  }

  /** A queued job and the context of the thread that submitted it. */
  private record Queued(
      QueryJob job,
      Supplier<ThreadContext.StoredContext> threadContext,
      Map<String, String> logContext,
      CancellableTask task) {

    /** Captures the current thread's context; call on the submitting thread. */
    static Queued capture(QueryJob job, ThreadContext threadContext) {
      return new Queued(
          job,
          threadContext.newRestorableContext(false),
          org.apache.logging.log4j.ThreadContext.getImmutableContext(),
          OpenSearchQueryManager.getCancellableTask());
    }

    /** Starts the job in the submitter's context, then puts back the current thread's. */
    void startAsSubmitter() {
      Map<String, String> callerLogContext =
          org.apache.logging.log4j.ThreadContext.getImmutableContext();
      CancellableTask callerTask = OpenSearchQueryManager.getCancellableTask();
      try (ThreadContext.StoredContext ignored = threadContext.get()) {
        restore(logContext, task);
        job.startRunner();
      } finally {
        restore(callerLogContext, callerTask);
      }
    }

    private static void restore(Map<String, String> logContext, CancellableTask task) {
      org.apache.logging.log4j.ThreadContext.clearMap();
      org.apache.logging.log4j.ThreadContext.putAll(logContext);
      if (task == null) {
        OpenSearchQueryManager.clearCancellableTask();
      } else {
        OpenSearchQueryManager.setCancellableTask(task);
      }
    }
  }
}
