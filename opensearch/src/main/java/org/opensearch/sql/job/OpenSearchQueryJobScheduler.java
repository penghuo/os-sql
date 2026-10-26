/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.job;

import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.IdentityHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.ExceptionsHelper;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.opensearch.executor.WorkerPoolSaturatedException;
import org.opensearch.threadpool.Scheduler.Cancellable;
import org.opensearch.threadpool.ThreadPool;

/**
 * Runs at most {@code plugins.ppl.async.node_concurrent_running_queries} async jobs at once on this
 * node and queues up to {@code plugins.ppl.async.max_queued_queries} more in submission order; a
 * submit beyond both is rejected with {@code 429}, like a thread pool executor with a bounded work
 * queue.
 *
 * <p>A queued job stays {@code PENDING} until a running job finishes. It fails if it waits longer
 * than {@code plugins.ppl.async.max_queued_time}. A running job whose worker pool is saturated goes
 * back to the head of the queue. No queued job starts until {@link #RETRY_DELAY} has passed, so a
 * saturated pool is not hit with a retry for every job that finishes. Jobs start one at a time, and
 * starting stops at the first refusal.
 *
 * <h2>Thread safety</h2>
 *
 * Queue and running set are guarded by {@code lock}. Jobs are started and failed after the lock is
 * released; a job never calls back into the scheduler while holding its own monitor, so the lock
 * order is always scheduler, then job.
 */
public final class OpenSearchQueryJobScheduler implements QueryJobScheduler {

  /** Delay before a job refused by a saturated worker pool is tried again. */
  static final TimeValue RETRY_DELAY = TimeValue.timeValueSeconds(1);

  private static final Logger LOG = LogManager.getLogger(OpenSearchQueryJobScheduler.class);

  private final Settings settings;
  private final ThreadPool threadPool;
  private final Object lock = new Object();
  private final Set<QueryJob> running = Collections.newSetFromMap(new IdentityHashMap<>());
  private final Deque<QueryJob> queue = new ArrayDeque<>();
  private final Map<QueryJob, Cancellable> queueTimeouts = new IdentityHashMap<>();
  // A worker pool refused a job; nothing starts until the retry timer fires.
  private boolean paused;
  // Marks a thread that is draining, so a drain triggered while starting a job does not nest.
  private final ThreadLocal<Boolean> draining = ThreadLocal.withInitial(() -> false);

  /**
   * @param settings plugin settings the limits are read from on every decision
   * @param threadPool runs the queue timeout and retry timers on the generic pool
   * @throws NullPointerException if either argument is {@code null}
   */
  public OpenSearchQueryJobScheduler(Settings settings, ThreadPool threadPool) {
    this.settings = Objects.requireNonNull(settings, "settings must not be null");
    this.threadPool = Objects.requireNonNull(threadPool, "threadPool must not be null");
  }

  /**
   * {@inheritDoc}
   *
   * @throws OpenSearchStatusException with status {@code 429} when no running slot is free and the
   *     queue already holds {@code plugins.ppl.async.max_queued_queries} jobs
   */
  @Override
  public void submit(QueryJob job) {
    Objects.requireNonNull(job, "job must not be null");
    int maxRunning =
        settings.getSettingValue(Settings.Key.PPL_ASYNC_NODE_CONCURRENT_RUNNING_QUERIES);
    int maxQueued = settings.getSettingValue(Settings.Key.PPL_ASYNC_MAX_QUEUED_QUERIES);
    boolean startNow;
    synchronized (lock) {
      startNow = !paused && queue.isEmpty() && running.size() < maxRunning;
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
        addToQueue(job, false);
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

  @Override
  public boolean isCapacityRejection(Throwable failure) {
    return ExceptionsHelper.unwrap(failure, WorkerPoolSaturatedException.class) != null;
  }

  @Override
  public void requeue(QueryJob job) {
    boolean scheduleRetry;
    synchronized (lock) {
      running.remove(job);
      scheduleRetry = !paused;
      paused = true;
    }
    enqueue(job, true);
    if (scheduleRetry) {
      try {
        threadPool.schedule(this::resume, RETRY_DELAY, ThreadPool.Names.GENERIC);
      } catch (RuntimeException e) {
        LOG.warn("Failed to schedule a retry of a queued async query", e);
        resume();
      }
    }
  }

  private void resume() {
    synchronized (lock) {
      paused = false;
    }
    drain();
  }

  /** Jobs started and not yet terminal. */
  public int running() {
    synchronized (lock) {
      return running.size();
    }
  }

  /** Jobs waiting to start. */
  public int queued() {
    synchronized (lock) {
      return queue.size();
    }
  }

  /** Puts a requeued job back at the head of the queue; the queue limit does not apply. */
  private void enqueue(QueryJob job, boolean atHead) {
    synchronized (lock) {
      if (!job.isPending()) {
        // Cancelled before it could be queued; its terminal hook has already run.
        return;
      }
      addToQueue(job, atHead);
    }
  }

  /** Must hold {@code lock}. */
  private void addToQueue(QueryJob job, boolean atHead) {
    if (atHead) {
      queue.addFirst(job);
    } else {
      queue.addLast(job);
    }
    queueTimeouts.put(job, scheduleQueueTimeout(job));
  }

  /**
   * Starts queued jobs one at a time while running slots are free and no pool refusal is pending.
   */
  private void drain() {
    if (draining.get()) {
      // Called back while this thread starts a job; the loop below picks up the free slot.
      return;
    }
    draining.set(true);
    try {
      while (true) {
        int maxRunning =
            settings.getSettingValue(Settings.Key.PPL_ASYNC_NODE_CONCURRENT_RUNNING_QUERIES);
        QueryJob next;
        Cancellable timeout;
        boolean start;
        synchronized (lock) {
          if (paused || running.size() >= maxRunning || queue.isEmpty()) {
            return;
          }
          next = queue.pollFirst();
          timeout = queueTimeouts.remove(next);
          start = next.isPending();
          if (start) {
            running.add(next);
          }
        }
        cancel(timeout);
        if (start) {
          // May requeue the job, and pause starting, when the worker pool refuses it.
          next.startRunner();
        }
      }
    } finally {
      draining.set(false);
    }
  }

  private void onTerminal(QueryJob job) {
    boolean freedSlot;
    Cancellable timeout = null;
    synchronized (lock) {
      freedSlot = running.remove(job);
      if (!freedSlot && queue.remove(job)) {
        timeout = queueTimeouts.remove(job);
      }
    }
    cancel(timeout);
    if (freedSlot) {
      drain();
    }
  }

  private Cancellable scheduleQueueTimeout(QueryJob job) {
    TimeValue maxQueuedTime = settings.getSettingValue(Settings.Key.PPL_ASYNC_MAX_QUEUED_TIME);
    try {
      return threadPool.schedule(
          () -> onQueueTimeout(job, maxQueuedTime), maxQueuedTime, ThreadPool.Names.GENERIC);
    } catch (RuntimeException e) {
      LOG.warn("Failed to schedule the queue timeout of an async query", e);
      return null;
    }
  }

  private void onQueueTimeout(QueryJob job, TimeValue maxQueuedTime) {
    synchronized (lock) {
      if (!queue.remove(job)) {
        return;
      }
      queueTimeouts.remove(job);
    }
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
}
