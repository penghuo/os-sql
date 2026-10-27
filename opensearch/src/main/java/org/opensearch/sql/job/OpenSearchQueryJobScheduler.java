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
 * <p>A job whose worker pool is saturated gives up its running slot and backs off on its own:
 * {@link #INITIAL_BACKOFF}, doubling up to {@link #MAX_BACKOFF}. It then returns to the head of the
 * queue. Other jobs keep starting meanwhile, as in OpenSearch's retries of rejected executions.
 *
 * <p>A job may wait, queued or backing off, for at most {@code plugins.ppl.async.max_queued_time}
 * in total, counted from when it first had to wait. After that it ends {@code FAILED}.
 *
 * <h2>Thread safety</h2>
 *
 * All state is guarded by {@code lock}. Jobs are started and failed after the lock is released; a
 * job never calls back into the scheduler while holding its own monitor, so the lock order is
 * always scheduler, then job.
 */
public final class OpenSearchQueryJobScheduler implements QueryJobScheduler {

  /** First wait after a worker pool refuses a job. */
  static final TimeValue INITIAL_BACKOFF = TimeValue.timeValueMillis(500);

  /** Longest wait between two attempts of a refused job. */
  static final TimeValue MAX_BACKOFF = TimeValue.timeValueSeconds(8);

  private static final Logger LOG = LogManager.getLogger(OpenSearchQueryJobScheduler.class);

  /** Marks a wait deadline that has fired. */
  private static final Cancellable EXPIRED =
      new Cancellable() {
        @Override
        public boolean cancel() {
          return false;
        }

        @Override
        public boolean isCancelled() {
          return false;
        }
      };

  private final Settings settings;
  private final ThreadPool threadPool;
  private final Object lock = new Object();
  private final Set<QueryJob> running = Collections.newSetFromMap(new IdentityHashMap<>());
  private final Deque<QueryJob> queue = new ArrayDeque<>();
  // Refused jobs waiting out their backoff, with the timer that puts them back in the queue.
  private final Map<QueryJob, Cancellable> backingOff = new IdentityHashMap<>();
  // Refusals so far, per job; sets the next backoff.
  private final Map<QueryJob, Integer> refusals = new IdentityHashMap<>();
  // Fails a job still waiting when its total wait runs out; armed when the job first waits.
  private final Map<QueryJob, Cancellable> waitDeadlines = new IdentityHashMap<>();
  // Marks a thread that is draining, so a drain triggered while starting a job does not nest.
  private final ThreadLocal<Boolean> draining = ThreadLocal.withInitial(() -> false);

  /**
   * @param settings plugin settings the limits are read from on every decision
   * @param threadPool runs the backoff and wait deadline timers on the generic pool
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
        queue.addLast(job);
        armWaitDeadline(job);
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

  /** A full pool is retried; a pool that refuses because the node is shutting down is not. */
  @Override
  public boolean isCapacityRejection(Throwable failure) {
    Throwable saturated = ExceptionsHelper.unwrap(failure, WorkerPoolSaturatedException.class);
    return saturated != null && !((WorkerPoolSaturatedException) saturated).isExecutorShutdown();
  }

  /** Frees the refused job's slot and puts it back in the queue after its backoff. */
  @Override
  public void requeue(QueryJob job) {
    boolean waitExpired = false;
    boolean retryUnscheduled = false;
    synchronized (lock) {
      running.remove(job);
      // Not pending when cancelled while being refused; its terminal hook cleans up.
      if (job.isPending()) {
        waitExpired = !armWaitDeadline(job);
        if (!waitExpired) {
          int attempt = refusals.merge(job, 1, Integer::sum);
          // The timer takes the lock, so it cannot run before the job is recorded below.
          Cancellable timer = scheduleBackoff(job, backoff(attempt));
          retryUnscheduled = timer == null;
          if (!retryUnscheduled) {
            backingOff.put(job, timer);
          }
        }
      }
    }
    if (waitExpired) {
      failWaitedTooLong(job);
    } else if (retryUnscheduled) {
      // The generic pool refused the timer, as it does while the node shuts down.
      job.failQueued(
          new OpenSearchStatusException(
              "Async query could not be retried on this node; retry later",
              RestStatus.TOO_MANY_REQUESTS));
    }
    // The freed slot can start another job while this one backs off.
    drain();
  }

  /** Jobs started and not yet terminal. */
  public int running() {
    synchronized (lock) {
      return running.size();
    }
  }

  /** Jobs waiting to start, queued or backing off. */
  public int queued() {
    synchronized (lock) {
      return queue.size() + backingOff.size();
    }
  }

  /**
   * The wait after a job's {@code refusals}-th refusal: {@link #INITIAL_BACKOFF} doubled, capped.
   */
  static TimeValue backoff(int refusals) {
    long millis = INITIAL_BACKOFF.millis() << Math.min(refusals - 1, 16);
    return TimeValue.timeValueMillis(Math.min(millis, MAX_BACKOFF.millis()));
  }

  private Cancellable scheduleBackoff(QueryJob job, TimeValue delay) {
    try {
      return threadPool.schedule(() -> onBackoffElapsed(job), delay, ThreadPool.Names.GENERIC);
    } catch (RuntimeException e) {
      LOG.warn("Failed to schedule the retry of a refused async query", e);
      return null;
    }
  }

  private void onBackoffElapsed(QueryJob job) {
    synchronized (lock) {
      if (!backingOff.containsKey(job)) {
        return;
      }
      backingOff.remove(job);
      queue.addFirst(job);
    }
    drain();
  }

  /** Starts queued jobs one at a time while running slots are free. */
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
        synchronized (lock) {
          if (running.size() >= maxRunning || queue.isEmpty()) {
            return;
          }
          next = queue.pollFirst();
          if (!next.isPending()) {
            continue;
          }
          running.add(next);
        }
        // May requeue the job when the worker pool refuses it.
        next.startRunner();
      }
    } finally {
      draining.set(false);
    }
  }

  private void onTerminal(QueryJob job) {
    boolean freedSlot;
    Cancellable backoff;
    Cancellable deadline;
    synchronized (lock) {
      freedSlot = running.remove(job);
      queue.remove(job);
      backoff = backingOff.remove(job);
      deadline = waitDeadlines.remove(job);
      refusals.remove(job);
    }
    cancel(backoff);
    cancel(deadline);
    if (freedSlot) {
      drain();
    }
  }

  /**
   * Arms the job's wait deadline the first time it has to wait. Must hold {@code lock}.
   *
   * @return {@code false} if the deadline has already fired, so the job may not wait again
   */
  private boolean armWaitDeadline(QueryJob job) {
    if (waitDeadlines.containsKey(job)) {
      return waitDeadlines.get(job) != EXPIRED;
    }
    TimeValue maxQueuedTime = settings.getSettingValue(Settings.Key.PPL_ASYNC_MAX_QUEUED_TIME);
    Cancellable deadline;
    try {
      deadline =
          threadPool.schedule(() -> onWaitDeadline(job), maxQueuedTime, ThreadPool.Names.GENERIC);
    } catch (RuntimeException e) {
      LOG.warn("Failed to schedule the wait deadline of an async query", e);
      deadline = null;
    }
    waitDeadlines.put(job, deadline);
    return true;
  }

  private void onWaitDeadline(QueryJob job) {
    Cancellable backoff;
    synchronized (lock) {
      if (!waitDeadlines.containsKey(job)) {
        return;
      }
      waitDeadlines.put(job, EXPIRED);
      boolean wasBackingOff = backingOff.containsKey(job);
      backoff = backingOff.remove(job);
      if (!queue.remove(job) && !wasBackingOff) {
        // Running now; if it is refused again, requeue fails it instead of waiting.
        return;
      }
    }
    cancel(backoff);
    failWaitedTooLong(job);
  }

  private void failWaitedTooLong(QueryJob job) {
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
    if (timer != null && timer != EXPIRED) {
      timer.cancel();
    }
  }
}
