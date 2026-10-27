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
import org.opensearch.OpenSearchStatusException;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.threadpool.Scheduler.Cancellable;
import org.opensearch.threadpool.ThreadPool;

/**
 * Runs at most {@code plugins.ppl.async.node_concurrent_running_queries} async jobs at once on this
 * node and queues up to {@code plugins.ppl.async.max_queued_queries} more in submission order; a
 * submit beyond both is rejected with {@code 429}, like a thread pool executor with a bounded work
 * queue.
 *
 * <p>A queued job may wait for at most {@code plugins.ppl.async.max_queued_time}. After that it
 * ends {@code FAILED}.
 *
 * <p>A running slot only bounds async jobs; it does not reserve room in the SQL worker pools, which
 * other queries share. A job whose worker pool refuses it fails.
 *
 * <h2>Thread safety</h2>
 *
 * All state is guarded by {@code lock}. Jobs are started and failed after the lock is released, so
 * the lock order is always scheduler, then job.
 */
public final class OpenSearchQueryJobScheduler {

  private static final Logger LOG = LogManager.getLogger(OpenSearchQueryJobScheduler.class);

  private final Settings settings;
  private final ThreadPool threadPool;
  private final Object lock = new Object();
  private final Set<QueryJob> running = Collections.newSetFromMap(new IdentityHashMap<>());
  private final Deque<QueryJob> queue = new ArrayDeque<>();
  // Fails a queued job that waited too long; armed when the job is queued.
  private final Map<QueryJob, Cancellable> waitDeadlines = new IdentityHashMap<>();
  // Marks a thread that is draining, so a drain triggered while starting a job does not nest.
  private final ThreadLocal<Boolean> draining = ThreadLocal.withInitial(() -> false);

  /**
   * @param settings plugin settings the limits are read from on every decision
   * @param threadPool runs the wait deadline timers on the generic pool
   * @throws NullPointerException if either argument is {@code null}
   */
  public OpenSearchQueryJobScheduler(Settings settings, ThreadPool threadPool) {
    this.settings = Objects.requireNonNull(settings, "settings must not be null");
    this.threadPool = Objects.requireNonNull(threadPool, "threadPool must not be null");
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

  /** Starts queued jobs in order while running slots are free. */
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
        Cancellable deadline;
        synchronized (lock) {
          if (running.size() >= maxRunning || queue.isEmpty()) {
            return;
          }
          next = queue.pollFirst();
          deadline = waitDeadlines.remove(next);
          running.add(next);
        }
        cancel(deadline);
        next.startRunner();
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
      queue.remove(job);
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
    try {
      waitDeadlines.put(
          job,
          threadPool.schedule(() -> onWaitDeadline(job), maxQueuedTime, ThreadPool.Names.GENERIC));
    } catch (RuntimeException e) {
      LOG.warn("Failed to schedule the wait deadline of an async query", e);
    }
  }

  private void onWaitDeadline(QueryJob job) {
    synchronized (lock) {
      waitDeadlines.remove(job);
      if (!queue.remove(job)) {
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
}
