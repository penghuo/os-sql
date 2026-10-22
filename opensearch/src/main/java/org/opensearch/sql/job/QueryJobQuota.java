/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.job;

import java.util.Locale;
import java.util.Objects;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.sql.common.setting.Settings;

/**
 * Node-local capacity for async query jobs.
 *
 * <p>Tracks three quantities against dynamic settings:
 *
 * <ul>
 *   <li>jobs that are not yet terminal, against {@code
 *       plugins.ppl.async.node_concurrent_running_queries};
 *   <li>jobs held in the store, against {@code plugins.ppl.async.max_retained_jobs};
 *   <li>estimated bytes of retained results, against {@code
 *       plugins.ppl.async.max_retained_result_size}.
 * </ul>
 *
 * <p>Admission checks and increments under one lock, so concurrent submits cannot overshoot a
 * limit. Settings are read on every check, so a lowered limit applies to the next submit; jobs
 * already admitted are not evicted. Rejections are {@link OpenSearchStatusException}s with status
 * {@code 429} and a message that names the exhausted setting and nothing else.
 */
public final class QueryJobQuota implements ResultBudget {

  private final Settings settings;
  private final Object lock = new Object();

  private int running;
  private int jobs;
  private long resultBytes;

  /**
   * @param settings plugin settings the limits are read from
   * @throws NullPointerException if {@code settings} is {@code null}
   */
  public QueryJobQuota(Settings settings) {
    this.settings = Objects.requireNonNull(settings, "settings must not be null");
  }

  /**
   * Admits one new job, counting it as running and stored.
   *
   * @throws OpenSearchStatusException with status {@code 429} when any limit is exhausted
   */
  void admit() {
    int maxRunning =
        settings.getSettingValue(Settings.Key.PPL_ASYNC_NODE_CONCURRENT_RUNNING_QUERIES);
    int maxJobs = settings.getSettingValue(Settings.Key.PPL_ASYNC_MAX_RETAINED_JOBS);
    long maxBytes = maxResultBytes();
    Settings.Key exhausted;
    synchronized (lock) {
      if (running >= maxRunning) {
        exhausted = Settings.Key.PPL_ASYNC_NODE_CONCURRENT_RUNNING_QUERIES;
      } else if (jobs >= maxJobs) {
        exhausted = Settings.Key.PPL_ASYNC_MAX_RETAINED_JOBS;
      } else if (resultBytes >= maxBytes) {
        exhausted = Settings.Key.PPL_ASYNC_MAX_RETAINED_RESULT_SIZE;
      } else {
        running++;
        jobs++;
        return;
      }
    }
    throw new OpenSearchStatusException(
        String.format(
            Locale.ROOT,
            "Async query capacity on this node is exhausted [%s]; retry later",
            exhausted.getKeyValue()),
        RestStatus.TOO_MANY_REQUESTS);
  }

  /** Reverts an {@link #admit()} whose job was never published. */
  void rollback() {
    synchronized (lock) {
      running--;
      jobs--;
    }
  }

  /** Releases the running slot of a job that reached a terminal state. Called once per job. */
  void releaseRunning() {
    synchronized (lock) {
      running--;
    }
  }

  /** Releases the stored slot of a job removed from the store. Called once per job. */
  void releaseJob() {
    synchronized (lock) {
      jobs--;
    }
  }

  /**
   * {@inheritDoc}
   *
   * @throws OpenSearchStatusException with status {@code 429} when {@code bytes} does not fit in
   *     the remaining budget
   */
  @Override
  public void reserve(long bytes) {
    long maxBytes = maxResultBytes();
    synchronized (lock) {
      if (bytes <= maxBytes - resultBytes) {
        resultBytes += bytes;
        return;
      }
    }
    throw new OpenSearchStatusException(
        String.format(
            Locale.ROOT,
            "Async query result does not fit in the retained result budget [%s]; reduce the"
                + " result size, or delete completed queries and retry",
            Settings.Key.PPL_ASYNC_MAX_RETAINED_RESULT_SIZE.getKeyValue()),
        RestStatus.TOO_MANY_REQUESTS);
  }

  @Override
  public void release(long bytes) {
    synchronized (lock) {
      resultBytes -= bytes;
    }
  }

  /** Jobs admitted and not yet terminal. */
  public int running() {
    synchronized (lock) {
      return running;
    }
  }

  /** Jobs admitted and not yet removed from the store. */
  public int jobs() {
    synchronized (lock) {
      return jobs;
    }
  }

  /** Estimated bytes of retained results currently charged. */
  public long resultBytes() {
    synchronized (lock) {
      return resultBytes;
    }
  }

  private long maxResultBytes() {
    ByteSizeValue max = settings.getSettingValue(Settings.Key.PPL_ASYNC_MAX_RETAINED_RESULT_SIZE);
    return max.getBytes();
  }
}
