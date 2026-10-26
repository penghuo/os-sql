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
 * Node-local capacity for holding async query jobs.
 *
 * <p>Tracks two quantities against dynamic settings:
 *
 * <ul>
 *   <li>jobs held in the store, whether queued, running, or retained for polling, against {@code
 *       plugins.ppl.async.max_retained_jobs};
 *   <li>estimated bytes of retained results, against {@code
 *       plugins.ppl.async.max_retained_result_size}.
 * </ul>
 *
 * <p>How many jobs run at once is the {@link OpenSearchQueryJobScheduler}'s concern; a job over
 * that limit waits instead of being rejected. Admission checks and increments under one lock, so
 * concurrent submits cannot overshoot a limit. Settings are read on every check, so a lowered limit
 * applies to the next submit; jobs already admitted are not evicted. Rejections are {@link
 * OpenSearchStatusException}s with status {@code 429} and a message that names the exhausted
 * setting and nothing else.
 */
public final class QueryJobQuota implements ResultBudget {

  private final Settings settings;
  private final Object lock = new Object();

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
   * Admits one new job into the store.
   *
   * @throws OpenSearchStatusException with status {@code 429} when either limit is exhausted
   */
  void admit() {
    int maxJobs = settings.getSettingValue(Settings.Key.PPL_ASYNC_MAX_RETAINED_JOBS);
    long maxBytes = maxResultBytes();
    Settings.Key exhausted;
    synchronized (lock) {
      if (jobs >= maxJobs) {
        exhausted = Settings.Key.PPL_ASYNC_MAX_RETAINED_JOBS;
      } else if (resultBytes >= maxBytes) {
        exhausted = Settings.Key.PPL_ASYNC_MAX_RETAINED_RESULT_SIZE;
      } else {
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

  /** Releases the slot of a job removed from the store, or of an admission that was not used. */
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
