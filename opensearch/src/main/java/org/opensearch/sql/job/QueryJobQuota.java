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
 * Node-local capacity for the results of finished async query jobs.
 *
 * <p>Tracks two quantities against dynamic settings:
 *
 * <ul>
 *   <li>finished jobs retained for polling, against {@code plugins.ppl.async.max_retained_jobs};
 *   <li>estimated bytes of retained results, against {@code
 *       plugins.ppl.async.max_retained_result_size}.
 * </ul>
 *
 * <p>A submit is rejected while either is full. Jobs that run or wait are bounded by the {@link
 * OpenSearchQueryJobScheduler} instead. Jobs admitted while the limit had room may still finish, so
 * the retained count can exceed its limit by at most the running and queued jobs. Settings are read
 * on every check, so a lowered limit applies to the next submit; retained jobs are not evicted.
 * Rejections are {@link OpenSearchStatusException}s with status {@code 429} and a message that
 * names the exhausted setting and nothing else.
 */
public final class QueryJobQuota implements ResultBudget {

  private final Settings settings;
  private final Object lock = new Object();

  private int finished;
  private long resultBytes;

  /**
   * @param settings plugin settings the limits are read from
   * @throws NullPointerException if {@code settings} is {@code null}
   */
  public QueryJobQuota(Settings settings) {
    this.settings = Objects.requireNonNull(settings, "settings must not be null");
  }

  /**
   * Checks that a new job can be admitted.
   *
   * @throws OpenSearchStatusException with status {@code 429} when either limit is exhausted
   */
  void admit() {
    int maxFinished = settings.getSettingValue(Settings.Key.PPL_ASYNC_MAX_RETAINED_JOBS);
    long maxBytes = maxResultBytes();
    Settings.Key exhausted;
    synchronized (lock) {
      if (finished >= maxFinished) {
        exhausted = Settings.Key.PPL_ASYNC_MAX_RETAINED_JOBS;
      } else if (resultBytes >= maxBytes) {
        exhausted = Settings.Key.PPL_ASYNC_MAX_RETAINED_RESULT_SIZE;
      } else {
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

  /** Counts a finished job retained for polling. */
  void addFinished() {
    synchronized (lock) {
      finished++;
    }
  }

  /** Releases a finished job removed from the store. */
  void releaseFinished() {
    synchronized (lock) {
      finished--;
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

  /** Finished jobs retained for polling. */
  public int finished() {
    synchronized (lock) {
      return finished;
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
