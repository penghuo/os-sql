/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.execution.task;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;

/**
 * Schedules timer-based query timeouts. After the configured timeout, triggers cancellation via the
 * {@link QueryCancellationHandler}.
 */
public class QueryTimeoutScheduler {

  private final ThreadPool threadPool;
  private final QueryCancellationHandler cancellationHandler;
  private final Map<String, Scheduler.Cancellable> scheduledTimeouts = new ConcurrentHashMap<>();

  public QueryTimeoutScheduler(
      ThreadPool threadPool, QueryCancellationHandler cancellationHandler) {
    this.threadPool = Objects.requireNonNull(threadPool, "threadPool must not be null");
    this.cancellationHandler =
        Objects.requireNonNull(cancellationHandler, "cancellationHandler must not be null");
  }

  /**
   * Schedules a timeout for a query.
   *
   * @param queryId the query identifier
   * @param timeout timeout duration
   * @return a Cancellable that can be used to cancel the timer on query completion
   */
  public Scheduler.Cancellable scheduleTimeout(String queryId, TimeValue timeout) {
    Scheduler.Cancellable cancellable =
        threadPool.schedule(
            () -> {
              cancellationHandler.cancelQuery(queryId, "Query timed out after " + timeout);
              scheduledTimeouts.remove(queryId);
            },
            timeout,
            threadPool.generic().toString().contains("generic")
                ? ThreadPool.Names.GENERIC
                : ThreadPool.Names.GENERIC);
    scheduledTimeouts.put(queryId, cancellable);
    return cancellable;
  }

  /** Cancels a pending timeout (called when query completes before timeout). */
  public void cancelTimeout(String queryId) {
    Scheduler.Cancellable cancellable = scheduledTimeouts.remove(queryId);
    if (cancellable != null) {
      cancellable.cancel();
    }
  }
}
