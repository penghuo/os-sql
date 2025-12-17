/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.executor;

import java.util.concurrent.atomic.AtomicLong;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.ThreadContext;

/**
 * Lightweight per-query latency tracker, activated only when profiling is requested.
 *
 * <p>All operations are no-ops when profiling is disabled to minimize overhead.
 */
public class QueryLatencyTracker {

  public static final String PROFILE_FLAG = "ppl_profile_enabled";

  private static final Logger LOG = LogManager.getLogger(QueryLatencyTracker.class);

  private static final QueryLatencyTracker NO_OP = new QueryLatencyTracker(false);
  private static final ThreadLocal<QueryLatencyTracker> CURRENT = new ThreadLocal<>();

  private final boolean enabled;
  private final long startNanos;

  private final AtomicLong buildResultSetNanos = new AtomicLong(0);
  private final AtomicLong finalResultSetNanos = new AtomicLong(0);
  private final AtomicLong analyzeNanos = new AtomicLong(0);
  private final AtomicLong dslLatency = new AtomicLong(0);
  private final AtomicLong buildResultSetRows = new AtomicLong(0);

  private QueryLatencyTracker(boolean enabled) {
    this.enabled = enabled;
    this.startNanos = enabled ? System.nanoTime() : 0;
  }

  public static QueryLatencyTracker startIfEnabled() {
    boolean profileEnabled = Boolean.parseBoolean(ThreadContext.get(PROFILE_FLAG));
    if (!profileEnabled) {
      CURRENT.remove();
      return NO_OP;
    }
    LOG.info(
        "[{}] query startTime={}", ThreadContext.get("request_id"), System.currentTimeMillis());
    QueryLatencyTracker tracker = new QueryLatencyTracker(true);
    CURRENT.set(tracker);
    return tracker;
  }

  public static QueryLatencyTracker current() {
    QueryLatencyTracker tracker = CURRENT.get();
    return tracker == null ? NO_OP : tracker;
  }

  public boolean isEnabled() {
    return enabled;
  }

  public void recordBuildResultSet(long nanos, long rows) {
    if (!enabled) {
      return;
    }
    buildResultSetNanos.addAndGet(nanos);
    buildResultSetRows.addAndGet(rows);
  }

  public void recordFinalResultSet(long nanos) {
    if (!enabled) {
      return;
    }
    finalResultSetNanos.addAndGet(nanos);
  }

  public void recordAnalyze(long nanos) {
    if (!enabled) {
      return;
    }
    analyzeNanos.addAndGet(nanos);
  }

  public void recordDSL(long nanos) {
    if (!enabled) {
      return;
    }
    dslLatency.addAndGet(nanos);
  }

  public void finish() {
    if (!enabled) {
      return;
    }
    long totalMillis = nanosToMillis(System.nanoTime() - startNanos);
    LOG.info(
        "PPL Profile: requestId={}, totalMs={}, analyzeMs={}, planMs={}, dslMs={}, "
            + "buildResultSetMs={},finalResultSetMs={}",
        ThreadContext.get("request_id"),
        totalMillis,
        nanosToMillis(analyzeNanos.get()),
        totalMillis
            - (nanosToMillis(analyzeNanos.get())
                + nanosToMillis(dslLatency.get())
                + nanosToMillis(buildResultSetNanos.get())
                + nanosToMillis(finalResultSetNanos.get())),
        nanosToMillis(dslLatency.get()),
        nanosToMillis(buildResultSetNanos.get()),
        nanosToMillis(finalResultSetNanos.get()));
    CURRENT.remove();
  }

  private static long nanosToMillis(long nanos) {
    return nanos / 1_000_000;
  }
}
