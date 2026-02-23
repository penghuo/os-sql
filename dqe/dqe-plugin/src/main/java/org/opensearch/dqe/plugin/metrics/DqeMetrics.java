/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.plugin.metrics;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * Thread-safe DQE Phase 1 observability metrics. All counters use {@link LongAdder} for
 * high-throughput concurrent increments. Gauges (active queries, memory) use {@link AtomicLong}.
 *
 * <p>Metrics tracked:
 *
 * <ul>
 *   <li>Query lifecycle: submitted, succeeded, failed (by category), cancelled, active
 *   <li>Data volume: rows scanned, rows returned, bytes scanned
 *   <li>Timing: cumulative wall time
 *   <li>Memory: current usage, breaker limit, breaker trip count
 * </ul>
 */
public class DqeMetrics {

  // Query lifecycle counters
  private final LongAdder queriesSubmitted = new LongAdder();
  private final LongAdder queriesSucceeded = new LongAdder();
  private final LongAdder queriesFailed = new LongAdder();
  private final LongAdder queriesCancelled = new LongAdder();
  private final AtomicLong activeQueries = new AtomicLong(0);

  // Timing
  private final LongAdder totalWallTimeMs = new LongAdder();

  // Data volume
  private final LongAdder totalRowsReturned = new LongAdder();
  private final LongAdder totalRowsScanned = new LongAdder();
  private final LongAdder totalBytesScanned = new LongAdder();

  // Memory
  private final AtomicLong memoryUsedBytes = new AtomicLong(0);
  private final AtomicLong breakerLimitBytes = new AtomicLong(0);
  private final LongAdder breakerTrippedCount = new LongAdder();

  public DqeMetrics() {}

  /** Record that a query was submitted. */
  public void recordQuerySubmitted() {
    queriesSubmitted.increment();
  }

  /**
   * Record a successful query completion.
   *
   * @param wallTimeMs wall clock time of the query
   * @param rowsReturned number of rows in the result set
   */
  public void recordQuerySucceeded(long wallTimeMs, long rowsReturned) {
    queriesSucceeded.increment();
    totalWallTimeMs.add(wallTimeMs);
    totalRowsReturned.add(rowsReturned);
  }

  /**
   * Record a failed query.
   *
   * @param errorCategory the error category (e.g. error code name)
   */
  public void recordQueryFailed(String errorCategory) {
    queriesFailed.increment();
  }

  /** Record a cancelled query. */
  public void recordQueryCancelled() {
    queriesCancelled.increment();
  }

  /** Increment active query count. Call when a query starts executing. */
  public void incrementActiveQueries() {
    activeQueries.incrementAndGet();
  }

  /** Decrement active query count. Call when a query finishes (success, failure, or cancel). */
  public void decrementActiveQueries() {
    activeQueries.decrementAndGet();
  }

  /**
   * Record rows scanned during query execution.
   *
   * @param rows number of rows scanned
   */
  public void recordRowsScanned(long rows) {
    totalRowsScanned.add(rows);
  }

  /**
   * Record bytes scanned during query execution.
   *
   * @param bytes number of bytes scanned
   */
  public void recordBytesScanned(long bytes) {
    totalBytesScanned.add(bytes);
  }

  /**
   * Update the current DQE memory usage gauge.
   *
   * @param bytes current memory usage in bytes
   */
  public void updateMemoryUsed(long bytes) {
    memoryUsedBytes.set(bytes);
  }

  /**
   * Update the circuit breaker limit gauge.
   *
   * @param bytes breaker limit in bytes
   */
  public void updateBreakerLimit(long bytes) {
    breakerLimitBytes.set(bytes);
  }

  /** Record that the circuit breaker was tripped. */
  public void recordBreakerTripped() {
    breakerTrippedCount.increment();
  }

  /**
   * Get a consistent snapshot of all metrics.
   *
   * @return an immutable metrics snapshot
   */
  public MetricsSnapshot getSnapshot() {
    return new MetricsSnapshot(
        queriesSubmitted.sum(),
        queriesSucceeded.sum(),
        queriesFailed.sum(),
        queriesCancelled.sum(),
        activeQueries.get(),
        totalWallTimeMs.sum(),
        totalRowsReturned.sum(),
        totalRowsScanned.sum(),
        totalBytesScanned.sum(),
        memoryUsedBytes.get(),
        breakerLimitBytes.get(),
        breakerTrippedCount.sum());
  }

  /** Reset all metrics counters and gauges. */
  public void reset() {
    queriesSubmitted.reset();
    queriesSucceeded.reset();
    queriesFailed.reset();
    queriesCancelled.reset();
    activeQueries.set(0);
    totalWallTimeMs.reset();
    totalRowsReturned.reset();
    totalRowsScanned.reset();
    totalBytesScanned.reset();
    memoryUsedBytes.set(0);
    breakerLimitBytes.set(0);
    breakerTrippedCount.reset();
  }

  /** Immutable snapshot of DQE metrics at a point in time. */
  public static class MetricsSnapshot {
    private final long queriesSubmitted;
    private final long queriesSucceeded;
    private final long queriesFailed;
    private final long queriesCancelled;
    private final long activeQueries;
    private final long totalWallTimeMs;
    private final long totalRowsReturned;
    private final long totalRowsScanned;
    private final long totalBytesScanned;
    private final long memoryUsedBytes;
    private final long breakerLimitBytes;
    private final long breakerTrippedCount;

    MetricsSnapshot(
        long queriesSubmitted,
        long queriesSucceeded,
        long queriesFailed,
        long queriesCancelled,
        long activeQueries,
        long totalWallTimeMs,
        long totalRowsReturned,
        long totalRowsScanned,
        long totalBytesScanned,
        long memoryUsedBytes,
        long breakerLimitBytes,
        long breakerTrippedCount) {
      this.queriesSubmitted = queriesSubmitted;
      this.queriesSucceeded = queriesSucceeded;
      this.queriesFailed = queriesFailed;
      this.queriesCancelled = queriesCancelled;
      this.activeQueries = activeQueries;
      this.totalWallTimeMs = totalWallTimeMs;
      this.totalRowsReturned = totalRowsReturned;
      this.totalRowsScanned = totalRowsScanned;
      this.totalBytesScanned = totalBytesScanned;
      this.memoryUsedBytes = memoryUsedBytes;
      this.breakerLimitBytes = breakerLimitBytes;
      this.breakerTrippedCount = breakerTrippedCount;
    }

    public long getQueriesSubmitted() { return queriesSubmitted; }
    public long getQueriesSucceeded() { return queriesSucceeded; }
    public long getQueriesFailed() { return queriesFailed; }
    public long getQueriesCancelled() { return queriesCancelled; }
    public long getActiveQueries() { return activeQueries; }
    public long getTotalWallTimeMs() { return totalWallTimeMs; }
    public long getTotalRowsReturned() { return totalRowsReturned; }
    public long getTotalRowsScanned() { return totalRowsScanned; }
    public long getTotalBytesScanned() { return totalBytesScanned; }
    public long getMemoryUsedBytes() { return memoryUsedBytes; }
    public long getBreakerLimitBytes() { return breakerLimitBytes; }
    public long getBreakerTrippedCount() { return breakerTrippedCount; }

    /**
     * Format this snapshot as a JSON string for the stats endpoint.
     *
     * @return JSON string
     */
    public String toJson() {
      return "{"
          + "\"queries\":{\"submitted\":" + queriesSubmitted
          + ",\"succeeded\":" + queriesSucceeded
          + ",\"failed\":" + queriesFailed
          + ",\"cancelled\":" + queriesCancelled
          + ",\"active\":" + activeQueries
          + ",\"wall_time_ms\":" + totalWallTimeMs
          + ",\"rows_returned\":" + totalRowsReturned
          + ",\"rows_scanned\":" + totalRowsScanned
          + ",\"bytes_scanned\":" + totalBytesScanned
          + "},"
          + "\"memory\":{\"used_bytes\":" + memoryUsedBytes
          + ",\"breaker_limit_bytes\":" + breakerLimitBytes
          + ",\"breaker_tripped\":" + breakerTrippedCount
          + "}}";
    }
  }
}
