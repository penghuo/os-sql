/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.plugin.logging;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.dqe.plugin.request.DqeQueryRequest;
import org.opensearch.dqe.plugin.response.DqeQueryResponse;
import org.opensearch.dqe.plugin.settings.DqeSettings;

/**
 * Logs queries that exceed the slow query threshold at WARN level. The threshold is configured via
 * {@code plugins.dqe.slow_query_log.threshold} (default 10s) and is read dynamically.
 *
 * <p>Logged information includes: query text (truncated to 1000 chars), execution time, rows
 * scanned/returned, shards involved, and memory peak usage.
 */
public class SlowQueryLogger {

  private static final Logger LOG = LogManager.getLogger(SlowQueryLogger.class);

  /** Maximum query text length in log output. */
  static final int MAX_LOG_QUERY_LENGTH = 1000;

  private final DqeSettings settings;

  /**
   * Creates a new slow query logger.
   *
   * @param settings the DQE settings for reading the slow query threshold dynamically
   */
  public SlowQueryLogger(DqeSettings settings) {
    this.settings = settings;
  }

  /**
   * Log a slow query if its elapsed time exceeds the configured threshold.
   *
   * @param request the original query request
   * @param stats the query execution stats
   * @param memoryPeakBytes peak memory usage in bytes during query execution
   */
  public void maybeLog(
      DqeQueryRequest request, DqeQueryResponse.QueryStats stats, long memoryPeakBytes) {
    if (!isSlowQuery(stats.getElapsedMs())) {
      return;
    }

    String truncatedQuery = truncateQuery(request.getQuery());

    LOG.warn(
        "Slow DQE query detected [query_id={}] [elapsed_ms={}] [rows_processed={}] "
            + "[bytes_processed={}] [shards={}] [stages={}] [memory_peak_bytes={}] [query={}]",
        stats.getQueryId(),
        stats.getElapsedMs(),
        stats.getRowsProcessed(),
        stats.getBytesProcessed(),
        stats.getShardsQueried(),
        stats.getStages(),
        memoryPeakBytes,
        truncatedQuery);
  }

  /**
   * Check if a query with the given elapsed time qualifies as slow.
   *
   * @param elapsedMs query wall time in milliseconds
   * @return true if the query exceeds the slow query threshold
   */
  public boolean isSlowQuery(long elapsedMs) {
    return elapsedMs >= settings.getSlowQueryLogThreshold().millis();
  }

  private static String truncateQuery(String query) {
    if (query == null) {
      return "<null>";
    }
    if (query.length() <= MAX_LOG_QUERY_LENGTH) {
      return query;
    }
    return query.substring(0, MAX_LOG_QUERY_LENGTH) + "...(truncated)";
  }
}
