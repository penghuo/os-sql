/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.metrics;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;

/**
 * Atomic counters tracking distributed engine execution statistics. Exposed via the {@code
 * _plugins/_sql/stats} endpoint.
 *
 * <p>Thread-safe: all counters use {@link LongAdder} for lock-free concurrent increments.
 */
public class DistributedEngineMetrics {

  private static final DistributedEngineMetrics INSTANCE = new DistributedEngineMetrics();

  /** Total number of queries that entered the distributed engine path. */
  private final LongAdder queriesTotal = new LongAdder();

  /** Number of queries successfully executed through the distributed engine. */
  private final LongAdder queriesDistributed = new LongAdder();

  /** Number of expected fallbacks (unsupported patterns like joins, windows). */
  private final LongAdder fallbackExpected = new LongAdder();

  /** Number of unexpected fallbacks (distributed execution failed with an error). */
  private final LongAdder fallbackError = new LongAdder();

  private DistributedEngineMetrics() {}

  public static DistributedEngineMetrics getInstance() {
    return INSTANCE;
  }

  public void incrementQueriesTotal() {
    queriesTotal.increment();
  }

  public void incrementQueriesDistributed() {
    queriesDistributed.increment();
  }

  public void incrementFallbackExpected() {
    fallbackExpected.increment();
  }

  public void incrementFallbackError() {
    fallbackError.increment();
  }

  public long getQueriesTotal() {
    return queriesTotal.sum();
  }

  public long getQueriesDistributed() {
    return queriesDistributed.sum();
  }

  public long getFallbackExpected() {
    return fallbackExpected.sum();
  }

  public long getFallbackError() {
    return fallbackError.sum();
  }

  /** Collect all metrics as a map (for JSON serialization via stats endpoint). */
  public Map<String, Long> collectMetrics() {
    Map<String, Long> metrics = new LinkedHashMap<>();
    metrics.put("distributed_engine.queries_total", queriesTotal.sum());
    metrics.put("distributed_engine.queries_distributed", queriesDistributed.sum());
    metrics.put("distributed_engine.fallback_expected", fallbackExpected.sum());
    metrics.put("distributed_engine.fallback_error", fallbackError.sum());
    return metrics;
  }

  /** Reset all counters (for testing). */
  public void reset() {
    queriesTotal.reset();
    queriesDistributed.reset();
    fallbackExpected.reset();
    fallbackError.reset();
  }
}
