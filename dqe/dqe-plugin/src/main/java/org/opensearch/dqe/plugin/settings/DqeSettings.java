/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.plugin.settings;

import java.util.Arrays;
import java.util.List;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;

/**
 * All DQE cluster settings from design doc Section 22 (Phase 1 scope). Settings are registered with
 * OpenSearch via {@link #getAllSettings()} and read dynamically via the instance getters.
 */
public class DqeSettings {

  // ---------------------------------------------------------------------------
  // Engine routing
  // ---------------------------------------------------------------------------

  /** Default SQL engine. "calcite" or "dqe". */
  public static final Setting<String> SQL_ENGINE_SETTING =
      Setting.simpleString(
          "plugins.sql.engine",
          "calcite",
          value -> {
            if (!"calcite".equals(value) && !"dqe".equals(value)) {
              throw new IllegalArgumentException(
                  "plugins.sql.engine must be 'calcite' or 'dqe', got: " + value);
            }
          },
          Setting.Property.NodeScope,
          Setting.Property.Dynamic);

  /** Whether the DQE engine is available. When false, engine=dqe requests are rejected. */
  public static final Setting<Boolean> DQE_ENABLED_SETTING =
      Setting.boolSetting(
          "plugins.dqe.enabled", true, Setting.Property.NodeScope, Setting.Property.Dynamic);

  // ---------------------------------------------------------------------------
  // Concurrency
  // ---------------------------------------------------------------------------

  /** Maximum concurrent DQE queries per node. */
  public static final Setting<Integer> MAX_CONCURRENT_QUERIES_SETTING =
      Setting.intSetting(
          "plugins.dqe.max_concurrent_queries",
          10,
          1,
          Setting.Property.NodeScope,
          Setting.Property.Dynamic);

  // ---------------------------------------------------------------------------
  // Timeout
  // ---------------------------------------------------------------------------

  /** Query execution timeout. */
  public static final Setting<TimeValue> QUERY_TIMEOUT_SETTING =
      Setting.timeSetting(
          "plugins.dqe.query_timeout",
          TimeValue.timeValueMinutes(5),
          TimeValue.timeValueSeconds(1),
          Setting.Property.NodeScope,
          Setting.Property.Dynamic);

  // ---------------------------------------------------------------------------
  // Memory
  // ---------------------------------------------------------------------------

  /** Max heap fraction for DQE circuit breaker (e.g. "20%"). */
  public static final Setting<String> MEMORY_BREAKER_LIMIT_SETTING =
      Setting.simpleString(
          "plugins.dqe.memory.breaker_limit",
          "20%",
          Setting.Property.NodeScope,
          Setting.Property.Dynamic);

  /** Default max memory per query. */
  public static final Setting<ByteSizeValue> QUERY_MAX_MEMORY_SETTING =
      Setting.byteSizeSetting(
          "plugins.dqe.memory.query_max_memory",
          new ByteSizeValue(256, ByteSizeUnit.MB),
          Setting.Property.NodeScope,
          Setting.Property.Dynamic);

  // ---------------------------------------------------------------------------
  // Exchange
  // ---------------------------------------------------------------------------

  /** Buffer size per exchange channel. */
  public static final Setting<ByteSizeValue> EXCHANGE_BUFFER_SIZE_SETTING =
      Setting.byteSizeSetting(
          "plugins.dqe.exchange.buffer_size",
          new ByteSizeValue(32, ByteSizeUnit.MB),
          Setting.Property.NodeScope,
          Setting.Property.Dynamic);

  /** Max size of a single exchange message. */
  public static final Setting<ByteSizeValue> EXCHANGE_CHUNK_SIZE_SETTING =
      Setting.byteSizeSetting(
          "plugins.dqe.exchange.chunk_size",
          new ByteSizeValue(1, ByteSizeUnit.MB),
          Setting.Property.NodeScope,
          Setting.Property.Dynamic);

  /** Transport timeout per exchange chunk. */
  public static final Setting<TimeValue> EXCHANGE_TIMEOUT_SETTING =
      Setting.timeSetting(
          "plugins.dqe.exchange.timeout",
          TimeValue.timeValueSeconds(60),
          TimeValue.timeValueSeconds(1),
          Setting.Property.NodeScope,
          Setting.Property.Dynamic);

  /** Max time producer blocks on full buffer. */
  public static final Setting<TimeValue> EXCHANGE_BACKPRESSURE_TIMEOUT_SETTING =
      Setting.timeSetting(
          "plugins.dqe.exchange.backpressure_timeout",
          TimeValue.timeValueSeconds(30),
          TimeValue.timeValueSeconds(1),
          Setting.Property.NodeScope,
          Setting.Property.Dynamic);

  // ---------------------------------------------------------------------------
  // Scan
  // ---------------------------------------------------------------------------

  /** Documents per search request in scan operator. */
  public static final Setting<Integer> SCAN_BATCH_SIZE_SETTING =
      Setting.intSetting(
          "plugins.dqe.scan.batch_size",
          1000,
          1,
          Setting.Property.NodeScope,
          Setting.Property.Dynamic);

  // ---------------------------------------------------------------------------
  // Slow query log
  // ---------------------------------------------------------------------------

  /** Slow query log threshold. Queries exceeding this are logged at WARN. */
  public static final Setting<TimeValue> SLOW_QUERY_LOG_THRESHOLD_SETTING =
      Setting.timeSetting(
          "plugins.dqe.slow_query_log.threshold",
          TimeValue.timeValueSeconds(10),
          TimeValue.timeValueMillis(0),
          Setting.Property.NodeScope,
          Setting.Property.Dynamic);

  // ---------------------------------------------------------------------------
  // Thread pools (node-level, non-dynamic)
  // ---------------------------------------------------------------------------

  /** Worker thread pool size. */
  public static final Setting<Integer> WORKER_POOL_SIZE_SETTING =
      Setting.intSetting(
          "plugins.dqe.worker.pool_size", defaultWorkerPoolSize(), 1, Setting.Property.NodeScope);

  /** Exchange thread pool size. */
  public static final Setting<Integer> EXCHANGE_POOL_SIZE_SETTING =
      Setting.intSetting(
          "plugins.dqe.exchange.pool_size",
          defaultExchangePoolSize(),
          1,
          Setting.Property.NodeScope);

  /** Coordinator thread pool size. */
  public static final Setting<Integer> COORDINATOR_POOL_SIZE_SETTING =
      Setting.intSetting(
          "plugins.dqe.coordinator.pool_size",
          defaultCoordinatorPoolSize(),
          1,
          Setting.Property.NodeScope);

  private final ClusterSettings clusterSettings;

  /**
   * Creates a new DqeSettings instance that reads current values from cluster settings.
   *
   * @param clusterSettings the OpenSearch cluster settings
   */
  public DqeSettings(ClusterSettings clusterSettings) {
    this.clusterSettings = clusterSettings;
  }

  // ---------------------------------------------------------------------------
  // Dynamic getters
  // ---------------------------------------------------------------------------

  public String getEngine() {
    return clusterSettings.get(SQL_ENGINE_SETTING);
  }

  public boolean isDqeEnabled() {
    return clusterSettings.get(DQE_ENABLED_SETTING);
  }

  public int getMaxConcurrentQueries() {
    return clusterSettings.get(MAX_CONCURRENT_QUERIES_SETTING);
  }

  public TimeValue getQueryTimeout() {
    return clusterSettings.get(QUERY_TIMEOUT_SETTING);
  }

  public ByteSizeValue getQueryMaxMemory() {
    return clusterSettings.get(QUERY_MAX_MEMORY_SETTING);
  }

  public ByteSizeValue getExchangeBufferSize() {
    return clusterSettings.get(EXCHANGE_BUFFER_SIZE_SETTING);
  }

  public ByteSizeValue getExchangeChunkSize() {
    return clusterSettings.get(EXCHANGE_CHUNK_SIZE_SETTING);
  }

  public TimeValue getExchangeTimeout() {
    return clusterSettings.get(EXCHANGE_TIMEOUT_SETTING);
  }

  public TimeValue getExchangeBackpressureTimeout() {
    return clusterSettings.get(EXCHANGE_BACKPRESSURE_TIMEOUT_SETTING);
  }

  public int getScanBatchSize() {
    return clusterSettings.get(SCAN_BATCH_SIZE_SETTING);
  }

  public TimeValue getSlowQueryLogThreshold() {
    return clusterSettings.get(SLOW_QUERY_LOG_THRESHOLD_SETTING);
  }

  /**
   * Returns all DQE settings for registration with OpenSearch.
   *
   * @return list of all DQE settings
   */
  public static List<Setting<?>> getAllSettings() {
    return Arrays.asList(
        SQL_ENGINE_SETTING,
        DQE_ENABLED_SETTING,
        MAX_CONCURRENT_QUERIES_SETTING,
        QUERY_TIMEOUT_SETTING,
        MEMORY_BREAKER_LIMIT_SETTING,
        QUERY_MAX_MEMORY_SETTING,
        EXCHANGE_BUFFER_SIZE_SETTING,
        EXCHANGE_CHUNK_SIZE_SETTING,
        EXCHANGE_TIMEOUT_SETTING,
        EXCHANGE_BACKPRESSURE_TIMEOUT_SETTING,
        SCAN_BATCH_SIZE_SETTING,
        SLOW_QUERY_LOG_THRESHOLD_SETTING,
        WORKER_POOL_SIZE_SETTING,
        EXCHANGE_POOL_SIZE_SETTING,
        COORDINATOR_POOL_SIZE_SETTING);
  }

  // ---------------------------------------------------------------------------
  // Default pool size calculations
  // ---------------------------------------------------------------------------

  static int defaultWorkerPoolSize() {
    return Math.max(1, Math.min(4, Runtime.getRuntime().availableProcessors() / 2));
  }

  static int defaultExchangePoolSize() {
    return Math.max(1, Math.min(2, Runtime.getRuntime().availableProcessors() / 4));
  }

  static int defaultCoordinatorPoolSize() {
    return Math.max(1, Math.min(2, Runtime.getRuntime().availableProcessors() / 4));
  }

}
