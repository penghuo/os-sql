/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.plugin.settings;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;

class DqeSettingsTests {

  private ClusterSettings clusterSettings;
  private DqeSettings dqeSettings;

  @BeforeEach
  void setUp() {
    Set<Setting<?>> knownSettings = new HashSet<>(DqeSettings.getAllSettings());
    clusterSettings = new ClusterSettings(Settings.EMPTY, knownSettings);
    dqeSettings = new DqeSettings(clusterSettings);
  }

  @Nested
  @DisplayName("getAllSettings()")
  class AllSettingsTests {

    @Test
    @DisplayName("returns exactly 15 settings")
    void returnsAllSettings() {
      List<Setting<?>> settings = DqeSettings.getAllSettings();
      assertEquals(15, settings.size());
    }

    @Test
    @DisplayName("all settings have unique keys")
    void allSettingsHaveUniqueKeys() {
      List<Setting<?>> settings = DqeSettings.getAllSettings();
      Set<String> keys = new HashSet<>();
      for (Setting<?> s : settings) {
        assertTrue(keys.add(s.getKey()), "Duplicate setting key: " + s.getKey());
      }
    }

    @Test
    @DisplayName("all settings have plugins.sql or plugins.dqe prefix")
    void allSettingsHaveCorrectPrefix() {
      for (Setting<?> s : DqeSettings.getAllSettings()) {
        assertTrue(
            s.getKey().startsWith("plugins.sql.") || s.getKey().startsWith("plugins.dqe."),
            "Setting " + s.getKey() + " does not have correct prefix");
      }
    }
  }

  @Nested
  @DisplayName("Default values")
  class DefaultValueTests {

    @Test
    @DisplayName("SQL_ENGINE defaults to calcite")
    void sqlEngineDefault() {
      assertEquals("calcite", dqeSettings.getEngine());
    }

    @Test
    @DisplayName("DQE_ENABLED defaults to true")
    void dqeEnabledDefault() {
      assertTrue(dqeSettings.isDqeEnabled());
    }

    @Test
    @DisplayName("MAX_CONCURRENT_QUERIES defaults to 10")
    void maxConcurrentQueriesDefault() {
      assertEquals(10, dqeSettings.getMaxConcurrentQueries());
    }

    @Test
    @DisplayName("QUERY_TIMEOUT defaults to 5m")
    void queryTimeoutDefault() {
      assertEquals(TimeValue.timeValueMinutes(5), dqeSettings.getQueryTimeout());
    }

    @Test
    @DisplayName("QUERY_MAX_MEMORY defaults to 256mb")
    void queryMaxMemoryDefault() {
      assertEquals(new ByteSizeValue(256, ByteSizeUnit.MB), dqeSettings.getQueryMaxMemory());
    }

    @Test
    @DisplayName("EXCHANGE_BUFFER_SIZE defaults to 32mb")
    void exchangeBufferSizeDefault() {
      assertEquals(new ByteSizeValue(32, ByteSizeUnit.MB), dqeSettings.getExchangeBufferSize());
    }

    @Test
    @DisplayName("EXCHANGE_CHUNK_SIZE defaults to 1mb")
    void exchangeChunkSizeDefault() {
      assertEquals(new ByteSizeValue(1, ByteSizeUnit.MB), dqeSettings.getExchangeChunkSize());
    }

    @Test
    @DisplayName("EXCHANGE_TIMEOUT defaults to 60s")
    void exchangeTimeoutDefault() {
      assertEquals(TimeValue.timeValueSeconds(60), dqeSettings.getExchangeTimeout());
    }

    @Test
    @DisplayName("EXCHANGE_BACKPRESSURE_TIMEOUT defaults to 30s")
    void exchangeBackpressureTimeoutDefault() {
      assertEquals(TimeValue.timeValueSeconds(30), dqeSettings.getExchangeBackpressureTimeout());
    }

    @Test
    @DisplayName("SCAN_BATCH_SIZE defaults to 1000")
    void scanBatchSizeDefault() {
      assertEquals(1000, dqeSettings.getScanBatchSize());
    }

    @Test
    @DisplayName("SLOW_QUERY_LOG_THRESHOLD defaults to 10s")
    void slowQueryLogThresholdDefault() {
      assertEquals(TimeValue.timeValueSeconds(10), dqeSettings.getSlowQueryLogThreshold());
    }

    @Test
    @DisplayName("MEMORY_BREAKER_LIMIT defaults to 20%")
    void memoryBreakerLimitDefault() {
      assertEquals("20%", DqeSettings.MEMORY_BREAKER_LIMIT_SETTING.getDefault(Settings.EMPTY));
    }
  }

  @Nested
  @DisplayName("Dynamic updates")
  class DynamicUpdateTests {

    @Test
    @DisplayName("SQL_ENGINE can be updated to dqe")
    void updateEngine() {
      clusterSettings.applySettings(Settings.builder().put("plugins.sql.engine", "dqe").build());
      assertEquals("dqe", dqeSettings.getEngine());
    }

    @Test
    @DisplayName("DQE_ENABLED can be set to false")
    void disableDqe() {
      clusterSettings.applySettings(Settings.builder().put("plugins.dqe.enabled", false).build());
      assertFalse(dqeSettings.isDqeEnabled());
    }

    @Test
    @DisplayName("MAX_CONCURRENT_QUERIES can be updated")
    void updateMaxConcurrent() {
      clusterSettings.applySettings(
          Settings.builder().put("plugins.dqe.max_concurrent_queries", 20).build());
      assertEquals(20, dqeSettings.getMaxConcurrentQueries());
    }

    @Test
    @DisplayName("QUERY_TIMEOUT can be updated")
    void updateQueryTimeout() {
      clusterSettings.applySettings(
          Settings.builder().put("plugins.dqe.query_timeout", "10m").build());
      assertEquals(TimeValue.timeValueMinutes(10), dqeSettings.getQueryTimeout());
    }

    @Test
    @DisplayName("SCAN_BATCH_SIZE can be updated")
    void updateScanBatchSize() {
      clusterSettings.applySettings(
          Settings.builder().put("plugins.dqe.scan.batch_size", 500).build());
      assertEquals(500, dqeSettings.getScanBatchSize());
    }

    @Test
    @DisplayName("SLOW_QUERY_LOG_THRESHOLD can be updated")
    void updateSlowQueryThreshold() {
      clusterSettings.applySettings(
          Settings.builder().put("plugins.dqe.slow_query_log.threshold", "30s").build());
      assertEquals(TimeValue.timeValueSeconds(30), dqeSettings.getSlowQueryLogThreshold());
    }
  }

  @Nested
  @DisplayName("Validation")
  class ValidationTests {

    @Test
    @DisplayName("SQL_ENGINE rejects invalid values via Setting.get()")
    void rejectsInvalidEngine() {
      Settings invalid = Settings.builder().put("plugins.sql.engine", "invalid").build();
      assertThrows(
          IllegalArgumentException.class, () -> DqeSettings.SQL_ENGINE_SETTING.get(invalid));
    }

    @Test
    @DisplayName("MAX_CONCURRENT_QUERIES rejects 0 via Setting.get()")
    void rejectsZeroConcurrent() {
      Settings invalid = Settings.builder().put("plugins.dqe.max_concurrent_queries", 0).build();
      assertThrows(
          IllegalArgumentException.class,
          () -> DqeSettings.MAX_CONCURRENT_QUERIES_SETTING.get(invalid));
    }

    @Test
    @DisplayName("SCAN_BATCH_SIZE rejects 0 via Setting.get()")
    void rejectsZeroBatchSize() {
      Settings invalid = Settings.builder().put("plugins.dqe.scan.batch_size", 0).build();
      assertThrows(
          IllegalArgumentException.class, () -> DqeSettings.SCAN_BATCH_SIZE_SETTING.get(invalid));
    }
  }

  @Nested
  @DisplayName("Thread pool defaults")
  class ThreadPoolDefaultTests {

    @Test
    @DisplayName("worker pool size is at least 1")
    void workerPoolMinimum() {
      assertThat(DqeSettings.defaultWorkerPoolSize(), greaterThanOrEqualTo(1));
    }

    @Test
    @DisplayName("worker pool size is at most 4")
    void workerPoolMaximum() {
      assertThat(DqeSettings.defaultWorkerPoolSize(), lessThanOrEqualTo(4));
    }

    @Test
    @DisplayName("exchange pool size is at least 1")
    void exchangePoolMinimum() {
      assertThat(DqeSettings.defaultExchangePoolSize(), greaterThanOrEqualTo(1));
    }

    @Test
    @DisplayName("coordinator pool size is at least 1")
    void coordinatorPoolMinimum() {
      assertThat(DqeSettings.defaultCoordinatorPoolSize(), greaterThanOrEqualTo(1));
    }
  }

  @Nested
  @DisplayName("Static setting properties")
  class SettingPropertiesTests {

    @Test
    @DisplayName("thread pool settings are non-dynamic")
    void threadPoolSettingsNonDynamic() {
      assertFalse(DqeSettings.WORKER_POOL_SIZE_SETTING.isDynamic());
      assertFalse(DqeSettings.EXCHANGE_POOL_SIZE_SETTING.isDynamic());
      assertFalse(DqeSettings.COORDINATOR_POOL_SIZE_SETTING.isDynamic());
    }

    @Test
    @DisplayName("most settings are dynamic")
    void dynamicSettings() {
      assertTrue(DqeSettings.SQL_ENGINE_SETTING.isDynamic());
      assertTrue(DqeSettings.DQE_ENABLED_SETTING.isDynamic());
      assertTrue(DqeSettings.MAX_CONCURRENT_QUERIES_SETTING.isDynamic());
      assertTrue(DqeSettings.QUERY_TIMEOUT_SETTING.isDynamic());
      assertTrue(DqeSettings.SCAN_BATCH_SIZE_SETTING.isDynamic());
      assertTrue(DqeSettings.SLOW_QUERY_LOG_THRESHOLD_SETTING.isDynamic());
    }
  }
}
