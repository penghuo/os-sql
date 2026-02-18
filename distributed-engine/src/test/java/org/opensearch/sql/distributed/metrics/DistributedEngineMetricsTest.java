/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.metrics;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class DistributedEngineMetricsTest {

  @BeforeEach
  void setUp() {
    DistributedEngineMetrics.getInstance().reset();
  }

  @Test
  @DisplayName("All counters start at zero")
  void testInitialCounters() {
    DistributedEngineMetrics metrics = DistributedEngineMetrics.getInstance();
    assertEquals(0, metrics.getQueriesTotal());
    assertEquals(0, metrics.getQueriesDistributed());
    assertEquals(0, metrics.getFallbackExpected());
    assertEquals(0, metrics.getFallbackError());
  }

  @Test
  @DisplayName("Increment operations are accurate")
  void testIncrements() {
    DistributedEngineMetrics metrics = DistributedEngineMetrics.getInstance();

    metrics.incrementQueriesTotal();
    metrics.incrementQueriesTotal();
    metrics.incrementQueriesDistributed();
    metrics.incrementFallbackExpected();
    metrics.incrementFallbackError();
    metrics.incrementFallbackError();
    metrics.incrementFallbackError();

    assertEquals(2, metrics.getQueriesTotal());
    assertEquals(1, metrics.getQueriesDistributed());
    assertEquals(1, metrics.getFallbackExpected());
    assertEquals(3, metrics.getFallbackError());
  }

  @Test
  @DisplayName("collectMetrics returns all counter values")
  void testCollectMetrics() {
    DistributedEngineMetrics metrics = DistributedEngineMetrics.getInstance();
    metrics.incrementQueriesTotal();
    metrics.incrementQueriesDistributed();

    Map<String, Long> collected = metrics.collectMetrics();
    assertEquals(4, collected.size());
    assertEquals(1L, collected.get("distributed_engine.queries_total"));
    assertEquals(1L, collected.get("distributed_engine.queries_distributed"));
    assertEquals(0L, collected.get("distributed_engine.fallback_expected"));
    assertEquals(0L, collected.get("distributed_engine.fallback_error"));
  }

  @Test
  @DisplayName("Reset clears all counters")
  void testReset() {
    DistributedEngineMetrics metrics = DistributedEngineMetrics.getInstance();
    metrics.incrementQueriesTotal();
    metrics.incrementQueriesDistributed();
    metrics.incrementFallbackExpected();
    metrics.incrementFallbackError();

    metrics.reset();

    assertEquals(0, metrics.getQueriesTotal());
    assertEquals(0, metrics.getQueriesDistributed());
    assertEquals(0, metrics.getFallbackExpected());
    assertEquals(0, metrics.getFallbackError());
  }

  @Test
  @DisplayName("Singleton instance is consistent")
  void testSingleton() {
    DistributedEngineMetrics a = DistributedEngineMetrics.getInstance();
    DistributedEngineMetrics b = DistributedEngineMetrics.getInstance();
    assertSame(a, b);
  }
}
