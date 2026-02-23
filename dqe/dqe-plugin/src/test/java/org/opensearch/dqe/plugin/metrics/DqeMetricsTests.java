/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.plugin.metrics;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@DisplayName("DqeMetrics")
class DqeMetricsTests {

  private DqeMetrics metrics;

  @BeforeEach
  void setUp() {
    metrics = new DqeMetrics();
  }

  @Nested
  @DisplayName("Initial state")
  class InitialState {

    @Test
    @DisplayName("all counters start at zero")
    void allCountersZero() {
      DqeMetrics.MetricsSnapshot snap = metrics.getSnapshot();
      assertEquals(0, snap.getQueriesSubmitted());
      assertEquals(0, snap.getQueriesSucceeded());
      assertEquals(0, snap.getQueriesFailed());
      assertEquals(0, snap.getQueriesCancelled());
      assertEquals(0, snap.getActiveQueries());
      assertEquals(0, snap.getTotalWallTimeMs());
      assertEquals(0, snap.getTotalRowsReturned());
      assertEquals(0, snap.getTotalRowsScanned());
      assertEquals(0, snap.getTotalBytesScanned());
      assertEquals(0, snap.getMemoryUsedBytes());
      assertEquals(0, snap.getBreakerLimitBytes());
      assertEquals(0, snap.getBreakerTrippedCount());
    }
  }

  @Nested
  @DisplayName("Query lifecycle")
  class QueryLifecycle {

    @Test
    @DisplayName("records submitted queries")
    void recordsSubmitted() {
      metrics.recordQuerySubmitted();
      metrics.recordQuerySubmitted();
      assertEquals(2, metrics.getSnapshot().getQueriesSubmitted());
    }

    @Test
    @DisplayName("records succeeded queries with timing and rows")
    void recordsSucceeded() {
      metrics.recordQuerySucceeded(100, 50);
      metrics.recordQuerySucceeded(200, 30);
      DqeMetrics.MetricsSnapshot snap = metrics.getSnapshot();
      assertEquals(2, snap.getQueriesSucceeded());
      assertEquals(300, snap.getTotalWallTimeMs());
      assertEquals(80, snap.getTotalRowsReturned());
    }

    @Test
    @DisplayName("records failed queries")
    void recordsFailed() {
      metrics.recordQueryFailed("PARSING_ERROR");
      metrics.recordQueryFailed("EXECUTION_ERROR");
      assertEquals(2, metrics.getSnapshot().getQueriesFailed());
    }

    @Test
    @DisplayName("records cancelled queries")
    void recordsCancelled() {
      metrics.recordQueryCancelled();
      assertEquals(1, metrics.getSnapshot().getQueriesCancelled());
    }
  }

  @Nested
  @DisplayName("Active queries gauge")
  class ActiveQueries {

    @Test
    @DisplayName("increments and decrements")
    void incrementDecrement() {
      metrics.incrementActiveQueries();
      metrics.incrementActiveQueries();
      assertEquals(2, metrics.getSnapshot().getActiveQueries());
      metrics.decrementActiveQueries();
      assertEquals(1, metrics.getSnapshot().getActiveQueries());
    }
  }

  @Nested
  @DisplayName("Data volume counters")
  class DataVolume {

    @Test
    @DisplayName("records rows scanned")
    void rowsScanned() {
      metrics.recordRowsScanned(1000);
      metrics.recordRowsScanned(500);
      assertEquals(1500, metrics.getSnapshot().getTotalRowsScanned());
    }

    @Test
    @DisplayName("records bytes scanned")
    void bytesScanned() {
      metrics.recordBytesScanned(4096);
      assertEquals(4096, metrics.getSnapshot().getTotalBytesScanned());
    }
  }

  @Nested
  @DisplayName("Memory gauges")
  class MemoryGauges {

    @Test
    @DisplayName("updates memory used")
    void memoryUsed() {
      metrics.updateMemoryUsed(1024 * 1024);
      assertEquals(1024 * 1024, metrics.getSnapshot().getMemoryUsedBytes());
      metrics.updateMemoryUsed(2048);
      assertEquals(2048, metrics.getSnapshot().getMemoryUsedBytes());
    }

    @Test
    @DisplayName("updates breaker limit")
    void breakerLimit() {
      metrics.updateBreakerLimit(1024 * 1024 * 100);
      assertEquals(1024 * 1024 * 100, metrics.getSnapshot().getBreakerLimitBytes());
    }

    @Test
    @DisplayName("records breaker tripped")
    void breakerTripped() {
      metrics.recordBreakerTripped();
      metrics.recordBreakerTripped();
      assertEquals(2, metrics.getSnapshot().getBreakerTrippedCount());
    }
  }

  @Nested
  @DisplayName("Reset")
  class Reset {

    @Test
    @DisplayName("resets all counters and gauges")
    void resetsAll() {
      metrics.recordQuerySubmitted();
      metrics.recordQuerySucceeded(100, 50);
      metrics.recordQueryFailed("err");
      metrics.recordQueryCancelled();
      metrics.incrementActiveQueries();
      metrics.recordRowsScanned(1000);
      metrics.recordBytesScanned(4096);
      metrics.updateMemoryUsed(2048);
      metrics.updateBreakerLimit(10000);
      metrics.recordBreakerTripped();

      metrics.reset();

      DqeMetrics.MetricsSnapshot snap = metrics.getSnapshot();
      assertEquals(0, snap.getQueriesSubmitted());
      assertEquals(0, snap.getQueriesSucceeded());
      assertEquals(0, snap.getQueriesFailed());
      assertEquals(0, snap.getQueriesCancelled());
      assertEquals(0, snap.getActiveQueries());
      assertEquals(0, snap.getTotalWallTimeMs());
      assertEquals(0, snap.getTotalRowsReturned());
      assertEquals(0, snap.getTotalRowsScanned());
      assertEquals(0, snap.getTotalBytesScanned());
      assertEquals(0, snap.getMemoryUsedBytes());
      assertEquals(0, snap.getBreakerLimitBytes());
      assertEquals(0, snap.getBreakerTrippedCount());
    }
  }

  @Nested
  @DisplayName("MetricsSnapshot.toJson")
  class ToJson {

    @Test
    @DisplayName("produces valid JSON structure")
    void producesValidJson() {
      metrics.recordQuerySubmitted();
      metrics.recordQuerySucceeded(50, 10);
      String json = metrics.getSnapshot().toJson();
      assertTrue(json.startsWith("{"));
      assertTrue(json.endsWith("}"));
      assertTrue(json.contains("\"queries\":"));
      assertTrue(json.contains("\"submitted\":1"));
      assertTrue(json.contains("\"succeeded\":1"));
      assertTrue(json.contains("\"memory\":"));
    }
  }
}
