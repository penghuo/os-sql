/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.plugin.logging;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.dqe.plugin.request.DqeQueryRequest;
import org.opensearch.dqe.plugin.response.DqeQueryResponse;
import org.opensearch.dqe.plugin.settings.DqeSettings;

@ExtendWith(MockitoExtension.class)
@DisplayName("SlowQueryLogger")
class SlowQueryLoggerTests {

  private DqeSettings settings;
  private SlowQueryLogger logger;

  @BeforeEach
  void setUp() {
    settings = mock(DqeSettings.class);
    lenient()
        .when(settings.getSlowQueryLogThreshold())
        .thenReturn(TimeValue.timeValueSeconds(10));
    logger = new SlowQueryLogger(settings);
  }

  @Nested
  @DisplayName("isSlowQuery")
  class IsSlowQuery {

    @Test
    @DisplayName("returns false for fast query")
    void fastQuery() {
      assertFalse(logger.isSlowQuery(5000));
    }

    @Test
    @DisplayName("returns true at exactly threshold")
    void atThreshold() {
      assertTrue(logger.isSlowQuery(10000));
    }

    @Test
    @DisplayName("returns true above threshold")
    void aboveThreshold() {
      assertTrue(logger.isSlowQuery(15000));
    }

    @Test
    @DisplayName("returns false at zero ms")
    void zeroMs() {
      assertFalse(logger.isSlowQuery(0));
    }
  }

  @Nested
  @DisplayName("maybeLog")
  class MaybeLog {

    @Test
    @DisplayName("does not throw for fast query")
    void fastQueryNoThrow() {
      DqeQueryRequest request = new DqeQueryRequest("SELECT 1", "dqe");
      DqeQueryResponse.QueryStats stats =
          DqeQueryResponse.QueryStats.builder()
              .queryId("q1")
              .elapsedMs(100)
              .rowsProcessed(10)
              .bytesProcessed(512)
              .stages(1)
              .shardsQueried(2)
              .build();
      // Should not throw
      logger.maybeLog(request, stats, 1024);
    }

    @Test
    @DisplayName("does not throw for slow query")
    void slowQueryNoThrow() {
      DqeQueryRequest request = new DqeQueryRequest("SELECT * FROM big_table", "dqe");
      DqeQueryResponse.QueryStats stats =
          DqeQueryResponse.QueryStats.builder()
              .queryId("q2")
              .elapsedMs(20000)
              .rowsProcessed(1000000)
              .bytesProcessed(1024 * 1024)
              .stages(3)
              .shardsQueried(10)
              .build();
      // Should not throw (logs at WARN)
      logger.maybeLog(request, stats, 2048);
    }

    @Test
    @DisplayName("handles long query text truncation")
    void handlesLongQuery() {
      String longQuery = "SELECT " + "a".repeat(2000) + " FROM t";
      DqeQueryRequest request = new DqeQueryRequest(longQuery, "dqe");
      DqeQueryResponse.QueryStats stats =
          DqeQueryResponse.QueryStats.builder()
              .queryId("q3")
              .elapsedMs(15000)
              .rowsProcessed(100)
              .bytesProcessed(4096)
              .stages(1)
              .shardsQueried(1)
              .build();
      // Should not throw
      logger.maybeLog(request, stats, 512);
    }
  }
}
