/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.plugin.logging;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@DisplayName("DqeAuditLogger")
class DqeAuditLoggerTests {

  private DqeAuditLogger auditLogger;

  @BeforeEach
  void setUp() {
    auditLogger = new DqeAuditLogger();
  }

  @Nested
  @DisplayName("logQueryStarted")
  class LogQueryStarted {

    @Test
    @DisplayName("logs without error for valid inputs")
    void logsValidInputs() {
      assertDoesNotThrow(
          () ->
              auditLogger.logQueryStarted(
                  "q-1", "testuser", "SELECT * FROM t", List.of("my_index")));
    }

    @Test
    @DisplayName("handles null query gracefully")
    void handlesNullQuery() {
      assertDoesNotThrow(() -> auditLogger.logQueryStarted("q-2", "testuser", null, List.of()));
    }

    @Test
    @DisplayName("handles empty indices list")
    void handlesEmptyIndices() {
      assertDoesNotThrow(
          () -> auditLogger.logQueryStarted("q-3", "testuser", "SELECT 1", List.of()));
    }
  }

  @Nested
  @DisplayName("logQuerySucceeded")
  class LogQuerySucceeded {

    @Test
    @DisplayName("logs without error for valid inputs")
    void logsValidInputs() {
      assertDoesNotThrow(() -> auditLogger.logQuerySucceeded("q-1", "testuser", 150, 42));
    }

    @Test
    @DisplayName("handles zero elapsed and rows")
    void handlesZeroValues() {
      assertDoesNotThrow(() -> auditLogger.logQuerySucceeded("q-2", "testuser", 0, 0));
    }
  }

  @Nested
  @DisplayName("logQueryFailed")
  class LogQueryFailed {

    @Test
    @DisplayName("logs without error for valid inputs")
    void logsValidInputs() {
      assertDoesNotThrow(() -> auditLogger.logQueryFailed("q-1", "testuser", "SYNTAX_ERROR"));
    }

    @Test
    @DisplayName("handles null error reason")
    void handlesNullReason() {
      assertDoesNotThrow(() -> auditLogger.logQueryFailed("q-2", "testuser", null));
    }
  }

  @Nested
  @DisplayName("logQueryCancelled")
  class LogQueryCancelled {

    @Test
    @DisplayName("logs without error for valid inputs")
    void logsValidInputs() {
      assertDoesNotThrow(() -> auditLogger.logQueryCancelled("q-1", "testuser", "user_cancelled"));
    }

    @Test
    @DisplayName("handles timeout cancellation")
    void handlesTimeoutReason() {
      assertDoesNotThrow(() -> auditLogger.logQueryCancelled("q-2", "testuser", "timeout"));
    }
  }

  @Nested
  @DisplayName("Query truncation")
  class QueryTruncation {

    @Test
    @DisplayName("short queries are not truncated")
    void shortQueryNotTruncated() {
      // Just verify no exception -- the logger uses a private truncation method
      assertDoesNotThrow(() -> auditLogger.logQueryStarted("q-1", "user", "SELECT 1", List.of()));
    }

    @Test
    @DisplayName("long queries are truncated without error")
    void longQueryTruncated() {
      String longQuery = "SELECT " + "x".repeat(1000) + " FROM t";
      assertDoesNotThrow(() -> auditLogger.logQueryStarted("q-2", "user", longQuery, List.of("t")));
    }

    @Test
    @DisplayName("MAX_AUDIT_QUERY_LENGTH is 500")
    void maxLengthIs500() {
      assertEquals(500, DqeAuditLogger.MAX_AUDIT_QUERY_LENGTH);
    }
  }
}
