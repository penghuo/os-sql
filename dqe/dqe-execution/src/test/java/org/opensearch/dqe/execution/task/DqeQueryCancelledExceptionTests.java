/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.execution.task;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opensearch.dqe.parser.DqeErrorCode;

class DqeQueryCancelledExceptionTests {

  @Test
  @DisplayName("exception carries queryId and reason")
  void carriesQueryIdAndReason() {
    DqeQueryCancelledException ex = new DqeQueryCancelledException("q-123", "User cancelled");

    assertEquals("q-123", ex.getQueryId());
    assertEquals("User cancelled", ex.getReason());
  }

  @Test
  @DisplayName("error code is QUERY_CANCELLED")
  void errorCodeIsQueryCancelled() {
    DqeQueryCancelledException ex = new DqeQueryCancelledException("q-123", "timeout");

    assertEquals(DqeErrorCode.QUERY_CANCELLED, ex.getErrorCode());
  }

  @Test
  @DisplayName("message contains queryId and reason")
  void messageContainsInfo() {
    DqeQueryCancelledException ex = new DqeQueryCancelledException("q-123", "User cancelled");
    String msg = ex.getMessage();

    assertTrue(msg.contains("q-123"));
    assertTrue(msg.contains("cancelled"));
  }
}
