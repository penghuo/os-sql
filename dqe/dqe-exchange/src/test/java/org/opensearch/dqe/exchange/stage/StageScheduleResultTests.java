/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.exchange.stage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class StageScheduleResultTests {

  @Test
  @DisplayName("getters return constructor values")
  void gettersReturnConstructorValues() {
    StageScheduleResult result = new StageScheduleResult("q1", 2, 5);
    assertEquals("q1", result.getQueryId());
    assertEquals(2, result.getStageId());
    assertEquals(5, result.getDispatchedTaskCount());
  }

  @Test
  @DisplayName("zero dispatched task count is valid")
  void zeroDispatchedTaskCountIsValid() {
    StageScheduleResult result = new StageScheduleResult("q1", 0, 0);
    assertEquals(0, result.getDispatchedTaskCount());
  }

  @Test
  @DisplayName("null queryId throws NullPointerException")
  void nullQueryIdThrows() {
    assertThrows(NullPointerException.class, () -> new StageScheduleResult(null, 0, 0));
  }
}
