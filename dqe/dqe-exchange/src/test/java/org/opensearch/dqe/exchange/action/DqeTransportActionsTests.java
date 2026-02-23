/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.exchange.action;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** Verifies that all DQE transport action names and singleton instances are correctly defined. */
class DqeTransportActionsTests {

  @Test
  @DisplayName("DqeExchangePushAction has correct name and instance")
  void exchangePushAction() {
    assertEquals("internal:dqe/exchange/push", DqeExchangePushAction.NAME);
    assertNotNull(DqeExchangePushAction.INSTANCE);
    assertEquals(DqeExchangePushAction.NAME, DqeExchangePushAction.INSTANCE.name());
  }

  @Test
  @DisplayName("DqeExchangeCloseAction has correct name and instance")
  void exchangeCloseAction() {
    assertEquals("internal:dqe/exchange/close", DqeExchangeCloseAction.NAME);
    assertNotNull(DqeExchangeCloseAction.INSTANCE);
    assertEquals(DqeExchangeCloseAction.NAME, DqeExchangeCloseAction.INSTANCE.name());
  }

  @Test
  @DisplayName("DqeExchangeAbortAction has correct name and instance")
  void exchangeAbortAction() {
    assertEquals("internal:dqe/exchange/abort", DqeExchangeAbortAction.NAME);
    assertNotNull(DqeExchangeAbortAction.INSTANCE);
    assertEquals(DqeExchangeAbortAction.NAME, DqeExchangeAbortAction.INSTANCE.name());
  }

  @Test
  @DisplayName("DqeStageExecuteAction has correct name and instance")
  void stageExecuteAction() {
    assertEquals("internal:dqe/stage/execute", DqeStageExecuteAction.NAME);
    assertNotNull(DqeStageExecuteAction.INSTANCE);
    assertEquals(DqeStageExecuteAction.NAME, DqeStageExecuteAction.INSTANCE.name());
  }

  @Test
  @DisplayName("DqeStageCancelAction has correct name and instance")
  void stageCancelAction() {
    assertEquals("internal:dqe/stage/cancel", DqeStageCancelAction.NAME);
    assertNotNull(DqeStageCancelAction.INSTANCE);
    assertEquals(DqeStageCancelAction.NAME, DqeStageCancelAction.INSTANCE.name());
  }

  @Test
  @DisplayName("all action names use internal: prefix")
  void allActionNamesUseInternalPrefix() {
    String[] actionNames = {
      DqeExchangePushAction.NAME,
      DqeExchangeCloseAction.NAME,
      DqeExchangeAbortAction.NAME,
      DqeStageExecuteAction.NAME,
      DqeStageCancelAction.NAME
    };

    for (String name : actionNames) {
      assertEquals(
          "internal:dqe/",
          name.substring(0, "internal:dqe/".length()),
          "Action name should start with 'internal:dqe/': " + name);
    }
  }
}
