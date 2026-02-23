/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.execution.task;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opensearch.dqe.execution.operator.OperatorContext;
import org.opensearch.dqe.memory.QueryMemoryBudget;

class QueryCancellationHandlerTests {

  private QueryCancellationHandler handler;

  @BeforeEach
  void setUp() {
    handler = new QueryCancellationHandler();
  }

  private OperatorContext createContext(String queryId) {
    QueryMemoryBudget budget = mock(QueryMemoryBudget.class);
    return new OperatorContext(queryId, 0, 0, 0, "TestOp", budget);
  }

  @Test
  @DisplayName("cancelQuery sets interrupt on all registered contexts")
  void cancelSetsInterruptOnAllContexts() {
    OperatorContext ctx1 = createContext("q1");
    OperatorContext ctx2 = createContext("q1");
    handler.registerOperatorContexts("q1", List.of(ctx1, ctx2));

    handler.cancelQuery("q1", "test cancel");

    assertTrue(ctx1.isInterrupted());
    assertTrue(ctx2.isInterrupted());
  }

  @Test
  @DisplayName("isCancelled returns false before cancellation")
  void isCancelledReturnsFalseBeforeCancellation() {
    assertFalse(handler.isCancelled("q1"));
  }

  @Test
  @DisplayName("isCancelled returns true after cancellation")
  void isCancelledReturnsTrueAfterCancellation() {
    handler.cancelQuery("q1", "test");
    assertTrue(handler.isCancelled("q1"));
  }

  @Test
  @DisplayName("cancel before register still interrupts contexts")
  void cancelBeforeRegisterStillInterrupts() {
    handler.cancelQuery("q1", "pre-cancel");

    OperatorContext ctx = createContext("q1");
    handler.registerOperatorContexts("q1", List.of(ctx));

    assertTrue(ctx.isInterrupted());
  }

  @Test
  @DisplayName("deregisterQuery removes tracking")
  void deregisterQueryRemovesTracking() {
    OperatorContext ctx = createContext("q1");
    handler.registerOperatorContexts("q1", List.of(ctx));
    handler.cancelQuery("q1", "test");

    handler.deregisterQuery("q1");
    assertFalse(handler.isCancelled("q1"));
  }

  @Test
  @DisplayName("cancelling non-existent query does not throw")
  void cancelNonExistentQueryNoThrow() {
    handler.cancelQuery("non-existent", "test"); // should not throw
    assertTrue(handler.isCancelled("non-existent"));
  }

  @Test
  @DisplayName("multiple queries tracked independently")
  void multipleQueriesTrackedIndependently() {
    OperatorContext ctx1 = createContext("q1");
    OperatorContext ctx2 = createContext("q2");
    handler.registerOperatorContexts("q1", List.of(ctx1));
    handler.registerOperatorContexts("q2", List.of(ctx2));

    handler.cancelQuery("q1", "cancel q1");

    assertTrue(ctx1.isInterrupted());
    assertFalse(ctx2.isInterrupted());
    assertTrue(handler.isCancelled("q1"));
    assertFalse(handler.isCancelled("q2"));
  }
}
