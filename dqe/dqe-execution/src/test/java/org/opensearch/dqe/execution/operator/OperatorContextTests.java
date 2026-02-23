/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.execution.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.verify;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dqe.execution.task.DqeQueryCancelledException;
import org.opensearch.dqe.memory.QueryMemoryBudget;

@ExtendWith(MockitoExtension.class)
class OperatorContextTests {

  @Mock private QueryMemoryBudget memoryBudget;

  private OperatorContext context;

  @BeforeEach
  void setUp() {
    context = new OperatorContext("q1", 0, 0, 1, "TestOperator", memoryBudget);
  }

  @Test
  @DisplayName("getters return constructor values")
  void gettersReturnConstructorValues() {
    assertEquals("q1", context.getQueryId());
    assertEquals(0, context.getStageId());
    assertEquals(0, context.getPipelineId());
    assertEquals(1, context.getOperatorId());
    assertEquals("TestOperator", context.getOperatorType());
  }

  @Test
  @DisplayName("reserveMemory delegates to budget")
  void reserveMemoryDelegatesToBudget() {
    context.reserveMemory(512);
    verify(memoryBudget).reserve(512, "TestOperator[1]");
    assertEquals(512, context.getReservedBytes());
  }

  @Test
  @DisplayName("releaseMemory delegates to budget")
  void releaseMemoryDelegatesToBudget() {
    context.reserveMemory(512);
    context.releaseMemory(256);
    verify(memoryBudget).release(256, "TestOperator[1]");
    assertEquals(256, context.getReservedBytes());
  }

  @Test
  @DisplayName("interrupted flag defaults to false")
  void interruptedDefaultsFalse() {
    assertFalse(context.isInterrupted());
  }

  @Test
  @DisplayName("setInterrupted changes flag")
  void setInterruptedChangesFlag() {
    context.setInterrupted(true);
    assertTrue(context.isInterrupted());
  }

  @Test
  @DisplayName("checkInterrupted throws when interrupted")
  void checkInterruptedThrowsWhenSet() {
    context.setInterrupted(true);
    assertThrows(DqeQueryCancelledException.class, () -> context.checkInterrupted());
  }

  @Test
  @DisplayName("checkInterrupted does nothing when not interrupted")
  void checkInterruptedPassesWhenNotSet() {
    context.checkInterrupted(); // should not throw
  }

  @Test
  @DisplayName("input and output position tracking")
  void positionTracking() {
    context.addInputPositions(100);
    context.addInputPositions(50);
    assertEquals(150, context.getInputPositions());

    context.addOutputPositions(80);
    assertEquals(80, context.getOutputPositions());
  }

  @Test
  @DisplayName("constructor rejects null queryId")
  void rejectsNullQueryId() {
    assertThrows(
        NullPointerException.class, () -> new OperatorContext(null, 0, 0, 0, "op", memoryBudget));
  }

  @Test
  @DisplayName("constructor rejects null operatorType")
  void rejectsNullOperatorType() {
    assertThrows(
        NullPointerException.class, () -> new OperatorContext("q1", 0, 0, 0, null, memoryBudget));
  }

  @Test
  @DisplayName("constructor rejects null memoryBudget")
  void rejectsNullMemoryBudget() {
    assertThrows(NullPointerException.class, () -> new OperatorContext("q1", 0, 0, 0, "op", null));
  }
}
