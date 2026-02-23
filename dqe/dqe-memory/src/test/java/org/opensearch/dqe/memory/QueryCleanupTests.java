/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.memory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.core.common.breaker.CircuitBreaker;

@ExtendWith(MockitoExtension.class)
class QueryCleanupTests {

  @Mock private CircuitBreaker dqeBreaker;

  @Mock private CircuitBreaker parentBreaker;

  private DqeMemoryTracker tracker;
  private QueryMemoryBudget budget;
  private QueryCleanup cleanup;

  @BeforeEach
  void setUp() {
    tracker = new DqeMemoryTracker(dqeBreaker, parentBreaker);
    budget = new QueryMemoryBudget("query-1", 10000, tracker);
    cleanup = new QueryCleanup(budget);
  }

  @Test
  @DisplayName("cleanup releases all budget memory")
  void cleanupReleasesAllBudgetMemory() {
    budget.reserve(500, "op1");
    budget.reserve(300, "op2");

    cleanup.cleanup();

    assertEquals(0, budget.getUsedBytes());
    assertTrue(cleanup.isCleaned());
  }

  @Test
  @DisplayName("cleanup runs registered actions in LIFO order")
  void cleanupRunsActionsInLifoOrder() {
    List<String> executionOrder = new ArrayList<>();
    cleanup.registerCleanupAction(() -> executionOrder.add("first"));
    cleanup.registerCleanupAction(() -> executionOrder.add("second"));
    cleanup.registerCleanupAction(() -> executionOrder.add("third"));

    cleanup.cleanup();

    assertEquals(List.of("third", "second", "first"), executionOrder);
  }

  @Test
  @DisplayName("cleanup continues after action failure")
  void cleanupContinuesAfterActionFailure() {
    List<String> executionOrder = new ArrayList<>();
    cleanup.registerCleanupAction(() -> executionOrder.add("first"));
    cleanup.registerCleanupAction(
        () -> {
          throw new RuntimeException("deliberate failure");
        });
    cleanup.registerCleanupAction(() -> executionOrder.add("third"));

    cleanup.cleanup();

    // All non-failing actions should run
    assertEquals(List.of("third", "first"), executionOrder);
    assertTrue(cleanup.isCleaned());
  }

  @Test
  @DisplayName("cleanup is idempotent")
  void cleanupIsIdempotent() {
    List<Integer> callCount = new ArrayList<>();
    cleanup.registerCleanupAction(() -> callCount.add(1));

    budget.reserve(500, "op1");

    cleanup.cleanup();
    cleanup.cleanup();
    cleanup.cleanup();

    // Action runs only once
    assertEquals(1, callCount.size());
    assertEquals(0, budget.getUsedBytes());
  }

  @Test
  @DisplayName("isCleaned returns false before cleanup")
  void isCleanedReturnsFalseBeforeCleanup() {
    assertFalse(cleanup.isCleaned());
  }

  @Test
  @DisplayName("isCleaned returns true after cleanup")
  void isCleanedReturnsTrueAfterCleanup() {
    cleanup.cleanup();
    assertTrue(cleanup.isCleaned());
  }

  @Test
  @DisplayName("registerCleanupAction throws after cleanup")
  void registerCleanupActionThrowsAfterCleanup() {
    cleanup.cleanup();

    assertThrows(IllegalStateException.class, () -> cleanup.registerCleanupAction(() -> {}));
  }

  @Test
  @DisplayName("cleanup with no registered actions just releases memory")
  void cleanupWithNoActionsReleasesMemory() {
    budget.reserve(500, "op1");

    cleanup.cleanup();

    assertEquals(0, budget.getUsedBytes());
    assertTrue(cleanup.isCleaned());
  }

  @Test
  @DisplayName("constructor rejects null budget")
  void constructorRejectsNullBudget() {
    assertThrows(NullPointerException.class, () -> new QueryCleanup(null));
  }

  @Test
  @DisplayName("registerCleanupAction rejects null action")
  void registerCleanupActionRejectsNull() {
    assertThrows(NullPointerException.class, () -> cleanup.registerCleanupAction(null));
  }

  @Test
  @DisplayName("cleanup releases memory even when all actions fail")
  void cleanupReleasesMemoryEvenWhenAllActionsFail() {
    budget.reserve(500, "op1");
    cleanup.registerCleanupAction(
        () -> {
          throw new RuntimeException("fail 1");
        });
    cleanup.registerCleanupAction(
        () -> {
          throw new RuntimeException("fail 2");
        });

    cleanup.cleanup();

    assertEquals(0, budget.getUsedBytes());
    assertTrue(cleanup.isCleaned());
  }
}
