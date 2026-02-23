/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.memory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.breaker.CircuitBreakingException;
import org.opensearch.dqe.parser.DqeErrorCode;
import org.opensearch.dqe.parser.DqeException;

@ExtendWith(MockitoExtension.class)
class QueryMemoryBudgetTests {

  @Mock private CircuitBreaker dqeBreaker;

  @Mock private CircuitBreaker parentBreaker;

  private DqeMemoryTracker tracker;
  private QueryMemoryBudget budget;

  @BeforeEach
  void setUp() {
    tracker = new DqeMemoryTracker(dqeBreaker, parentBreaker);
    budget = new QueryMemoryBudget("query-1", 1024, tracker);
  }

  @Test
  @DisplayName("reserve allocates from budget and tracker")
  void reserveAllocatesFromBudgetAndTracker() {
    budget.reserve(100, "op1");

    assertEquals(100, budget.getUsedBytes());
    assertEquals(100, tracker.getUsedBytes());
    verify(dqeBreaker).addEstimateBytesAndMaybeBreak(100, "query-1:op1");
    verify(parentBreaker).addEstimateBytesAndMaybeBreak(100, "query-1:op1");
  }

  @Test
  @DisplayName("reserve throws DqeException when budget exceeded")
  void reserveThrowsWhenBudgetExceeded() {
    budget.reserve(900, "op1");

    DqeException ex = assertThrows(DqeException.class, () -> budget.reserve(200, "op2"));
    assertEquals(DqeErrorCode.EXCEEDED_QUERY_MEMORY_LIMIT, ex.getErrorCode());
    assertEquals(900, budget.getUsedBytes());
  }

  @Test
  @DisplayName("reserve does not call tracker when budget check fails")
  void reserveDoesNotCallTrackerOnBudgetExceeded() {
    budget.reserve(900, "op1");

    assertThrows(DqeException.class, () -> budget.reserve(200, "op2"));
    // Only one call for the first reserve, none for the second
    verify(dqeBreaker).addEstimateBytesAndMaybeBreak(900, "query-1:op1");
  }

  @Test
  @DisplayName("reserve propagates CircuitBreakingException from tracker")
  void reservePropagatesCircuitBreakingException() {
    CircuitBreakingException cbe =
        new CircuitBreakingException(
            "DQE breaker tripped", 100, 50, CircuitBreaker.Durability.TRANSIENT);
    doThrow(cbe).when(dqeBreaker).addEstimateBytesAndMaybeBreak(anyLong(), anyString());

    assertThrows(CircuitBreakingException.class, () -> budget.reserve(100, "op1"));
    assertEquals(0, budget.getUsedBytes());
  }

  @Test
  @DisplayName("release returns bytes to budget and tracker")
  void releaseReturnsBytesToBudgetAndTracker() {
    budget.reserve(500, "op1");
    budget.release(200, "op1");

    assertEquals(300, budget.getUsedBytes());
    verify(dqeBreaker).addWithoutBreaking(-200);
    verify(parentBreaker).addWithoutBreaking(-200);
  }

  @Test
  @DisplayName("releaseAll releases all memory and is idempotent")
  void releaseAllReleasesAllMemory() {
    budget.reserve(500, "op1");
    budget.reserve(300, "op2");

    budget.releaseAll();
    assertEquals(0, budget.getUsedBytes());
    assertEquals(0, tracker.getUsedBytes());

    // Second call is a no-op
    budget.releaseAll();
    assertEquals(0, budget.getUsedBytes());
  }

  @Test
  @DisplayName("releaseAll with zero usage does not call tracker release")
  void releaseAllWithZeroUsageIsNoop() {
    budget.releaseAll();
    assertEquals(0, budget.getUsedBytes());
    // No release calls should be made to the breakers
    verify(dqeBreaker, never()).addWithoutBreaking(anyLong());
  }

  @Test
  @DisplayName("getRemainingBytes returns correct value")
  void getRemainingBytesReturnsCorrectValue() {
    budget.reserve(300, "op1");
    assertEquals(724, budget.getRemainingBytes());
  }

  @Test
  @DisplayName("getBudgetBytes returns configured budget")
  void getBudgetBytesReturnsConfiguredBudget() {
    assertEquals(1024, budget.getBudgetBytes());
  }

  @Test
  @DisplayName("getQueryId returns correct id")
  void getQueryIdReturnsCorrectId() {
    assertEquals("query-1", budget.getQueryId());
  }

  @Test
  @DisplayName("constructor rejects null queryId")
  void constructorRejectsNullQueryId() {
    assertThrows(NullPointerException.class, () -> new QueryMemoryBudget(null, 1024, tracker));
  }

  @Test
  @DisplayName("constructor rejects zero budget")
  void constructorRejectsZeroBudget() {
    assertThrows(IllegalArgumentException.class, () -> new QueryMemoryBudget("q1", 0, tracker));
  }

  @Test
  @DisplayName("constructor rejects negative budget")
  void constructorRejectsNegativeBudget() {
    assertThrows(IllegalArgumentException.class, () -> new QueryMemoryBudget("q1", -100, tracker));
  }

  @Test
  @DisplayName("constructor rejects null tracker")
  void constructorRejectsNullTracker() {
    assertThrows(NullPointerException.class, () -> new QueryMemoryBudget("q1", 1024, null));
  }

  @Test
  @DisplayName("reserve rejects zero bytes")
  void reserveRejectsZeroBytes() {
    assertThrows(IllegalArgumentException.class, () -> budget.reserve(0, "op"));
  }

  @Test
  @DisplayName("reserve rejects negative bytes")
  void reserveRejectsNegativeBytes() {
    assertThrows(IllegalArgumentException.class, () -> budget.reserve(-1, "op"));
  }

  @Test
  @DisplayName("reserve rejects null label")
  void reserveRejectsNullLabel() {
    assertThrows(NullPointerException.class, () -> budget.reserve(100, null));
  }

  @Test
  @DisplayName("release rejects zero bytes")
  void releaseRejectsZeroBytes() {
    assertThrows(IllegalArgumentException.class, () -> budget.release(0, "op"));
  }

  @Test
  @DisplayName("reserve at exact budget limit succeeds")
  void reserveAtExactBudgetLimitSucceeds() {
    budget.reserve(1024, "op1");
    assertEquals(1024, budget.getUsedBytes());
    assertEquals(0, budget.getRemainingBytes());
  }

  @Test
  @DisplayName("reserve one byte over budget fails")
  void reserveOneByteOverBudgetFails() {
    budget.reserve(1024, "op1");
    DqeException ex = assertThrows(DqeException.class, () -> budget.reserve(1, "op2"));
    assertTrue(ex.getMessage().contains("exceeded memory budget"));
  }

  @Test
  @DisplayName("multiple reserves and releases track correctly")
  void multipleReservesAndReleasesTrackCorrectly() {
    budget.reserve(100, "op1");
    budget.reserve(200, "op2");
    budget.reserve(300, "op3");
    budget.release(100, "op1");
    budget.release(200, "op2");

    assertEquals(300, budget.getUsedBytes());

    budget.release(300, "op3");
    assertEquals(0, budget.getUsedBytes());
  }

  @Test
  @DisplayName("reserve rolls back local budget when tracker throws")
  void reserveRollsBackOnTrackerFailure() {
    // First reservation succeeds
    budget.reserve(100, "op1");
    assertEquals(100, budget.getUsedBytes());

    // Second reservation: tracker will throw
    CircuitBreakingException cbe =
        new CircuitBreakingException(
            "DQE breaker tripped", 200, 150, CircuitBreaker.Durability.TRANSIENT);
    doThrow(cbe).when(dqeBreaker).addEstimateBytesAndMaybeBreak(200, "query-1:op2");

    assertThrows(CircuitBreakingException.class, () -> budget.reserve(200, "op2"));
    // Local budget should be rolled back to 100, not 300
    assertEquals(100, budget.getUsedBytes());
  }

  @Test
  @DisplayName("concurrent reserves do not exceed budget")
  void concurrentReservesDoNotExceedBudget() throws InterruptedException {
    // Budget of 1000 bytes; 20 threads each try to reserve 100 bytes.
    // Exactly 10 should succeed, the rest should fail.
    QueryMemoryBudget concurrentBudget = new QueryMemoryBudget("concurrent-q", 1000, tracker);

    int threadCount = 20;
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch doneLatch = new CountDownLatch(threadCount);
    AtomicInteger successes = new AtomicInteger(0);
    AtomicInteger failures = new AtomicInteger(0);

    ExecutorService executor = Executors.newFixedThreadPool(threadCount);
    for (int i = 0; i < threadCount; i++) {
      final int idx = i;
      executor.submit(
          () -> {
            try {
              startLatch.await();
              concurrentBudget.reserve(100, "thread-" + idx);
              successes.incrementAndGet();
            } catch (DqeException e) {
              failures.incrementAndGet();
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            } finally {
              doneLatch.countDown();
            }
          });
    }

    startLatch.countDown();
    doneLatch.await();
    executor.shutdown();

    assertEquals(10, successes.get());
    assertEquals(10, failures.get());
    assertEquals(1000, concurrentBudget.getUsedBytes());
  }
}
