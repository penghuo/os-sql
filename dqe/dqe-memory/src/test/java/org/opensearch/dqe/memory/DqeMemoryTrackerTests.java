/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.memory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.breaker.CircuitBreakingException;

@ExtendWith(MockitoExtension.class)
class DqeMemoryTrackerTests {

  @Mock private CircuitBreaker dqeBreaker;

  @Mock private CircuitBreaker parentBreaker;

  private DqeMemoryTracker tracker;

  @BeforeEach
  void setUp() {
    tracker = new DqeMemoryTracker(dqeBreaker, parentBreaker);
  }

  @Test
  @DisplayName("reserve adds bytes to both DQE and parent breakers")
  void reserveAddsBytesToBothBreakers() {
    tracker.reserve(1024, "test");

    verify(dqeBreaker).addEstimateBytesAndMaybeBreak(1024, "test");
    verify(parentBreaker).addEstimateBytesAndMaybeBreak(1024, "test");
  }

  @Test
  @DisplayName("reserve updates usedBytes counter")
  void reserveUpdatesUsedBytes() {
    tracker.reserve(1024, "test1");
    tracker.reserve(2048, "test2");

    assertEquals(3072, tracker.getUsedBytes());
  }

  @Test
  @DisplayName("reserve throws when DQE breaker trips")
  void reserveThrowsOnDqeBreakerTrip() {
    CircuitBreakingException cbe =
        new CircuitBreakingException(
            "DQE breaker tripped", 100, 50, CircuitBreaker.Durability.TRANSIENT);
    doThrow(cbe).when(dqeBreaker).addEstimateBytesAndMaybeBreak(anyLong(), anyString());

    assertThrows(CircuitBreakingException.class, () -> tracker.reserve(1024, "test"));
    assertEquals(0, tracker.getUsedBytes());
    verify(parentBreaker, never()).addEstimateBytesAndMaybeBreak(anyLong(), anyString());
  }

  @Test
  @DisplayName("reserve throws when parent breaker trips and rolls back DQE breaker")
  void reserveThrowsOnParentBreakerTripAndRollsBack() {
    CircuitBreakingException cbe =
        new CircuitBreakingException(
            "Parent breaker tripped", 100, 50, CircuitBreaker.Durability.TRANSIENT);
    doThrow(cbe).when(parentBreaker).addEstimateBytesAndMaybeBreak(anyLong(), anyString());

    assertThrows(CircuitBreakingException.class, () -> tracker.reserve(1024, "test"));

    verify(dqeBreaker).addWithoutBreaking(-1024);
    assertEquals(0, tracker.getUsedBytes());
  }

  @Test
  @DisplayName("release reduces bytes in both breakers")
  void releaseReducesBytesInBothBreakers() {
    tracker.reserve(1024, "test");
    tracker.release(512, "test");

    verify(dqeBreaker).addWithoutBreaking(-512);
    verify(parentBreaker).addWithoutBreaking(-512);
    assertEquals(512, tracker.getUsedBytes());
  }

  @Test
  @DisplayName("release full amount returns usedBytes to zero")
  void releaseFullAmountReturnsToZero() {
    tracker.reserve(2048, "test");
    tracker.release(2048, "test");

    assertEquals(0, tracker.getUsedBytes());
  }

  @Test
  @DisplayName("reserve throws IllegalArgumentException for zero bytes")
  void reserveThrowsForZeroBytes() {
    assertThrows(IllegalArgumentException.class, () -> tracker.reserve(0, "test"));
  }

  @Test
  @DisplayName("reserve throws IllegalArgumentException for negative bytes")
  void reserveThrowsForNegativeBytes() {
    assertThrows(IllegalArgumentException.class, () -> tracker.reserve(-1, "test"));
  }

  @Test
  @DisplayName("release throws IllegalArgumentException for zero bytes")
  void releaseThrowsForZeroBytes() {
    assertThrows(IllegalArgumentException.class, () -> tracker.release(0, "test"));
  }

  @Test
  @DisplayName("release throws IllegalArgumentException for negative bytes")
  void releaseThrowsForNegativeBytes() {
    assertThrows(IllegalArgumentException.class, () -> tracker.release(-1, "test"));
  }

  @Test
  @DisplayName("reserve throws NullPointerException for null label")
  void reserveThrowsForNullLabel() {
    assertThrows(NullPointerException.class, () -> tracker.reserve(1024, null));
  }

  @Test
  @DisplayName("release throws NullPointerException for null label")
  void releaseThrowsForNullLabel() {
    assertThrows(NullPointerException.class, () -> tracker.release(1024, null));
  }

  @Test
  @DisplayName("getLimitBytes returns DQE breaker limit")
  void getLimitBytesReturnsDqeBreakerLimit() {
    when(dqeBreaker.getLimit()).thenReturn(1024L * 1024L * 100L);

    assertEquals(1024L * 1024L * 100L, tracker.getLimitBytes());
  }

  @Test
  @DisplayName("getRemainingBytes returns difference between limit and used")
  void getRemainingBytesReturnsCorrectValue() {
    when(dqeBreaker.getLimit()).thenReturn(10000L);
    when(dqeBreaker.getUsed()).thenReturn(3000L);

    assertEquals(7000L, tracker.getRemainingBytes());
  }

  @Test
  @DisplayName("constructor rejects null circuitBreaker")
  void constructorRejectsNullCircuitBreaker() {
    assertThrows(NullPointerException.class, () -> new DqeMemoryTracker(null, parentBreaker));
  }

  @Test
  @DisplayName("constructor rejects null parentBreaker")
  void constructorRejectsNullParentBreaker() {
    assertThrows(NullPointerException.class, () -> new DqeMemoryTracker(dqeBreaker, null));
  }

  @Test
  @DisplayName("multiple reserves and releases track correctly")
  void multipleReservesAndReleasesTrackCorrectly() {
    tracker.reserve(100, "op1");
    tracker.reserve(200, "op2");
    tracker.reserve(300, "op3");
    tracker.release(100, "op1");
    tracker.release(200, "op2");

    assertEquals(300, tracker.getUsedBytes());

    tracker.release(300, "op3");
    assertEquals(0, tracker.getUsedBytes());
  }

  @Test
  @DisplayName("reserve passes label through to breaker")
  void reservePassesLabelToBreaker() {
    String label = "HashAggregation:query-abc-123";
    tracker.reserve(4096, label);

    verify(dqeBreaker).addEstimateBytesAndMaybeBreak(4096, label);
    verify(parentBreaker).addEstimateBytesAndMaybeBreak(4096, label);
  }
}
