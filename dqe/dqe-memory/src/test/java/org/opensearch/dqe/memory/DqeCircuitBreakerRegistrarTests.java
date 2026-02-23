/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.memory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.indices.breaker.CircuitBreakerService;
import org.opensearch.indices.breaker.BreakerSettings;

@ExtendWith(MockitoExtension.class)
class DqeCircuitBreakerRegistrarTests {

  @Mock private CircuitBreakerService circuitBreakerService;

  @Mock private CircuitBreaker dqeBreaker;

  @Mock private CircuitBreaker parentBreaker;

  @Test
  @DisplayName("createBreakerSettings returns settings with correct name and limit")
  void createBreakerSettingsReturnsCorrectSettings() {
    long limit = 1024L * 1024L * 200L; // 200MB
    BreakerSettings settings = DqeCircuitBreakerRegistrar.createBreakerSettings(limit);

    assertEquals(DqeCircuitBreakerRegistrar.DQE_BREAKER_NAME, settings.getName());
    assertEquals(limit, settings.getLimit());
    assertEquals(CircuitBreaker.Type.MEMORY, settings.getType());
    assertEquals(CircuitBreaker.Durability.TRANSIENT, settings.getDurability());
  }

  @Test
  @DisplayName("createBreakerSettings throws on zero limit")
  void createBreakerSettingsThrowsOnZeroLimit() {
    assertThrows(
        IllegalArgumentException.class, () -> DqeCircuitBreakerRegistrar.createBreakerSettings(0));
  }

  @Test
  @DisplayName("createBreakerSettings throws on negative limit")
  void createBreakerSettingsThrowsOnNegativeLimit() {
    assertThrows(
        IllegalArgumentException.class,
        () -> DqeCircuitBreakerRegistrar.createBreakerSettings(-100));
  }

  @Test
  @DisplayName("createTracker returns a functional DqeMemoryTracker")
  void createTrackerReturnsFunctionalTracker() {
    when(circuitBreakerService.getBreaker(CircuitBreaker.PARENT)).thenReturn(parentBreaker);

    DqeMemoryTracker tracker =
        DqeCircuitBreakerRegistrar.createTracker(dqeBreaker, circuitBreakerService);

    assertNotNull(tracker);
    tracker.reserve(100, "test");
    assertEquals(100, tracker.getUsedBytes());
  }

  @Test
  @DisplayName("createTracker throws on null dqeBreaker")
  void createTrackerThrowsOnNullDqeBreaker() {
    assertThrows(
        NullPointerException.class,
        () -> DqeCircuitBreakerRegistrar.createTracker(null, circuitBreakerService));
  }

  @Test
  @DisplayName("createTracker throws on null circuitBreakerService")
  void createTrackerThrowsOnNullService() {
    assertThrows(
        NullPointerException.class,
        () -> DqeCircuitBreakerRegistrar.createTracker(dqeBreaker, null));
  }

  @Test
  @DisplayName("register returns a functional DqeMemoryTracker")
  void registerReturnsFunctionalTracker() {
    when(circuitBreakerService.getBreaker(DqeCircuitBreakerRegistrar.DQE_BREAKER_NAME))
        .thenReturn(dqeBreaker);
    when(circuitBreakerService.getBreaker(CircuitBreaker.PARENT)).thenReturn(parentBreaker);

    DqeMemoryTracker tracker =
        DqeCircuitBreakerRegistrar.register(circuitBreakerService, 1024L * 1024L * 100L);

    assertNotNull(tracker);
    tracker.reserve(100, "test");
    assertEquals(100, tracker.getUsedBytes());
  }

  @Test
  @DisplayName("register throws on null service")
  void registerThrowsOnNullService() {
    assertThrows(
        NullPointerException.class, () -> DqeCircuitBreakerRegistrar.register(null, 1024L));
  }

  @Test
  @DisplayName("register throws on zero limit")
  void registerThrowsOnZeroLimit() {
    assertThrows(
        IllegalArgumentException.class,
        () -> DqeCircuitBreakerRegistrar.register(circuitBreakerService, 0));
  }

  @Test
  @DisplayName("register throws on negative limit")
  void registerThrowsOnNegativeLimit() {
    assertThrows(
        IllegalArgumentException.class,
        () -> DqeCircuitBreakerRegistrar.register(circuitBreakerService, -100));
  }

  @Test
  @DisplayName("deregister resets DQE breaker usage to zero")
  void deregisterResetsUsageToZero() {
    when(circuitBreakerService.getBreaker(DqeCircuitBreakerRegistrar.DQE_BREAKER_NAME))
        .thenReturn(dqeBreaker);
    when(dqeBreaker.getUsed()).thenReturn(5000L);

    DqeCircuitBreakerRegistrar.deregister(circuitBreakerService);

    verify(dqeBreaker).addWithoutBreaking(-5000L);
  }

  @Test
  @DisplayName("deregister handles zero usage gracefully")
  void deregisterHandlesZeroUsage() {
    when(circuitBreakerService.getBreaker(DqeCircuitBreakerRegistrar.DQE_BREAKER_NAME))
        .thenReturn(dqeBreaker);
    when(dqeBreaker.getUsed()).thenReturn(0L);

    // Should not throw
    DqeCircuitBreakerRegistrar.deregister(circuitBreakerService);
  }

  @Test
  @DisplayName("deregister throws on null service")
  void deregisterThrowsOnNullService() {
    assertThrows(NullPointerException.class, () -> DqeCircuitBreakerRegistrar.deregister(null));
  }

  @Test
  @DisplayName("DQE breaker name constant is 'dqe'")
  void breakerNameConstant() {
    assertEquals("dqe", DqeCircuitBreakerRegistrar.DQE_BREAKER_NAME);
  }
}
