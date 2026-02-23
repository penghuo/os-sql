/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.memory;

import java.util.Objects;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.indices.breaker.CircuitBreakerService;
import org.opensearch.indices.breaker.BreakerSettings;

/**
 * Provides circuit breaker configuration for the DQE engine.
 *
 * <p>The DQE circuit breaker is a child of the OpenSearch parent circuit breaker. Its default limit
 * is 20% of heap (configurable via {@code plugins.dqe.memory.breaker_limit}).
 *
 * <p>Usage:
 *
 * <ol>
 *   <li>Plugin calls {@link #createBreakerSettings(long)} to get the {@link BreakerSettings} for
 *       returning from {@code CircuitBreakerPlugin.getCircuitBreaker()}
 *   <li>Plugin receives the created {@link CircuitBreaker} via {@code
 *       CircuitBreakerPlugin.setCircuitBreaker()}, then calls {@link #createTracker(CircuitBreaker,
 *       CircuitBreakerService)} to create the {@link DqeMemoryTracker}
 * </ol>
 */
public class DqeCircuitBreakerRegistrar {

  /** The name used to register the DQE circuit breaker. */
  public static final String DQE_BREAKER_NAME = "dqe";

  private DqeCircuitBreakerRegistrar() {
    // static utility class
  }

  /**
   * Create the breaker settings for the DQE circuit breaker.
   *
   * <p>The returned settings should be passed from {@code
   * CircuitBreakerPlugin.getCircuitBreaker(Settings)} in the plugin.
   *
   * @param breakerLimitBytes limit in bytes (default: 20% of heap)
   * @return breaker settings for registration
   * @throws IllegalArgumentException if breakerLimitBytes &lt;= 0
   */
  public static BreakerSettings createBreakerSettings(long breakerLimitBytes) {
    if (breakerLimitBytes <= 0) {
      throw new IllegalArgumentException(
          "breakerLimitBytes must be > 0, got: " + breakerLimitBytes);
    }

    return new BreakerSettings(
        DQE_BREAKER_NAME,
        breakerLimitBytes,
        1.0, // overhead multiplier: no additional overhead
        CircuitBreaker.Type.MEMORY,
        CircuitBreaker.Durability.TRANSIENT);
  }

  /**
   * Create a {@link DqeMemoryTracker} from the DQE breaker and the breaker service.
   *
   * <p>Called after OpenSearch has created the circuit breaker and invoked {@code
   * CircuitBreakerPlugin.setCircuitBreaker()}.
   *
   * @param dqeBreaker the created DQE circuit breaker
   * @param circuitBreakerService OpenSearch's breaker service (for parent breaker)
   * @return a configured DqeMemoryTracker
   */
  public static DqeMemoryTracker createTracker(
      CircuitBreaker dqeBreaker, CircuitBreakerService circuitBreakerService) {
    Objects.requireNonNull(dqeBreaker, "dqeBreaker must not be null");
    Objects.requireNonNull(circuitBreakerService, "circuitBreakerService must not be null");

    CircuitBreaker parentBreaker = circuitBreakerService.getBreaker(CircuitBreaker.PARENT);
    return new DqeMemoryTracker(dqeBreaker, parentBreaker);
  }

  /**
   * Combined convenience method: create breaker settings, register via the service, and return a
   * tracker. This is a shortcut for testing or when the plugin initializes the breaker service
   * directly.
   *
   * @param circuitBreakerService OpenSearch's CircuitBreakerService
   * @param breakerLimitBytes limit in bytes (default: 20% of heap)
   * @return a configured DqeMemoryTracker
   */
  public static DqeMemoryTracker register(
      CircuitBreakerService circuitBreakerService, long breakerLimitBytes) {
    Objects.requireNonNull(circuitBreakerService, "circuitBreakerService must not be null");
    if (breakerLimitBytes <= 0) {
      throw new IllegalArgumentException(
          "breakerLimitBytes must be > 0, got: " + breakerLimitBytes);
    }

    CircuitBreaker dqeBreaker = circuitBreakerService.getBreaker(DQE_BREAKER_NAME);
    CircuitBreaker parentBreaker = circuitBreakerService.getBreaker(CircuitBreaker.PARENT);
    return new DqeMemoryTracker(dqeBreaker, parentBreaker);
  }

  /**
   * Clean up the DQE circuit breaker during plugin shutdown.
   *
   * <p>Resets the breaker's usage to zero. Callers should ensure all queries are completed or
   * cancelled before calling this.
   *
   * @param circuitBreakerService OpenSearch's CircuitBreakerService
   */
  public static void deregister(CircuitBreakerService circuitBreakerService) {
    Objects.requireNonNull(circuitBreakerService, "circuitBreakerService must not be null");

    CircuitBreaker dqeBreaker = circuitBreakerService.getBreaker(DQE_BREAKER_NAME);
    if (dqeBreaker != null) {
      long used = dqeBreaker.getUsed();
      if (used > 0) {
        dqeBreaker.addWithoutBreaking(-used);
      }
    }
  }
}
