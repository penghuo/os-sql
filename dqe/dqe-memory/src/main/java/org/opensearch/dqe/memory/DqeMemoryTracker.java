/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.memory;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.breaker.CircuitBreakingException;

/**
 * Central memory tracking for all DQE allocations. Wraps OpenSearch's {@link CircuitBreaker}
 * interface.
 *
 * <p>Every DQE memory allocation goes through this tracker. Each call to {@link #reserve(long,
 * String)} checks both the DQE-specific breaker and the parent breaker (total heap pressure). This
 * ensures:
 *
 * <ul>
 *   <li>If the DQE breaker limit is hit, only DQE queries fail.
 *   <li>If the parent breaker limit is hit, the DQE query fails — protecting the cluster.
 * </ul>
 *
 * <p>Consumed by: dqe-execution (OperatorContext), dqe-exchange (ExchangeBuffer).
 */
public class DqeMemoryTracker {

  private final CircuitBreaker dqeBreaker;
  private final CircuitBreaker parentBreaker;
  private final AtomicLong usedBytes;

  /**
   * Create a new memory tracker.
   *
   * @param circuitBreaker the "dqe" circuit breaker instance
   * @param parentBreaker the parent circuit breaker for total heap check
   */
  public DqeMemoryTracker(CircuitBreaker circuitBreaker, CircuitBreaker parentBreaker) {
    this.dqeBreaker = Objects.requireNonNull(circuitBreaker, "circuitBreaker must not be null");
    this.parentBreaker = Objects.requireNonNull(parentBreaker, "parentBreaker must not be null");
    this.usedBytes = new AtomicLong(0);
  }

  /**
   * Reserve bytes under the DQE breaker and parent breaker.
   *
   * <p>The reservation is first checked against the DQE breaker, then against the parent breaker.
   * If either limit is exceeded, a {@link CircuitBreakingException} is thrown and no bytes are
   * reserved.
   *
   * @param bytes number of bytes to reserve (must be &gt; 0)
   * @param label descriptive label for debugging (e.g., "HashAggregation:query-123")
   * @throws CircuitBreakingException if DQE breaker or parent breaker limit exceeded
   * @throws IllegalArgumentException if bytes &lt;= 0
   */
  public void reserve(long bytes, String label) throws CircuitBreakingException {
    if (bytes <= 0) {
      throw new IllegalArgumentException("reserve bytes must be > 0, got: " + bytes);
    }
    Objects.requireNonNull(label, "label must not be null");

    // Check DQE breaker first
    try {
      dqeBreaker.addEstimateBytesAndMaybeBreak(bytes, label);
    } catch (CircuitBreakingException e) {
      throw e;
    }

    // Check parent breaker
    try {
      parentBreaker.addEstimateBytesAndMaybeBreak(bytes, label);
    } catch (CircuitBreakingException e) {
      // Roll back the DQE breaker reservation
      dqeBreaker.addWithoutBreaking(-bytes);
      throw e;
    }

    usedBytes.addAndGet(bytes);
  }

  /**
   * Release previously reserved bytes.
   *
   * @param bytes number of bytes to release (must be &gt; 0)
   * @param label same label used in {@link #reserve(long, String)} for tracking
   * @throws IllegalArgumentException if bytes &lt;= 0
   */
  public void release(long bytes, String label) {
    if (bytes <= 0) {
      throw new IllegalArgumentException("release bytes must be > 0, got: " + bytes);
    }
    Objects.requireNonNull(label, "label must not be null");

    dqeBreaker.addWithoutBreaking(-bytes);
    parentBreaker.addWithoutBreaking(-bytes);
    usedBytes.addAndGet(-bytes);
  }

  /**
   * Current total bytes used by all DQE queries on this node.
   *
   * @return total bytes currently reserved
   */
  public long getUsedBytes() {
    return usedBytes.get();
  }

  /**
   * Maximum bytes allowed by the DQE circuit breaker.
   *
   * @return limit in bytes
   */
  public long getLimitBytes() {
    return dqeBreaker.getLimit();
  }

  /**
   * Remaining bytes before the DQE breaker trips.
   *
   * @return remaining bytes
   */
  public long getRemainingBytes() {
    return dqeBreaker.getLimit() - dqeBreaker.getUsed();
  }
}
