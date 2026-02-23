/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.memory;

import java.util.concurrent.Semaphore;

/**
 * Node-level semaphore limiting concurrent DQE queries.
 *
 * <p>When the semaphore is full, new queries are rejected immediately. There is no queueing — the
 * orchestrator is expected to throw {@code DqeException(TOO_MANY_CONCURRENT_QUERIES)} when {@link
 * #tryAcquire()} returns {@code false}.
 *
 * <p>Default: 10 concurrent queries (configurable via {@code plugins.dqe.max_concurrent_queries}).
 *
 * <p>Consumed by: dqe-plugin (request entry point).
 */
public class AdmissionController {

  private final Semaphore semaphore;
  private final int maxConcurrentQueries;

  /**
   * Create an admission controller.
   *
   * @param maxConcurrentQueries maximum concurrent DQE queries (must be &gt; 0)
   */
  public AdmissionController(int maxConcurrentQueries) {
    if (maxConcurrentQueries <= 0) {
      throw new IllegalArgumentException(
          "maxConcurrentQueries must be > 0, got: " + maxConcurrentQueries);
    }
    this.maxConcurrentQueries = maxConcurrentQueries;
    this.semaphore = new Semaphore(maxConcurrentQueries);
  }

  /**
   * Attempt to acquire a slot for a new query.
   *
   * <p>This is non-blocking. If no slot is available, returns {@code false} immediately without
   * queueing.
   *
   * @return {@code true} if a slot was acquired, {@code false} if at capacity
   */
  public boolean tryAcquire() {
    return semaphore.tryAcquire();
  }

  /**
   * Release a slot when a query completes (success, failure, or cancel).
   *
   * <p>Callers must ensure that {@link #release()} is called exactly once for each successful
   * {@link #tryAcquire()} call.
   */
  public void release() {
    semaphore.release();
  }

  /**
   * Number of currently running queries.
   *
   * @return running query count
   */
  public int getRunningQueryCount() {
    return maxConcurrentQueries - semaphore.availablePermits();
  }

  /**
   * Maximum concurrent queries allowed.
   *
   * @return the configured maximum
   */
  public int getMaxConcurrentQueries() {
    return maxConcurrentQueries;
  }

  /**
   * Whether the controller is at capacity (no more slots available).
   *
   * @return {@code true} if at capacity
   */
  public boolean isAtCapacity() {
    return semaphore.availablePermits() == 0;
  }
}
