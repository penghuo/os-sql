/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.memory;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.opensearch.core.common.breaker.CircuitBreakingException;
import org.opensearch.dqe.parser.DqeErrorCode;
import org.opensearch.dqe.parser.DqeException;

/**
 * Per-query memory budget that sub-allocates from the node-level {@link DqeMemoryTracker}.
 *
 * <p>Each query gets a budget (default 256MB, configurable via session properties). Every operator
 * allocates memory through this budget. When the budget is exceeded, the query fails with {@link
 * DqeErrorCode#EXCEEDED_QUERY_MEMORY_LIMIT}.
 *
 * <p>The budget delegates to the {@link DqeMemoryTracker} which in turn checks the DQE circuit
 * breaker and parent breaker. This means a query can fail either because:
 *
 * <ul>
 *   <li>Its per-query budget is exceeded
 *   <li>The node-level DQE breaker is exceeded
 *   <li>The parent (heap) breaker is exceeded
 * </ul>
 */
public class QueryMemoryBudget {

  private final String queryId;
  private final long budgetBytes;
  private final DqeMemoryTracker memoryTracker;
  private final AtomicLong usedBytes;
  private final AtomicBoolean released;

  /**
   * Create a per-query memory budget.
   *
   * @param queryId unique query identifier
   * @param budgetBytes maximum bytes this query may use (must be &gt; 0)
   * @param memoryTracker the node-level memory tracker
   */
  public QueryMemoryBudget(String queryId, long budgetBytes, DqeMemoryTracker memoryTracker) {
    this.queryId = Objects.requireNonNull(queryId, "queryId must not be null");
    if (budgetBytes <= 0) {
      throw new IllegalArgumentException("budgetBytes must be > 0, got: " + budgetBytes);
    }
    this.budgetBytes = budgetBytes;
    this.memoryTracker = Objects.requireNonNull(memoryTracker, "memoryTracker must not be null");
    this.usedBytes = new AtomicLong(0);
    this.released = new AtomicBoolean(false);
  }

  /**
   * Reserve bytes for this query.
   *
   * <p>First checks the per-query budget, then delegates to the node-level tracker.
   *
   * @param bytes number of bytes to reserve (must be &gt; 0)
   * @param label descriptive label for debugging
   * @throws DqeException with {@link DqeErrorCode#EXCEEDED_QUERY_MEMORY_LIMIT} if budget exceeded
   * @throws CircuitBreakingException if node-level breaker trips
   */
  public void reserve(long bytes, String label) throws DqeException {
    if (bytes <= 0) {
      throw new IllegalArgumentException("reserve bytes must be > 0, got: " + bytes);
    }
    Objects.requireNonNull(label, "label must not be null");

    String queryLabel = queryId + ":" + label;

    // Atomically check and update per-query budget using CAS loop
    long current;
    do {
      current = usedBytes.get();
      if (current + bytes > budgetBytes) {
        throw new DqeException(
            String.format(
                "Query [%s] exceeded memory budget: attempted to reserve %d bytes,"
                    + " current usage %d bytes, budget %d bytes",
                queryId, bytes, current, budgetBytes),
            DqeErrorCode.EXCEEDED_QUERY_MEMORY_LIMIT);
      }
    } while (!usedBytes.compareAndSet(current, current + bytes));

    // Delegate to node-level tracker (checks DQE + parent breakers).
    // If the tracker throws, roll back the local reservation.
    try {
      memoryTracker.reserve(bytes, queryLabel);
    } catch (RuntimeException e) {
      usedBytes.addAndGet(-bytes);
      throw e;
    }
  }

  /**
   * Release previously reserved bytes.
   *
   * @param bytes number of bytes to release (must be &gt; 0)
   * @param label descriptive label for tracking
   */
  public void release(long bytes, String label) {
    if (bytes <= 0) {
      throw new IllegalArgumentException("release bytes must be > 0, got: " + bytes);
    }
    Objects.requireNonNull(label, "label must not be null");

    String queryLabel = queryId + ":" + label;
    memoryTracker.release(bytes, queryLabel);
    usedBytes.addAndGet(-bytes);
  }

  /**
   * Release all memory held by this query. Idempotent — safe to call multiple times.
   *
   * <p>Called during query cleanup (success, failure, or cancellation).
   */
  public void releaseAll() {
    if (released.compareAndSet(false, true)) {
      long current = usedBytes.getAndSet(0);
      if (current > 0) {
        memoryTracker.release(current, queryId + ":releaseAll");
      }
    }
  }

  /** Current bytes used by this query. */
  public long getUsedBytes() {
    return usedBytes.get();
  }

  /** Maximum bytes allowed for this query. */
  public long getBudgetBytes() {
    return budgetBytes;
  }

  /** Remaining bytes before the per-query budget is exceeded. */
  public long getRemainingBytes() {
    return budgetBytes - usedBytes.get();
  }

  /** The query identifier. */
  public String getQueryId() {
    return queryId;
  }
}
