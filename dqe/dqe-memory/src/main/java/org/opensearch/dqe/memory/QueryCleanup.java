/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.memory;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Handles resource cleanup when a query completes (success, failure, or cancellation).
 *
 * <p>Registered cleanup actions run in reverse registration order (LIFO). All actions run even if
 * some throw exceptions — failures are logged but do not prevent subsequent cleanups.
 *
 * <p>After {@link #cleanup()} executes, the query's {@link QueryMemoryBudget#releaseAll()} is
 * called to return all memory to the node-level tracker.
 *
 * <p>Cleanup is idempotent — calling {@link #cleanup()} multiple times has no additional effect.
 */
public class QueryCleanup {

  private static final Logger logger = LogManager.getLogger(QueryCleanup.class);

  private final QueryMemoryBudget budget;
  private final List<Runnable> cleanupActions;
  private final AtomicBoolean cleaned;

  /**
   * Create a cleanup handler for a query.
   *
   * @param budget the per-query memory budget to release on cleanup
   */
  public QueryCleanup(QueryMemoryBudget budget) {
    this.budget = Objects.requireNonNull(budget, "budget must not be null");
    this.cleanupActions = new ArrayList<>();
    this.cleaned = new AtomicBoolean(false);
  }

  /**
   * Register a cleanup action to run when {@link #cleanup()} is called.
   *
   * <p>Actions are run in reverse registration order (LIFO). Must be called before {@link
   * #cleanup()}.
   *
   * @param action the cleanup action
   * @throws IllegalStateException if cleanup has already been performed
   */
  public void registerCleanupAction(Runnable action) {
    Objects.requireNonNull(action, "action must not be null");
    if (cleaned.get()) {
      throw new IllegalStateException(
          "Cannot register cleanup action after cleanup for query: " + budget.getQueryId());
    }
    synchronized (cleanupActions) {
      cleanupActions.add(action);
    }
  }

  /**
   * Execute all registered cleanup actions and release the query's memory budget.
   *
   * <p>Actions run in reverse registration order (LIFO). Each action's failure is logged but does
   * not prevent the remaining actions from running. After all actions, {@link
   * QueryMemoryBudget#releaseAll()} is called.
   *
   * <p>This method is idempotent.
   */
  public void cleanup() {
    if (!cleaned.compareAndSet(false, true)) {
      return;
    }

    String queryId = budget.getQueryId();
    long memoryBefore = budget.getUsedBytes();

    // Run actions in reverse order
    List<Runnable> actions;
    synchronized (cleanupActions) {
      actions = new ArrayList<>(cleanupActions);
    }

    for (int i = actions.size() - 1; i >= 0; i--) {
      try {
        actions.get(i).run();
      } catch (Exception e) {
        logger.warn("Cleanup action failed for query [{}]: {}", queryId, e.getMessage(), e);
      }
    }

    // Release all remaining memory
    budget.releaseAll();

    logger.debug("Query [{}] cleanup complete. Memory released: {} bytes", queryId, memoryBefore);
  }

  /**
   * Whether cleanup has already been performed.
   *
   * @return {@code true} if {@link #cleanup()} has been called
   */
  public boolean isCleaned() {
    return cleaned.get();
  }
}
