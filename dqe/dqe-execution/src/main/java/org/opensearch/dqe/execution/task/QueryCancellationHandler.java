/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.execution.task;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import org.opensearch.dqe.execution.operator.OperatorContext;

/**
 * Manages cancellation propagation for DQE queries. When a query is cancelled, sets the interrupt
 * flag on all registered operator contexts.
 */
public class QueryCancellationHandler {

  private final Map<String, List<OperatorContext>> queryContexts = new ConcurrentHashMap<>();
  private final Map<String, AtomicBoolean> cancelledQueries = new ConcurrentHashMap<>();

  public QueryCancellationHandler() {}

  /** Registers operator contexts for a query so cancellation can propagate to them. */
  public void registerOperatorContexts(String queryId, List<OperatorContext> contexts) {
    queryContexts.computeIfAbsent(queryId, k -> new CopyOnWriteArrayList<>()).addAll(contexts);
    // If the query was already cancelled before these contexts were registered, interrupt now
    if (isCancelled(queryId)) {
      for (OperatorContext ctx : contexts) {
        ctx.setInterrupted(true);
      }
    }
  }

  /** Cancels a query: sets interrupt flag on all registered operator contexts. */
  public void cancelQuery(String queryId, String reason) {
    cancelledQueries.computeIfAbsent(queryId, k -> new AtomicBoolean(false)).set(true);
    List<OperatorContext> contexts = queryContexts.get(queryId);
    if (contexts != null) {
      for (OperatorContext ctx : contexts) {
        ctx.setInterrupted(true);
      }
    }
  }

  /** Removes tracking for a completed query. */
  public void deregisterQuery(String queryId) {
    queryContexts.remove(queryId);
    cancelledQueries.remove(queryId);
  }

  /** Returns true if the query has been cancelled. */
  public boolean isCancelled(String queryId) {
    AtomicBoolean flag = cancelledQueries.get(queryId);
    return flag != null && flag.get();
  }
}
