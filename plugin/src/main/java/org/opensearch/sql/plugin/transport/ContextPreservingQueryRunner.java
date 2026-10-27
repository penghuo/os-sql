/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.sql.job.QueryResult;
import org.opensearch.sql.job.QueryRunner;
import org.opensearch.sql.opensearch.executor.OpenSearchQueryManager;
import org.opensearch.tasks.CancellableTask;

/**
 * Runs a {@link QueryRunner} in the context of the request that submitted it.
 *
 * <p>An async job that waits in the queue starts later on whichever thread frees a slot. Each run
 * restores what the submitting thread held: the OpenSearch thread context (security user, headers,
 * trace span), the logging context (request id), and the PPL task that cancellation reaches. Each
 * is put back afterwards, so the starting thread is left as it was.
 */
final class ContextPreservingQueryRunner implements QueryRunner {

  private final QueryRunner delegate;
  private final Supplier<ThreadContext.StoredContext> submitterContext;
  private final Map<String, String> submitterLogContext;
  private final PPLQueryTask task;

  /**
   * Captures the current thread's context; call on the submitting thread.
   *
   * @param delegate runner to execute
   * @param threadContext the node's thread context
   * @param task the PPL task cancellation reaches and the engine reports execution start to
   */
  ContextPreservingQueryRunner(
      QueryRunner delegate, ThreadContext threadContext, PPLQueryTask task) {
    this(
        delegate,
        threadContext.newRestorableContext(false),
        org.apache.logging.log4j.ThreadContext.getImmutableContext(),
        task);
  }

  private ContextPreservingQueryRunner(
      QueryRunner delegate,
      Supplier<ThreadContext.StoredContext> submitterContext,
      Map<String, String> submitterLogContext,
      PPLQueryTask task) {
    this.delegate = Objects.requireNonNull(delegate, "delegate must not be null");
    this.submitterContext = submitterContext;
    this.submitterLogContext = submitterLogContext;
    this.task = Objects.requireNonNull(task, "task must not be null");
  }

  /** Runs the query and reports when the engine begins executing it, through the PPL task. */
  @Override
  public CompletionStage<QueryResult> run(Runnable onStarted) {
    task.setExecutionStartListener(onStarted);
    return run();
  }

  @Override
  public CompletionStage<QueryResult> run() {
    Map<String, String> callerLogContext =
        org.apache.logging.log4j.ThreadContext.getImmutableContext();
    CancellableTask callerTask = OpenSearchQueryManager.getCancellableTask();
    try (ThreadContext.StoredContext ignored = submitterContext.get()) {
      org.apache.logging.log4j.ThreadContext.clearMap();
      org.apache.logging.log4j.ThreadContext.putAll(submitterLogContext);
      OpenSearchQueryManager.setCancellableTask(task);
      return delegate.run();
    } finally {
      if (callerTask == null) {
        OpenSearchQueryManager.clearCancellableTask();
      } else {
        OpenSearchQueryManager.setCancellableTask(callerTask);
      }
      org.apache.logging.log4j.ThreadContext.clearMap();
      org.apache.logging.log4j.ThreadContext.putAll(callerLogContext);
    }
  }

  @Override
  public void cancel() {
    delegate.cancel();
  }

  @Override
  public QueryRunner newAttempt() {
    QueryRunner next = delegate.newAttempt();
    return next == null
        ? null
        : new ContextPreservingQueryRunner(next, submitterContext, submitterLogContext, task);
  }
}
