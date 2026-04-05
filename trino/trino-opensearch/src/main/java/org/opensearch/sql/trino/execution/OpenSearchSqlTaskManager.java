/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.trino.execution;

import io.trino.Session;
import io.trino.execution.SplitAssignment;
import io.trino.execution.SqlTaskManager;
import io.trino.execution.SqlTaskManager.SqlTaskWithResults;
import io.trino.execution.TaskId;
import io.trino.execution.TaskInfo;
import io.trino.execution.TaskState;
import io.trino.execution.StateMachine.StateChangeListener;
import io.trino.execution.buffer.BufferResult;
import io.trino.execution.buffer.OutputBuffers;
import io.trino.execution.buffer.PipelinedOutputBuffers.OutputBufferId;
import io.trino.sql.planner.PlanFragment;
import io.trino.sql.planner.plan.DynamicFilterId;
import io.trino.spi.predicate.Domain;
import io.opentelemetry.api.trace.Span;
import java.io.Closeable;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Worker-side task manager. Receives plan fragments via transport actions, creates local Trino
 * tasks, executes them, and serves output pages.
 *
 * <p>Wraps Trino's {@link SqlTaskManager} — does NOT reimplement task lifecycle. The delegate is
 * injected after Trino engine bootstrap completes.
 *
 * <p>Also tracks diagnostic counters for distribution proof (Task 18 tests).
 */
public class OpenSearchSqlTaskManager implements Closeable {

  private static final Logger LOG = LogManager.getLogger(OpenSearchSqlTaskManager.class);

  private volatile SqlTaskManager delegate;

  // Diagnostic counters for proving distributed execution
  private final AtomicLong tasksReceived = new AtomicLong();
  private final AtomicLong splitsProcessed = new AtomicLong();
  private final AtomicLong pagesProduced = new AtomicLong();
  private final AtomicLong resultsFetched = new AtomicLong();
  private final AtomicLong taskUpdatesReceived = new AtomicLong();

  /** Creates an uninitialized manager. Call {@link #initialize(SqlTaskManager)} before use. */
  public OpenSearchSqlTaskManager() {}

  /**
   * Creates a manager wrapping the given delegate.
   *
   * @param delegate the Trino SqlTaskManager to delegate to
   */
  public OpenSearchSqlTaskManager(SqlTaskManager delegate) {
    this.delegate = delegate;
  }

  /**
   * Set the delegate after construction. Used when the Trino engine bootstraps asynchronously.
   *
   * @param delegate the Trino SqlTaskManager
   */
  public void initialize(SqlTaskManager delegate) {
    this.delegate = delegate;
    LOG.info("OpenSearchSqlTaskManager initialized with SqlTaskManager delegate");
  }

  private SqlTaskManager requireDelegate() {
    SqlTaskManager d = delegate;
    if (d == null) {
      throw new IllegalStateException(
          "OpenSearchSqlTaskManager not initialized — call initialize() first");
    }
    return d;
  }

  /**
   * Called by {@code TransportTrinoTaskUpdateAction} when a coordinator dispatches work. Creates or
   * updates a local task from the given plan fragment and splits.
   *
   * @param session the query session
   * @param taskId the task identifier
   * @param fragment the plan fragment (empty for split-only updates)
   * @param splits split assignments for the task
   * @param outputBuffers output buffer configuration
   * @param dynamicFilterDomains dynamic filter domains
   * @return current TaskInfo after the update
   */
  public TaskInfo updateTask(
      Session session,
      TaskId taskId,
      Optional<PlanFragment> fragment,
      List<SplitAssignment> splits,
      OutputBuffers outputBuffers,
      Map<DynamicFilterId, Domain> dynamicFilterDomains) {
    taskUpdatesReceived.incrementAndGet();
    if (fragment.isPresent()) {
      tasksReceived.incrementAndGet();
    }
    splitsProcessed.addAndGet(
        splits.stream().mapToLong(s -> s.getSplits().size()).sum());
    LOG.debug(
        "updateTask: taskId={}, hasFragment={}, splits={}, outputBuffers={}",
        taskId,
        fragment.isPresent(),
        splits.size(),
        outputBuffers);
    return requireDelegate()
        .updateTask(
            session,
            taskId,
            Span.getInvalid(),
            fragment,
            splits,
            outputBuffers,
            dynamicFilterDomains,
            false);
  }

  /**
   * Called by {@code TransportTrinoTaskResultsAction}. Returns pages from the given output buffer.
   *
   * @param taskId the task identifier
   * @param bufferId the output buffer identifier
   * @param token sequence number for exactly-once delivery
   * @param maxSize maximum bytes to return
   * @return the task with results future
   */
  public SqlTaskWithResults getTaskResults(
      TaskId taskId, OutputBufferId bufferId, long token,
      io.airlift.units.DataSize maxSize) {
    resultsFetched.incrementAndGet();
    return requireDelegate().getTaskResults(taskId, bufferId, token, maxSize);
  }

  /**
   * Called by {@code TransportTrinoTaskResultsAckAction}. Advances the ack token for flow control.
   */
  public void acknowledgeTaskResults(TaskId taskId, OutputBufferId bufferId, long token) {
    requireDelegate().acknowledgeTaskResults(taskId, bufferId, token);
  }

  /** Called by {@code TransportTrinoTaskStatusAction}. */
  public TaskInfo getTaskInfo(TaskId taskId) {
    return requireDelegate().getTaskInfo(taskId);
  }

  /** Called by {@code TransportTrinoTaskCancelAction}. */
  public TaskInfo cancelTask(TaskId taskId) {
    return requireDelegate().cancelTask(taskId);
  }

  /** Abort a task (forced cancellation). */
  public TaskInfo abortTask(TaskId taskId) {
    return requireDelegate().abortTask(taskId);
  }

  /** Fail a task with an error. */
  public TaskInfo failTask(TaskId taskId, Throwable failure) {
    return requireDelegate().failTask(taskId, failure);
  }

  /** Add a state change listener for a task. */
  public void addStateChangeListener(TaskId taskId, StateChangeListener<TaskState> listener) {
    requireDelegate().addStateChangeListener(taskId, listener);
  }

  /** Get all task info for diagnostics. */
  public List<TaskInfo> getAllTaskInfo() {
    return requireDelegate().getAllTaskInfo();
  }

  // --- Diagnostic counters ---

  public long getTasksReceived() {
    return tasksReceived.get();
  }

  public long getSplitsProcessed() {
    return splitsProcessed.get();
  }

  public long getPagesProduced() {
    return pagesProduced.get();
  }

  public long getResultsFetched() {
    return resultsFetched.get();
  }

  public long getTaskUpdatesReceived() {
    return taskUpdatesReceived.get();
  }

  /** Increment pages produced counter (called by output buffer wrapper). */
  public void recordPagesProduced(long count) {
    pagesProduced.addAndGet(count);
  }

  /** Reset all diagnostic counters. */
  public void resetStats() {
    tasksReceived.set(0);
    splitsProcessed.set(0);
    pagesProduced.set(0);
    resultsFetched.set(0);
    taskUpdatesReceived.set(0);
  }

  @Override
  public void close() {
    SqlTaskManager d = delegate;
    if (d != null) {
      try {
        d.close();
      } catch (Exception e) {
        LOG.warn("Error closing SqlTaskManager delegate", e);
      }
    }
  }
}
