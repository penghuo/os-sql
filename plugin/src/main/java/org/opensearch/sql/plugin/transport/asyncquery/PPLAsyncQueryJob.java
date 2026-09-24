/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport.asyncquery;

import java.util.Objects;
import org.opensearch.ResourceNotFoundException;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.sql.executor.AsyncQueryExecution;
import org.opensearch.tasks.CancellableTask;

/**
 * Mutable state machine for one asynchronous PPL query.
 *
 * <p>Every lifecycle and lease transition is synchronized on this object. Methods mutate only
 * job-owned state and return immutable transition values; they never call listeners, mutate the
 * service map, update capacity counters, or cancel tasks while holding the lock.
 *
 * <p>After attachment, the job owns one {@link AsyncQueryExecution}. Reads borrow it through a
 * {@link ResponseContext}; removal transitions detach it so the service can close it outside the
 * lock.
 *
 * <pre>
 * Every job starts in RUNNING. It becomes retained when wait_for_completion_timeout expires.
 *
 * Current state       Event              Next state             Response
 * RUNNING             success            REMOVED                final result without ID
 * RUNNING             failure            REMOVED                failure without ID
 * RUNNING             retain             RETAINED_RUNNING       running status with ID
 * RETAINED_RUNNING    success            RETAINED_SUCCEEDED     none
 * RETAINED_RUNNING    failure            RETAINED_FAILED        none
 * RUNNING             abort/close        REMOVED                none
 * RETAINED_*          delete/expire/abort/close REMOVED         none
 * </pre>
 *
 * <p>GET lease renewal and execution attachment do not change the lifecycle state. Events received
 * after {@code REMOVED} are ignored or reported as not found; a late execution handle is rejected
 * so the service can close it.
 */
final class PPLAsyncQueryJob {
  private final String id;
  private final PPLAsyncQueryUser owner;
  private final long startTimeMillis;
  private JobTask task;

  private long keepAliveMillis;
  private long expirationTimeMillis;
  private State state;
  private AsyncQueryExecution execution;
  private PPLAsyncQueryService.Failure failure;
  private long completionTimeMillis = -1L;

  PPLAsyncQueryJob(
      String id,
      PPLAsyncQueryUser owner,
      long startTimeMillis,
      long keepAliveMillis,
      JobTask task) {
    this.id = id;
    this.owner = owner;
    this.startTimeMillis = startTimeMillis;
    this.keepAliveMillis = keepAliveMillis;
    this.expirationTimeMillis = addWithoutOverflow(startTimeMillis, keepAliveMillis);
    this.task = task;
    this.state = State.RUNNING;
  }

  String id() {
    return id;
  }

  synchronized Transition retain(long now) {
    if (state != State.RUNNING) {
      return null;
    }
    state = State.RETAINED_RUNNING;
    expirationTimeMillis = addWithoutOverflow(now, keepAliveMillis);
    return Transition.retain(retainedResponse());
  }

  synchronized boolean tryAttachExecution(AsyncQueryExecution execution) {
    Objects.requireNonNull(execution);
    if (!isExecuting() || this.execution != null) {
      return false;
    }
    this.execution = execution;
    return true;
  }

  synchronized Transition complete(long now) {
    if (!isExecuting()) {
      return null;
    }
    if (execution == null) {
      throw new IllegalStateException(
          "PPL asynchronous execution must be attached before successful completion");
    }
    return finish(State.RETAINED_SUCCEEDED, now);
  }

  synchronized Transition fail(PPLAsyncQueryService.Failure failure, long now) {
    if (!isExecuting()) {
      return null;
    }
    this.failure = failure;
    return finish(State.RETAINED_FAILED, now);
  }

  private Transition finish(State terminalState, long now) {
    boolean retained = state == State.RETAINED_RUNNING;
    JobTask taskToClose = detachTask();
    state = terminalState;
    completionTimeMillis = now;
    if (!retained) {
      ResponseContext response = directResponse();
      AsyncQueryExecution executionToClose = detachExecution();
      state = State.REMOVED;
      return Transition.returnDirect(response, executionToClose, taskToClose);
    }
    if (state == State.RETAINED_FAILED) {
      return Transition.finishRetained(detachExecution(), taskToClose);
    }
    return Transition.finishRetained(null, taskToClose);
  }

  synchronized GetResult get(PPLAsyncQueryUser caller, long now, TimeValue requestedKeepAlive) {
    ensurePresent();
    owner.authorize(caller);
    if (now >= expirationTimeMillis) {
      return new GetResult.Expired(expireLocked("PPL asynchronous query expired"));
    }
    if (requestedKeepAlive != null) {
      keepAliveMillis = requestedKeepAlive.millis();
      expirationTimeMillis = addWithoutOverflow(now, keepAliveMillis);
    }
    return new GetResult.Found(retainedResponse());
  }

  synchronized Removal delete(PPLAsyncQueryUser caller, long now) {
    ensurePresent();
    owner.authorize(caller);
    if (now >= expirationTimeMillis) {
      return expireLocked("PPL asynchronous query expired");
    }
    boolean wasRunning = isExecuting();
    PPLAsyncQueryService.Status responseStatus =
        wasRunning ? PPLAsyncQueryService.Status.CANCELLED : responseStatus();
    JobTask taskToCancel = wasRunning ? detachTask() : null;
    AsyncQueryExecution executionToClose = detachExecution();
    state = State.REMOVED;
    return new Removal(
        responseStatus,
        taskToCancel,
        executionToClose,
        "PPL asynchronous query cancelled by user",
        false,
        wasRunning);
  }

  synchronized Removal expire(long now) {
    if (state == State.REMOVED || state == State.RUNNING || now < expirationTimeMillis) {
      return null;
    }
    return expireLocked("PPL asynchronous query expired");
  }

  private Removal expireLocked(String reason) {
    boolean wasRunning = isExecuting();
    PPLAsyncQueryService.Status responseStatus = responseStatus();
    JobTask taskToCancel = wasRunning ? detachTask() : null;
    AsyncQueryExecution executionToClose = detachExecution();
    state = State.REMOVED;
    return new Removal(responseStatus, taskToCancel, executionToClose, reason, true, wasRunning);
  }

  synchronized Removal abort() {
    if (state == State.REMOVED) {
      return null;
    }
    boolean wasRunning = isExecuting();
    PPLAsyncQueryService.Status responseStatus = responseStatus();
    JobTask taskToCancel = wasRunning ? detachTask() : null;
    AsyncQueryExecution executionToClose = detachExecution();
    state = State.REMOVED;
    return new Removal(
        responseStatus,
        taskToCancel,
        executionToClose,
        "PPL asynchronous query startup failed",
        false,
        wasRunning);
  }

  synchronized Removal close(String reason) {
    if (state == State.REMOVED) {
      return null;
    }
    boolean wasRunning = isExecuting();
    PPLAsyncQueryService.Status responseStatus = responseStatus();
    JobTask taskToCancel = wasRunning ? detachTask() : null;
    AsyncQueryExecution executionToClose = detachExecution();
    state = State.REMOVED;
    return new Removal(responseStatus, taskToCancel, executionToClose, reason, false, wasRunning);
  }

  private ResponseContext directResponse() {
    return responseContext(null);
  }

  private ResponseContext retainedResponse() {
    return responseContext(id);
  }

  private ResponseContext responseContext(String responseId) {
    long tookMillis =
        completionTimeMillis < 0 ? -1L : Math.max(0L, completionTimeMillis - startTimeMillis);
    return new ResponseContext(responseId, responseStatus(), execution, failure, tookMillis);
  }

  private boolean isExecuting() {
    return state == State.RUNNING || state == State.RETAINED_RUNNING;
  }

  private PPLAsyncQueryService.Status responseStatus() {
    return switch (state) {
      case RUNNING, RETAINED_RUNNING -> PPLAsyncQueryService.Status.RUNNING;
      case RETAINED_SUCCEEDED -> PPLAsyncQueryService.Status.SUCCEEDED;
      case RETAINED_FAILED -> PPLAsyncQueryService.Status.FAILED;
      case REMOVED -> throw new IllegalStateException("PPL asynchronous query was removed");
    };
  }

  private AsyncQueryExecution detachExecution() {
    AsyncQueryExecution detached = execution;
    execution = null;
    return detached;
  }

  private JobTask detachTask() {
    JobTask detached = task;
    task = null;
    return detached;
  }

  private void ensurePresent() {
    if (state == State.REMOVED) {
      throw new ResourceNotFoundException("PPL asynchronous query not found");
    }
  }

  private static long addWithoutOverflow(long left, long right) {
    try {
      return Math.addExact(left, right);
    } catch (ArithmeticException e) {
      return Long.MAX_VALUE;
    }
  }

  record ResponseContext(
      String id,
      PPLAsyncQueryService.Status status,
      AsyncQueryExecution execution,
      PPLAsyncQueryService.Failure failure,
      long tookMillis) {}

  record JobTask(CancellableTask task, Runnable release) {
    void close() {
      release.run();
    }
  }

  /** Internal lifecycle; unlike the response status, this includes retention and removal. */
  enum State {
    RUNNING,
    RETAINED_RUNNING,
    RETAINED_SUCCEEDED,
    RETAINED_FAILED,
    REMOVED
  }

  enum Retention {
    RETAIN,
    REMOVE
  }

  enum RunningSlotAction {
    KEEP,
    RELEASE
  }

  record Transition(
      ResponseContext response,
      Retention retention,
      RunningSlotAction runningSlotAction,
      AsyncQueryExecution executionToClose,
      JobTask taskToClose) {

    private static Transition retain(ResponseContext response) {
      return new Transition(response, Retention.RETAIN, RunningSlotAction.KEEP, null, null);
    }

    private static Transition returnDirect(
        ResponseContext response, AsyncQueryExecution executionToClose, JobTask taskToClose) {
      return new Transition(
          response, Retention.REMOVE, RunningSlotAction.RELEASE, executionToClose, taskToClose);
    }

    private static Transition finishRetained(
        AsyncQueryExecution executionToClose, JobTask taskToClose) {
      return new Transition(
          null, Retention.RETAIN, RunningSlotAction.RELEASE, executionToClose, taskToClose);
    }
  }

  sealed interface GetResult {
    record Found(ResponseContext response) implements GetResult {}

    record Expired(Removal removal) implements GetResult {}
  }

  record Removal(
      PPLAsyncQueryService.Status responseStatus,
      JobTask task,
      AsyncQueryExecution execution,
      String reason,
      boolean expired,
      boolean releaseRunningSlot) {}
}
