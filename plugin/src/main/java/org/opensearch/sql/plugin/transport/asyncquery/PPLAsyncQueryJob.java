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
 * {@link View}; removal transitions detach it so the service can close it outside the lock.
 */
final class PPLAsyncQueryJob {
  private final String id;
  private final PPLAsyncQueryUser owner;
  private final long startTimeMillis;
  private JobTask task;

  private long keepAliveMillis;
  private long expirationTimeMillis;
  private PPLAsyncQueryService.Status status = PPLAsyncQueryService.Status.RUNNING;
  private AsyncQueryExecution execution;
  private PPLAsyncQueryService.Failure failure;
  private long completionTimeMillis = -1L;
  private PPLAsyncQueryService.SubmitWaiter submitWaiter;
  private boolean removed;

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
  }

  String id() {
    return id;
  }

  synchronized SubmitRegistration registerSubmitWaiter(
      PPLAsyncQueryService.SubmitWaiter waiter, boolean returnImmediately) {
    ensurePresent();
    if (submitWaiter != null) {
      throw new IllegalStateException("PPL asynchronous submit waiter is already registered");
    }
    if (status != PPLAsyncQueryService.Status.RUNNING) {
      removed = true;
      View view = view(false);
      AsyncQueryExecution executionToClose = detachExecution();
      return SubmitRegistration.respondImmediately(
          Transition.respond(
              view, waiter, Retention.REMOVE, RunningSlotAction.KEEP, executionToClose, null));
    }
    if (returnImmediately) {
      return SubmitRegistration.respondImmediately(
          Transition.respond(
              view(true), waiter, Retention.RETAIN, RunningSlotAction.KEEP, null, null));
    }
    submitWaiter = waiter;
    return SubmitRegistration.waiting();
  }

  synchronized Transition timeout(PPLAsyncQueryService.SubmitWaiter waiter, long now) {
    if (removed || submitWaiter != waiter) {
      return null;
    }
    submitWaiter = null;
    if (status == PPLAsyncQueryService.Status.RUNNING) {
      expirationTimeMillis = addWithoutOverflow(now, keepAliveMillis);
      return Transition.respond(
          view(true), waiter, Retention.RETAIN, RunningSlotAction.KEEP, null, null);
    }
    removed = true;
    View view = view(false);
    AsyncQueryExecution executionToClose = detachExecution();
    return Transition.respond(
        view, waiter, Retention.REMOVE, RunningSlotAction.KEEP, executionToClose, null);
  }

  synchronized boolean tryAttachExecution(AsyncQueryExecution execution) {
    Objects.requireNonNull(execution);
    if (removed || status != PPLAsyncQueryService.Status.RUNNING || this.execution != null) {
      return false;
    }
    this.execution = execution;
    return true;
  }

  synchronized Transition complete(long now) {
    if (removed || status != PPLAsyncQueryService.Status.RUNNING) {
      return null;
    }
    if (execution == null) {
      throw new IllegalStateException(
          "PPL asynchronous execution must be attached before successful completion");
    }
    JobTask taskToClose = detachTask();
    status = PPLAsyncQueryService.Status.SUCCEEDED;
    completionTimeMillis = now;
    return terminalTransition(taskToClose);
  }

  synchronized Transition fail(PPLAsyncQueryService.Failure failure, long now) {
    if (removed || status != PPLAsyncQueryService.Status.RUNNING) {
      return null;
    }
    this.failure = failure;
    JobTask taskToClose = detachTask();
    status = PPLAsyncQueryService.Status.FAILED;
    completionTimeMillis = now;
    return terminalTransition(taskToClose);
  }

  private Transition terminalTransition(JobTask taskToClose) {
    PPLAsyncQueryService.SubmitWaiter waiter = submitWaiter;
    submitWaiter = null;
    if (waiter != null) {
      removed = true;
      View view = view(false);
      AsyncQueryExecution executionToClose = detachExecution();
      return Transition.respond(
          view, waiter, Retention.REMOVE, RunningSlotAction.RELEASE, executionToClose, taskToClose);
    }
    if (status == PPLAsyncQueryService.Status.FAILED) {
      return Transition.withoutSubmitResponse(
          Retention.RETAIN, RunningSlotAction.RELEASE, detachExecution(), taskToClose);
    }
    return Transition.withoutSubmitResponse(
        Retention.RETAIN, RunningSlotAction.RELEASE, null, taskToClose);
  }

  synchronized Access get(PPLAsyncQueryUser caller, long now, TimeValue requestedKeepAlive) {
    ensurePresent();
    owner.authorize(caller);
    if (now >= expirationTimeMillis) {
      return Access.removed(expireLocked("PPL asynchronous query expired"));
    }
    if (requestedKeepAlive != null) {
      keepAliveMillis = requestedKeepAlive.millis();
    }
    expirationTimeMillis = addWithoutOverflow(now, keepAliveMillis);
    return Access.view(view(true));
  }

  synchronized Removal delete(PPLAsyncQueryUser caller, long now) {
    ensurePresent();
    owner.authorize(caller);
    if (now >= expirationTimeMillis) {
      return expireLocked("PPL asynchronous query expired");
    }
    boolean wasRunning = status == PPLAsyncQueryService.Status.RUNNING;
    PPLAsyncQueryService.Status responseStatus =
        wasRunning ? PPLAsyncQueryService.Status.CANCELLED : status;
    JobTask taskToCancel = wasRunning ? detachTask() : null;
    AsyncQueryExecution executionToClose = detachExecution();
    removed = true;
    submitWaiter = null;
    return new Removal(
        responseStatus,
        taskToCancel,
        executionToClose,
        "PPL asynchronous query cancelled by user",
        false,
        wasRunning);
  }

  synchronized Removal expire(long now) {
    if (removed || submitWaiter != null || now < expirationTimeMillis) {
      return null;
    }
    return expireLocked("PPL asynchronous query expired");
  }

  private Removal expireLocked(String reason) {
    boolean wasRunning = status == PPLAsyncQueryService.Status.RUNNING;
    JobTask taskToCancel = wasRunning ? detachTask() : null;
    AsyncQueryExecution executionToClose = detachExecution();
    removed = true;
    submitWaiter = null;
    return new Removal(status, taskToCancel, executionToClose, reason, true, wasRunning);
  }

  synchronized Removal abort() {
    if (removed) {
      return null;
    }
    boolean wasRunning = status == PPLAsyncQueryService.Status.RUNNING;
    JobTask taskToCancel = wasRunning ? detachTask() : null;
    AsyncQueryExecution executionToClose = detachExecution();
    removed = true;
    submitWaiter = null;
    return new Removal(
        status,
        taskToCancel,
        executionToClose,
        "PPL asynchronous query submission failed",
        false,
        wasRunning);
  }

  synchronized Removal close(String reason) {
    if (removed) {
      return null;
    }
    boolean wasRunning = status == PPLAsyncQueryService.Status.RUNNING;
    JobTask taskToCancel = wasRunning ? detachTask() : null;
    AsyncQueryExecution executionToClose = detachExecution();
    removed = true;
    submitWaiter = null;
    return new Removal(status, taskToCancel, executionToClose, reason, false, wasRunning);
  }

  private View view(boolean includeId) {
    long tookMillis =
        completionTimeMillis < 0 ? -1L : Math.max(0L, completionTimeMillis - startTimeMillis);
    return new View(includeId ? id : null, status, execution, failure, tookMillis);
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
    if (removed) {
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

  record View(
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

  record SubmitRegistration(Transition immediateTransition) {
    private static SubmitRegistration waiting() {
      return new SubmitRegistration(null);
    }

    private static SubmitRegistration respondImmediately(Transition transition) {
      return new SubmitRegistration(transition);
    }
  }

  enum Retention {
    RETAIN,
    REMOVE
  }

  enum RunningSlotAction {
    KEEP,
    RELEASE
  }

  record SubmitResponse(View view, PPLAsyncQueryService.SubmitWaiter waiter) {}

  record Transition(
      SubmitResponse submitResponse,
      Retention retention,
      RunningSlotAction runningSlotAction,
      AsyncQueryExecution executionToClose,
      JobTask taskToClose) {

    private static Transition respond(
        View view,
        PPLAsyncQueryService.SubmitWaiter waiter,
        Retention retention,
        RunningSlotAction runningSlotAction,
        AsyncQueryExecution executionToClose,
        JobTask taskToClose) {
      return new Transition(
          new SubmitResponse(view, waiter),
          retention,
          runningSlotAction,
          executionToClose,
          taskToClose);
    }

    private static Transition withoutSubmitResponse(
        Retention retention,
        RunningSlotAction runningSlotAction,
        AsyncQueryExecution executionToClose,
        JobTask taskToClose) {
      return new Transition(null, retention, runningSlotAction, executionToClose, taskToClose);
    }
  }

  record Access(View view, Removal removal) {
    private static Access view(View view) {
      return new Access(view, null);
    }

    private static Access removed(Removal removal) {
      return new Access(null, removal);
    }
  }

  record Removal(
      PPLAsyncQueryService.Status responseStatus,
      JobTask task,
      AsyncQueryExecution execution,
      String reason,
      boolean expired,
      boolean releaseRunningSlot) {}
}
