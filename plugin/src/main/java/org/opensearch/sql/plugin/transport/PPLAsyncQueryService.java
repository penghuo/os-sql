/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.IntSupplier;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.ResourceNotFoundException;
import org.opensearch.common.lifecycle.AbstractLifecycleComponent;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.executor.ExecutionEngine.QueryResponse;
import org.opensearch.sql.executor.ExecutionEngine.Schema;
import org.opensearch.tasks.CancellableTask;
import org.opensearch.tasks.TaskManager;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;

/** Owner-node lifecycle and complete-result store for asynchronous PPL queries. */
public final class PPLAsyncQueryService extends AbstractLifecycleComponent {
  private static final Logger LOG = LogManager.getLogger(PPLAsyncQueryService.class);

  static final TimeValue DEFAULT_WAIT_FOR_COMPLETION = TimeValue.timeValueSeconds(5);
  static final TimeValue DEFAULT_KEEP_ALIVE = TimeValue.timeValueMinutes(5);
  private static final TimeValue REAPER_INTERVAL = TimeValue.timeValueMinutes(1);

  enum Status {
    RUNNING,
    SUCCEEDED,
    FAILED,
    CANCELLED
  }

  @FunctionalInterface
  interface TimeoutHandle {
    void cancel();
  }

  @FunctionalInterface
  interface TimeoutScheduler {
    TimeoutHandle schedule(TimeValue delay, Runnable task);
  }

  record Snapshot(
      String id, Status status, QueryResponse response, Failure failure, long tookMillis) {}

  record DeleteResult(String id, Status status) {}

  record Failure(String type, String reason) {
    private static Failure from(Exception exception) {
      String type =
          exception.getClass().getSimpleName().isBlank()
              ? exception.getClass().getName()
              : exception.getClass().getSimpleName();
      return new Failure(type, "query execution failed");
    }
  }

  private final Supplier<String> ownerNodeIdSupplier;
  private final LongSupplier currentTimeMillis;
  private final TimeoutScheduler timeoutScheduler;
  private final IntSupplier maxRunningQueries;
  private final IntSupplier maxRetainedJobs;
  private final Supplier<TimeValue> maxWaitForCompletion;
  private final Supplier<TimeValue> maxKeepAlive;
  private final ThreadPool threadPool;
  private final ConcurrentMap<String, Job> jobs = new ConcurrentHashMap<>();
  private final Object admissionLock = new Object();

  private int runningQueries;
  private int retainedJobs;
  private volatile boolean acceptingSubmissions = true;
  private volatile Scheduler.Cancellable reaper;
  private volatile TaskManager taskManager;

  public PPLAsyncQueryService(
      Supplier<String> ownerNodeIdSupplier, ThreadPool threadPool, Settings settings) {
    this(
        ownerNodeIdSupplier,
        System::currentTimeMillis,
        (delay, task) -> {
          Scheduler.ScheduledCancellable cancellable =
              threadPool.schedule(task, delay, ThreadPool.Names.GENERIC);
          return cancellable::cancel;
        },
        () ->
            (Integer)
                settings.getSettingValue(Settings.Key.PPL_ASYNC_NODE_CONCURRENT_RUNNING_QUERIES),
        () -> (Integer) settings.getSettingValue(Settings.Key.PPL_ASYNC_MAX_RETAINED_JOBS),
        () ->
            (TimeValue)
                settings.getSettingValue(Settings.Key.PPL_ASYNC_MAX_WAIT_FOR_COMPLETION_TIMEOUT),
        () -> (TimeValue) settings.getSettingValue(Settings.Key.PPL_ASYNC_MAX_KEEP_ALIVE),
        threadPool);
  }

  PPLAsyncQueryService(
      String ownerNodeId,
      LongSupplier currentTimeMillis,
      TimeoutScheduler timeoutScheduler,
      IntSupplier maxRunningQueries,
      IntSupplier maxRetainedJobs,
      Supplier<TimeValue> maxWaitForCompletion,
      Supplier<TimeValue> maxKeepAlive) {
    this(
        () -> ownerNodeId,
        currentTimeMillis,
        timeoutScheduler,
        maxRunningQueries,
        maxRetainedJobs,
        maxWaitForCompletion,
        maxKeepAlive,
        null);
  }

  private PPLAsyncQueryService(
      Supplier<String> ownerNodeIdSupplier,
      LongSupplier currentTimeMillis,
      TimeoutScheduler timeoutScheduler,
      IntSupplier maxRunningQueries,
      IntSupplier maxRetainedJobs,
      Supplier<TimeValue> maxWaitForCompletion,
      Supplier<TimeValue> maxKeepAlive,
      ThreadPool threadPool) {
    this.ownerNodeIdSupplier = Objects.requireNonNull(ownerNodeIdSupplier);
    this.currentTimeMillis = Objects.requireNonNull(currentTimeMillis);
    this.timeoutScheduler = Objects.requireNonNull(timeoutScheduler);
    this.maxRunningQueries = Objects.requireNonNull(maxRunningQueries);
    this.maxRetainedJobs = Objects.requireNonNull(maxRetainedJobs);
    this.maxWaitForCompletion = Objects.requireNonNull(maxWaitForCompletion);
    this.maxKeepAlive = Objects.requireNonNull(maxKeepAlive);
    this.threadPool = threadPool;
  }

  String create(PPLAsyncQueryUser owner, TimeValue keepAlive, CancellableTask task) {
    Objects.requireNonNull(owner);
    validateKeepAlive(keepAlive);
    reserveCapacity();
    boolean stored = false;
    try {
      String ownerNodeId =
          Objects.requireNonNull(ownerNodeIdSupplier.get(), "Local node ID is not initialized");
      long now = currentTimeMillis.getAsLong();
      while (true) {
        PPLAsyncQueryJobId jobId = PPLAsyncQueryJobId.create(ownerNodeId);
        Job job = new Job(jobId.encode(), owner, now, keepAlive.millis(), task);
        if (jobs.putIfAbsent(jobId.contextId(), job) == null) {
          stored = true;
          return job.id;
        }
      }
    } finally {
      if (!stored) {
        releaseRunning();
        releaseRetained();
      }
    }
  }

  boolean awaitSubmit(
      String id, TimeValue waitForCompletion, ActionListener<Snapshot> responseListener) {
    validateWaitForCompletion(waitForCompletion);
    LocatedJob located = findLocal(id);
    SubmitWaiter waiter = new SubmitWaiter(responseListener);
    Registration registration =
        located.job.registerSubmitWaiter(waiter, waitForCompletion.millis() == 0);
    if (registration.publication != null) {
      applyPublication(located, registration.publication);
      return true;
    }

    try {
      TimeoutHandle timeout =
          timeoutScheduler.schedule(
              waitForCompletion,
              () ->
                  applyPublication(
                      located, located.job.timeout(waiter, currentTimeMillis.getAsLong())));
      waiter.setTimeout(timeout);
      return true;
    } catch (Exception e) {
      Removal removal = located.job.abort();
      applyRemoval(located, removal);
      waiter.fail(e);
      return false;
    }
  }

  void complete(String id, QueryResponse response) {
    LocatedJob located = findInternal(id);
    if (located != null) {
      applyPublication(
          located, located.job.complete(copy(response), currentTimeMillis.getAsLong()));
    }
  }

  void fail(String id, Exception failure) {
    LocatedJob located = findInternal(id);
    if (located != null) {
      applyPublication(
          located,
          located.job.fail(
              Failure.from(Objects.requireNonNull(failure)), currentTimeMillis.getAsLong()));
    }
  }

  Snapshot get(String id, PPLAsyncQueryUser caller, TimeValue requestedKeepAlive) {
    if (requestedKeepAlive != null) {
      validateKeepAlive(requestedKeepAlive);
    }
    LocatedJob located = findLocal(id);
    Access access = located.job.get(caller, currentTimeMillis.getAsLong(), requestedKeepAlive);
    if (access.removal != null) {
      applyRemoval(located, access.removal);
      throw notFound();
    }
    return access.snapshot;
  }

  DeleteResult delete(String id, PPLAsyncQueryUser caller) {
    LocatedJob located = findLocal(id);
    Removal removal = located.job.delete(caller, currentTimeMillis.getAsLong());
    applyRemoval(located, removal);
    if (removal.expired) {
      throw notFound();
    }
    return new DeleteResult(id, removal.responseStatus);
  }

  void reapExpired() {
    long now = currentTimeMillis.getAsLong();
    jobs.forEach((contextId, job) -> applyRemoval(new LocatedJob(contextId, job), job.expire(now)));
  }

  int runningQueryCount() {
    synchronized (admissionLock) {
      return runningQueries;
    }
  }

  int retainedJobCount() {
    synchronized (admissionLock) {
      return retainedJobs;
    }
  }

  void attachTaskManager(TaskManager taskManager) {
    this.taskManager = Objects.requireNonNull(taskManager);
  }

  private void reserveCapacity() {
    synchronized (admissionLock) {
      if (!acceptingSubmissions) {
        throw new OpenSearchStatusException(
            "PPL asynchronous query service is stopping", RestStatus.SERVICE_UNAVAILABLE);
      }
      if (runningQueries >= maxRunningQueries.getAsInt()
          || retainedJobs >= maxRetainedJobs.getAsInt()) {
        throw new OpenSearchStatusException(
            "PPL asynchronous query capacity is exhausted", RestStatus.TOO_MANY_REQUESTS);
      }
      runningQueries++;
      retainedJobs++;
    }
  }

  private void releaseRunning() {
    synchronized (admissionLock) {
      if (runningQueries > 0) {
        runningQueries--;
      }
    }
  }

  private void releaseRetained() {
    synchronized (admissionLock) {
      if (retainedJobs > 0) {
        retainedJobs--;
      }
    }
  }

  private void applyPublication(LocatedJob located, Publication publication) {
    if (publication == null) {
      return;
    }
    if (publication.releaseRunning) {
      releaseRunning();
    }
    if (publication.remove && jobs.remove(located.contextId, located.job)) {
      releaseRetained();
    }
    if (publication.waiter != null) {
      publication.waiter.respond(publication.snapshot);
    }
  }

  private void applyRemoval(LocatedJob located, Removal removal) {
    if (removal == null) {
      return;
    }
    if (jobs.remove(located.contextId, located.job)) {
      if (removal.releaseRunning) {
        releaseRunning();
      }
      releaseRetained();
    }
    cancel(removal.task, removal.reason);
  }

  private LocatedJob findLocal(String encodedId) {
    PPLAsyncQueryJobId jobId = PPLAsyncQueryJobId.parse(encodedId);
    String localNodeId =
        Objects.requireNonNull(ownerNodeIdSupplier.get(), "Local node ID is not initialized");
    if (!localNodeId.equals(jobId.ownerNodeId())) {
      throw new IllegalArgumentException("PPL asynchronous query is not owned by this node");
    }
    Job job = jobs.get(jobId.contextId());
    if (job == null) {
      throw notFound();
    }
    return new LocatedJob(jobId.contextId(), job);
  }

  private LocatedJob findInternal(String encodedId) {
    try {
      return findLocal(encodedId);
    } catch (ResourceNotFoundException e) {
      return null;
    }
  }

  void validateKeepAlive(TimeValue keepAlive) {
    TimeValue maximum = maxKeepAlive.get();
    if (keepAlive == null || keepAlive.millis() <= 0 || keepAlive.millis() > maximum.millis()) {
      throw new IllegalArgumentException(
          "[keep_alive] must be greater than 0 and no more than " + maximum);
    }
  }

  void validateWaitForCompletion(TimeValue waitForCompletion) {
    TimeValue maximum = maxWaitForCompletion.get();
    if (waitForCompletion == null
        || waitForCompletion.millis() < 0
        || waitForCompletion.millis() > maximum.millis()) {
      throw new IllegalArgumentException(
          "[wait_for_completion_timeout] must be between 0 and " + maximum);
    }
  }

  private static QueryResponse copy(QueryResponse response) {
    Schema schema = new Schema(List.copyOf(response.getSchema().getColumns()));
    QueryResponse copy =
        new QueryResponse(schema, List.copyOf(response.getResults()), response.getCursor());
    copy.setWarnings(List.copyOf(response.getWarnings()));
    return copy;
  }

  private void cancel(CancellableTask task, String reason) {
    if (task == null || task.isCancelled()) {
      return;
    }
    try {
      TaskManager currentTaskManager = taskManager;
      if (currentTaskManager == null) {
        task.cancel(reason);
      } else {
        currentTaskManager.cancelTaskAndDescendants(
            task,
            reason,
            false,
            ActionListener.wrap(
                ignored -> {},
                failure ->
                    LOG.warn(
                        "Failed to cancel descendants of PPL asynchronous query task ({})",
                        failure.getClass().getSimpleName())));
      }
    } catch (RuntimeException e) {
      LOG.warn("Failed to cancel PPL asynchronous query task ({})", e.getClass().getSimpleName());
    }
  }

  private static ResourceNotFoundException notFound() {
    return new ResourceNotFoundException("PPL asynchronous query not found");
  }

  @Override
  protected void doStart() {
    acceptingSubmissions = true;
    if (threadPool != null) {
      reaper =
          threadPool.scheduleWithFixedDelay(
              this::safeReapExpired, REAPER_INTERVAL, ThreadPool.Names.GENERIC);
    }
  }

  @Override
  protected void doStop() {
    acceptingSubmissions = false;
    Scheduler.Cancellable scheduledReaper = reaper;
    if (scheduledReaper != null) {
      scheduledReaper.cancel();
      reaper = null;
    }
  }

  @Override
  protected void doClose() throws IOException {
    acceptingSubmissions = false;
    jobs.forEach(
        (contextId, job) ->
            applyRemoval(
                new LocatedJob(contextId, job),
                job.close("PPL asynchronous query service is closing")));
  }

  private void safeReapExpired() {
    try {
      reapExpired();
    } catch (RuntimeException e) {
      LOG.warn("Failed to reap expired PPL asynchronous queries");
    }
  }

  private record LocatedJob(String contextId, Job job) {}

  private record Registration(Publication publication) {
    private static Registration waiting() {
      return new Registration(null);
    }
  }

  private record Publication(
      Snapshot snapshot, SubmitWaiter waiter, boolean remove, boolean releaseRunning) {}

  private record Access(Snapshot snapshot, Removal removal) {
    private static Access snapshot(Snapshot snapshot) {
      return new Access(snapshot, null);
    }

    private static Access removed(Removal removal) {
      return new Access(null, removal);
    }
  }

  private record Removal(
      Status responseStatus,
      CancellableTask task,
      String reason,
      boolean expired,
      boolean releaseRunning) {}

  private static final class SubmitWaiter {
    private final ActionListener<Snapshot> listener;
    private final AtomicBoolean responded = new AtomicBoolean();
    private volatile TimeoutHandle timeout;

    private SubmitWaiter(ActionListener<Snapshot> listener) {
      this.listener = Objects.requireNonNull(listener);
    }

    private void setTimeout(TimeoutHandle timeout) {
      this.timeout = timeout;
      if (responded.get()) {
        timeout.cancel();
      }
    }

    private void respond(Snapshot snapshot) {
      if (responded.compareAndSet(false, true)) {
        TimeoutHandle scheduled = timeout;
        if (scheduled != null) {
          scheduled.cancel();
        }
        listener.onResponse(snapshot);
      }
    }

    private void fail(Exception exception) {
      if (responded.compareAndSet(false, true)) {
        TimeoutHandle scheduled = timeout;
        if (scheduled != null) {
          scheduled.cancel();
        }
        listener.onFailure(exception);
      }
    }
  }

  private static final class Job {
    private final String id;
    private final PPLAsyncQueryUser owner;
    private final long startTimeMillis;
    private CancellableTask task;

    private long keepAliveMillis;
    private long expirationTimeMillis;
    private Status status = Status.RUNNING;
    private QueryResponse response;
    private Failure failure;
    private long completionTimeMillis = -1L;
    private SubmitWaiter submitWaiter;
    private boolean removed;

    private Job(
        String id,
        PPLAsyncQueryUser owner,
        long startTimeMillis,
        long keepAliveMillis,
        CancellableTask task) {
      this.id = id;
      this.owner = owner;
      this.startTimeMillis = startTimeMillis;
      this.keepAliveMillis = keepAliveMillis;
      this.expirationTimeMillis = addWithoutOverflow(startTimeMillis, keepAliveMillis);
      this.task = task;
    }

    private synchronized Registration registerSubmitWaiter(
        SubmitWaiter waiter, boolean returnImmediately) {
      ensurePresent();
      if (submitWaiter != null) {
        throw new IllegalStateException("PPL asynchronous submit waiter is already registered");
      }
      if (status != Status.RUNNING) {
        removed = true;
        return new Registration(new Publication(snapshot(false), waiter, true, false));
      }
      if (returnImmediately) {
        return new Registration(new Publication(snapshot(true), waiter, false, false));
      }
      submitWaiter = waiter;
      return Registration.waiting();
    }

    private synchronized Publication timeout(SubmitWaiter waiter, long now) {
      if (removed || submitWaiter != waiter) {
        return null;
      }
      submitWaiter = null;
      if (status == Status.RUNNING) {
        expirationTimeMillis = addWithoutOverflow(now, keepAliveMillis);
        return new Publication(snapshot(true), waiter, false, false);
      }
      removed = true;
      return new Publication(snapshot(false), waiter, true, false);
    }

    private synchronized Publication complete(QueryResponse response, long now) {
      if (removed || status != Status.RUNNING) {
        return null;
      }
      this.response = response;
      task = null;
      status = Status.SUCCEEDED;
      completionTimeMillis = now;
      return terminalPublication();
    }

    private synchronized Publication fail(Failure failure, long now) {
      if (removed || status != Status.RUNNING) {
        return null;
      }
      this.failure = failure;
      task = null;
      status = Status.FAILED;
      completionTimeMillis = now;
      return terminalPublication();
    }

    private Publication terminalPublication() {
      SubmitWaiter waiter = submitWaiter;
      submitWaiter = null;
      if (waiter != null) {
        removed = true;
        return new Publication(snapshot(false), waiter, true, true);
      }
      return new Publication(snapshot(true), null, false, true);
    }

    private synchronized Access get(
        PPLAsyncQueryUser caller, long now, TimeValue requestedKeepAlive) {
      ensurePresent();
      owner.authorize(caller);
      if (now >= expirationTimeMillis) {
        return Access.removed(expireLocked("PPL asynchronous query expired"));
      }
      if (requestedKeepAlive != null) {
        keepAliveMillis = requestedKeepAlive.millis();
      }
      expirationTimeMillis = addWithoutOverflow(now, keepAliveMillis);
      return Access.snapshot(snapshot(true));
    }

    private synchronized Removal delete(PPLAsyncQueryUser caller, long now) {
      ensurePresent();
      owner.authorize(caller);
      if (now >= expirationTimeMillis) {
        return expireLocked("PPL asynchronous query expired");
      }
      boolean wasRunning = status == Status.RUNNING;
      Status responseStatus = wasRunning ? Status.CANCELLED : status;
      CancellableTask taskToCancel = wasRunning ? task : null;
      task = null;
      removed = true;
      submitWaiter = null;
      return new Removal(
          responseStatus,
          taskToCancel,
          "PPL asynchronous query cancelled by user",
          false,
          wasRunning);
    }

    private synchronized Removal expire(long now) {
      if (removed || submitWaiter != null || now < expirationTimeMillis) {
        return null;
      }
      return expireLocked("PPL asynchronous query expired");
    }

    private Removal expireLocked(String reason) {
      boolean wasRunning = status == Status.RUNNING;
      CancellableTask taskToCancel = wasRunning ? task : null;
      task = null;
      removed = true;
      submitWaiter = null;
      return new Removal(status, taskToCancel, reason, true, wasRunning);
    }

    private synchronized Removal abort() {
      if (removed) {
        return null;
      }
      boolean wasRunning = status == Status.RUNNING;
      CancellableTask taskToCancel = wasRunning ? task : null;
      task = null;
      removed = true;
      submitWaiter = null;
      return new Removal(
          status, taskToCancel, "PPL asynchronous query submission failed", false, wasRunning);
    }

    private synchronized Removal close(String reason) {
      if (removed) {
        return null;
      }
      boolean wasRunning = status == Status.RUNNING;
      CancellableTask taskToCancel = wasRunning ? task : null;
      task = null;
      removed = true;
      submitWaiter = null;
      return new Removal(status, taskToCancel, reason, false, wasRunning);
    }

    private Snapshot snapshot(boolean includeId) {
      long tookMillis =
          completionTimeMillis < 0 ? -1L : Math.max(0L, completionTimeMillis - startTimeMillis);
      return new Snapshot(includeId ? id : null, status, response, failure, tookMillis);
    }

    private void ensurePresent() {
      if (removed) {
        throw notFound();
      }
    }

    private static long addWithoutOverflow(long left, long right) {
      try {
        return Math.addExact(left, right);
      } catch (ArithmeticException e) {
        return Long.MAX_VALUE;
      }
    }
  }
}
