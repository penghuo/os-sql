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
import java.util.function.Function;
import java.util.function.IntSupplier;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.ResourceNotFoundException;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.lifecycle.AbstractLifecycleComponent;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.executor.AsyncQueryExecution;
import org.opensearch.sql.executor.ExecutionEngine.QueryResponse;
import org.opensearch.sql.executor.ExecutionEngine.Schema;
import org.opensearch.sql.ppl.domain.PPLQueryRequest;
import org.opensearch.tasks.CancellableTask;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskManager;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;

/** Owner-node lifecycle and current-result access for asynchronous PPL queries. */
public final class PPLAsyncQueryService extends AbstractLifecycleComponent {
  private static final Logger LOG = LogManager.getLogger(PPLAsyncQueryService.class);

  static final TimeValue DEFAULT_WAIT_FOR_COMPLETION =
      TimeValue.parseTimeValue(
          PPLQueryRequest.DEFAULT_WAIT_FOR_COMPLETION_TIMEOUT,
          PPLQueryRequest.WAIT_FOR_COMPLETION_TIMEOUT_FIELD);
  static final TimeValue DEFAULT_KEEP_ALIVE =
      TimeValue.parseTimeValue(
          PPLQueryRequest.DEFAULT_KEEP_ALIVE, PPLQueryRequest.KEEP_ALIVE_FIELD);
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

  /**
   * Detached point-in-time view of a job used to build an HTTP response.
   *
   * <p>This is an internal response model, not part of the public API. The public API is the JSON
   * produced by {@link PPLAsyncQueryResponseFormatter}. A snapshot deliberately copies data out of
   * the mutable {@link Job}, so response formatting never reads live job state.
   *
   * @param id opaque job ID, or {@code null} for a terminal response returned directly by submit
   * @param status lifecycle state captured with the result
   * @param response current query result, or {@code null} before a result is available
   * @param failure sanitized failure for {@link Status#FAILED}, otherwise {@code null}
   * @param tookMillis elapsed execution time, available for a completed job
   */
  record JobSnapshot(
      String id, Status status, QueryResponse response, Failure failure, long tookMillis) {}

  /**
   * Lightweight lifecycle state captured atomically under the job lock.
   *
   * <p>The service reads {@link #execution} and creates a {@link JobSnapshot} only after the job
   * lock has been released.
   */
  private record JobView(
      String id, Status status, AsyncQueryExecution execution, Failure failure, long tookMillis) {}

  /** Response model returned after DELETE removes a retained job. */
  record DeleteResult(String id, Status status) {}

  /** Admitted job whose query execution has not yet been attached. */
  final class Submission {
    private final String id;
    private final CancellableTask task;

    private Submission(String id, CancellableTask task) {
      this.id = id;
      this.task = task;
    }

    /** Starts the query with the retained task, then transfers its execution handle to the job. */
    void start(Function<CancellableTask, AsyncQueryExecution> executionStarter) {
      try {
        AsyncQueryExecution execution = Objects.requireNonNull(executionStarter.apply(task));
        attachExecution(id, execution);
      } catch (Exception e) {
        PPLAsyncQueryService.this.fail(id, e);
      }
    }
  }

  /** Sanitized failure retained by a job; raw exception messages are not stored. */
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

  /**
   * Admits an asynchronous query and starts the submit timeout/completion race.
   *
   * <p>The service registers a query task whose cancellation is linked to the original submit task
   * until the POST response is returned, then transfers ownership to the job. The returned {@link
   * Submission} starts execution with that task. The job releases it on completion, cancellation,
   * expiration, or shutdown.
   *
   * @param owner authenticated owner retained with the job
   * @param requestedKeepAlive requested job lease
   * @param requestedWaitForCompletion maximum time to wait for a direct result
   * @param request transport request used to create the retained query task
   * @param submitTask task associated with the original POST request
   * @param responseListener listener that receives either the direct result or retained job ID
   * @return admitted submission used to start query execution
   */
  Submission submit(
      PPLAsyncQueryUser owner,
      String requestedKeepAlive,
      String requestedWaitForCompletion,
      TransportPPLQueryRequest request,
      PPLQueryTask submitTask,
      ActionListener<JobSnapshot> responseListener) {
    TimeValue keepAlive =
        TimeValue.parseTimeValue(requestedKeepAlive, PPLQueryRequest.KEEP_ALIVE_FIELD);
    TimeValue waitForCompletion =
        TimeValue.parseTimeValue(
            requestedWaitForCompletion, PPLQueryRequest.WAIT_FOR_COMPLETION_TIMEOUT_FIELD);
    validateKeepAlive(keepAlive);
    validateWaitForCompletion(waitForCompletion);
    JobTask jobTask = registerJobTask(request, submitTask);
    String id = null;
    try {
      id = create(owner, keepAlive, jobTask);
      registerSubmitWaiter(id, waitForCompletion, responseListener);
      return new Submission(id, jobTask.task());
    } catch (RuntimeException e) {
      if (id == null) {
        jobTask.close();
      } else {
        Job job = findInternal(id);
        if (job != null) {
          applyRemoval(job, job.abort());
        }
      }
      throw e;
    }
  }

  String create(PPLAsyncQueryUser owner, TimeValue keepAlive, CancellableTask task) {
    return create(owner, keepAlive, new JobTask(task, () -> {}));
  }

  private String create(PPLAsyncQueryUser owner, TimeValue keepAlive, JobTask task) {
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
        String encodedId = jobId.encode();
        Job job = new Job(encodedId, owner, now, keepAlive.millis(), task);
        if (jobs.putIfAbsent(encodedId, job) == null) {
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

  void registerSubmitWaiter(
      String id, TimeValue waitForCompletion, ActionListener<JobSnapshot> responseListener) {
    validateWaitForCompletion(waitForCompletion);
    Job job = findLocal(id);
    SubmitWaiter waiter = new SubmitWaiter(responseListener);
    SubmitRegistration registration =
        job.registerSubmitWaiter(waiter, waitForCompletion.millis() == 0);
    if (registration.immediateTransition != null) {
      applyTransition(job, registration.immediateTransition);
      return;
    }

    try {
      TimeoutHandle timeout =
          timeoutScheduler.schedule(
              waitForCompletion,
              () -> applyTransition(job, job.timeout(waiter, currentTimeMillis.getAsLong())));
      waiter.setTimeout(timeout);
    } catch (RuntimeException e) {
      Removal removal = job.abort();
      applyRemoval(job, removal);
      throw e;
    }
  }

  /**
   * Transfers ownership of an execution handle to its job.
   *
   * <p>If the job was already removed, the late handle is closed immediately.
   */
  void attachExecution(String id, AsyncQueryExecution execution) {
    Objects.requireNonNull(execution);
    Job job = findInternal(id);
    if (job == null) {
      closeExecution(execution);
      return;
    }
    AsyncQueryExecution rejected = job.attachExecution(execution);
    if (rejected != null) {
      closeExecution(rejected);
      return;
    }
    execution
        .completion()
        .whenComplete(
            (ignored, failure) -> {
              if (failure == null) {
                complete(id);
              } else {
                fail(id, asException(failure));
              }
            });
  }

  void complete(String id) {
    Job job = findInternal(id);
    if (job != null) {
      applyTransition(job, job.complete(currentTimeMillis.getAsLong()));
    }
  }

  void fail(String id, Exception failure) {
    Job job = findInternal(id);
    if (job != null) {
      applyTransition(
          job,
          job.fail(Failure.from(Objects.requireNonNull(failure)), currentTimeMillis.getAsLong()));
    }
  }

  JobSnapshot get(String id, PPLAsyncQueryUser caller, TimeValue requestedKeepAlive) {
    if (requestedKeepAlive != null) {
      validateKeepAlive(requestedKeepAlive);
    }
    Job job = findLocal(id);
    Access access = job.get(caller, currentTimeMillis.getAsLong(), requestedKeepAlive);
    if (access.removal != null) {
      applyRemoval(job, access.removal);
      throw notFound();
    }
    return materialize(access.view);
  }

  DeleteResult delete(String id, PPLAsyncQueryUser caller) {
    Job job = findLocal(id);
    Removal removal = job.delete(caller, currentTimeMillis.getAsLong());
    applyRemoval(job, removal);
    if (removal.expired) {
      throw notFound();
    }
    return new DeleteResult(id, removal.responseStatus);
  }

  void reapExpired() {
    long now = currentTimeMillis.getAsLong();
    jobs.forEach((id, job) -> applyRemoval(job, job.expire(now)));
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

  /**
   * Applies side effects selected by a {@link Job} transition.
   *
   * <p>The job decides its state change while holding the job lock, then returns a value describing
   * the required side effects. Map mutation, capacity accounting, and listener callbacks happen
   * here after the lock has been released.
   */
  private void applyTransition(Job job, JobTransition transition) {
    if (transition == null) {
      return;
    }
    if (transition.runningSlotAction == RunningSlotAction.RELEASE) {
      releaseRunning();
    }
    if (transition.jobRetention == JobRetention.REMOVE && jobs.remove(job.id, job)) {
      releaseRetained();
    }

    JobSnapshot snapshot = null;
    RuntimeException materializationFailure = null;
    try {
      if (transition.submitResponse != null) {
        snapshot = materialize(transition.submitResponse.view);
      }
    } catch (RuntimeException e) {
      materializationFailure = e;
    } finally {
      closeExecution(transition.executionToClose);
      closeTask(transition.taskToClose);
    }

    if (transition.submitResponse != null) {
      if (materializationFailure == null) {
        transition.submitResponse.waiter.respond(snapshot);
      } else {
        if (transition.jobRetention == JobRetention.RETAIN) {
          applyRemoval(job, job.abort());
        }
        transition.submitResponse.waiter.fail(materializationFailure);
      }
    }
  }

  private void applyRemoval(Job job, Removal removal) {
    if (removal == null) {
      return;
    }
    if (jobs.remove(job.id, job)) {
      if (removal.releaseRunningSlot) {
        releaseRunning();
      }
      releaseRetained();
    }
    cancel(removal.task, removal.reason);
    closeExecution(removal.execution);
  }

  private JobSnapshot materialize(JobView view) {
    QueryResponse response = null;
    if (view.status == Status.SUCCEEDED || view.status == Status.RUNNING) {
      response =
          view.execution == null
              ? null
              : view.execution.currentResult().map(PPLAsyncQueryService::copy).orElse(null);
    }
    if (view.status == Status.SUCCEEDED && response == null) {
      throw new IllegalStateException(
          "Successful PPL asynchronous execution completed without a final result");
    }
    return new JobSnapshot(view.id, view.status, response, view.failure, view.tookMillis);
  }

  private static void closeExecution(AsyncQueryExecution execution) {
    if (execution == null) {
      return;
    }
    try {
      execution.close();
    } catch (RuntimeException e) {
      LOG.warn(
          "Failed to close PPL asynchronous query execution ({})", e.getClass().getSimpleName());
    }
  }

  private static void closeTask(JobTask task) {
    if (task != null) {
      task.close();
    }
  }

  private Job findLocal(String encodedId) {
    PPLAsyncQueryJobId jobId = PPLAsyncQueryJobId.parse(encodedId);
    String localNodeId =
        Objects.requireNonNull(ownerNodeIdSupplier.get(), "Local node ID is not initialized");
    if (!localNodeId.equals(jobId.ownerNodeId())) {
      throw new IllegalArgumentException("PPL asynchronous query is not owned by this node");
    }
    Job job = jobs.get(encodedId);
    if (job == null) {
      throw notFound();
    }
    return job;
  }

  private Job findInternal(String encodedId) {
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

  private void cancel(JobTask task, String reason) {
    if (task == null) {
      return;
    }
    CancellableTask cancellableTask = task.task();
    if (cancellableTask == null || cancellableTask.isCancelled()) {
      task.close();
      return;
    }
    try {
      TaskManager currentTaskManager = taskManager;
      if (currentTaskManager == null) {
        cancellableTask.cancel(reason);
        task.close();
      } else {
        currentTaskManager.cancelTaskAndDescendants(
            cancellableTask,
            reason,
            false,
            ActionListener.wrap(
                ignored -> task.close(),
                failure -> {
                  task.close();
                  LOG.warn(
                      "Failed to cancel descendants of PPL asynchronous query task ({})",
                      failure.getClass().getSimpleName());
                }));
      }
    } catch (RuntimeException e) {
      task.close();
      LOG.warn("Failed to cancel PPL asynchronous query task ({})", e.getClass().getSimpleName());
    }
  }

  private JobTask registerJobTask(TransportPPLQueryRequest request, PPLQueryTask submitTask) {
    TaskManager currentTaskManager =
        Objects.requireNonNull(
            taskManager, "PPL asynchronous query task manager is not initialized");
    Objects.requireNonNull(submitTask, "PPL asynchronous query submit task is not initialized");
    DiscoveryNode localNode =
        Objects.requireNonNull(currentTaskManager.localNode(), "Local node is not initialized");

    // This task is registered directly rather than through TransportAction.execute(), so reproduce
    // the two pieces of OpenSearch child-task bookkeeping that TransportAction normally performs.
    // The child-node registration lets parent cancellation send a ban to this node; parentTaskId
    // lets that ban find and cancel the retained task.
    Releasable childNodeRegistration =
        currentTaskManager.registerChildNode(submitTask.getId(), localNode);
    TaskId originalParent = request.getParentTask();
    boolean registered = false;
    try {
      request.setParentTask(localNode.getId(), submitTask.getId());
      Task task = currentTaskManager.register("transport", PPLQueryAction.NAME, request);
      if (!(task instanceof PPLQueryTask pplQueryTask)) {
        currentTaskManager.unregister(task);
        throw new IllegalStateException("Failed to create PPL asynchronous query task");
      }
      registered = true;
      return new JobTask(
          pplQueryTask,
          () -> {
            try {
              currentTaskManager.unregister(pplQueryTask);
            } finally {
              childNodeRegistration.close();
            }
          });
    } finally {
      request.setParentTask(originalParent);
      if (!registered) {
        childNodeRegistration.close();
      }
    }
  }

  private static Exception asException(Throwable failure) {
    Throwable cause =
        failure instanceof java.util.concurrent.CompletionException && failure.getCause() != null
            ? failure.getCause()
            : failure;
    return cause instanceof Exception exception ? exception : new RuntimeException(cause);
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
        (id, job) -> applyRemoval(job, job.close("PPL asynchronous query service is closing")));
  }

  private void safeReapExpired() {
    try {
      reapExpired();
    } catch (RuntimeException e) {
      LOG.warn("Failed to reap expired PPL asynchronous queries");
    }
  }

  private record JobTask(CancellableTask task, Runnable release) {
    private void close() {
      release.run();
    }
  }

  /**
   * Result of attaching the original POST listener to a job.
   *
   * @param immediateTransition non-null when submit must respond immediately; null when the waiter
   *     was attached and the query/timeout race should continue
   */
  private record SubmitRegistration(JobTransition immediateTransition) {
    private static SubmitRegistration waiting() {
      return new SubmitRegistration(null);
    }

    private static SubmitRegistration respondImmediately(JobTransition transition) {
      return new SubmitRegistration(transition);
    }
  }

  /** Whether the service should keep or remove the job after applying a transition. */
  private enum JobRetention {
    RETAIN,
    REMOVE
  }

  /** Whether a transition keeps or releases the per-node running-query capacity slot. */
  private enum RunningSlotAction {
    KEEP,
    RELEASE
  }

  /**
   * The original POST response selected by a job transition.
   *
   * <p>It is absent after submit already returned a job ID; in that case a later GET creates a new
   * snapshot from the retained job.
   *
   * @param view point-in-time job state to materialize for the POST response
   * @param waiter one-shot listener for the POST request that is still waiting
   */
  private record SubmitResponse(JobView view, SubmitWaiter waiter) {}

  /**
   * Side effects selected atomically by a {@link Job} state transition.
   *
   * @param submitResponse response for the original POST, or {@code null} when POST already
   *     returned
   * @param jobRetention whether the owner-node job map keeps or removes the job
   * @param runningSlotAction whether this transition releases running-query capacity
   * @param executionToClose execution-owned resources to release after response materialization
   * @param taskToClose retained task registration to release after execution completes
   */
  private record JobTransition(
      SubmitResponse submitResponse,
      JobRetention jobRetention,
      RunningSlotAction runningSlotAction,
      AsyncQueryExecution executionToClose,
      JobTask taskToClose) {

    private static JobTransition respond(
        JobView view,
        SubmitWaiter waiter,
        JobRetention jobRetention,
        RunningSlotAction runningSlotAction,
        AsyncQueryExecution executionToClose,
        JobTask taskToClose) {
      return new JobTransition(
          new SubmitResponse(view, waiter),
          jobRetention,
          runningSlotAction,
          executionToClose,
          taskToClose);
    }

    private static JobTransition withoutSubmitResponse(
        JobRetention jobRetention,
        RunningSlotAction runningSlotAction,
        AsyncQueryExecution executionToClose,
        JobTask taskToClose) {
      return new JobTransition(
          null, jobRetention, runningSlotAction, executionToClose, taskToClose);
    }
  }

  /** Result of reading a job: either a view to materialize or removal of an expired job. */
  private record Access(JobView view, Removal removal) {
    private static Access view(JobView view) {
      return new Access(view, null);
    }

    private static Access removed(Removal removal) {
      return new Access(null, removal);
    }
  }

  /**
   * Side effects required after DELETE, expiry, abort, or service shutdown removes a job.
   *
   * @param responseStatus status returned by DELETE when applicable
   * @param task running task to cancel, or {@code null} for a terminal job
   * @param execution execution-owned resources detached from the job
   * @param reason non-sensitive cancellation reason
   * @param expired whether the caller should observe the removal as not found
   * @param releaseRunningSlot whether running-query capacity must be released
   */
  private record Removal(
      Status responseStatus,
      JobTask task,
      AsyncQueryExecution execution,
      String reason,
      boolean expired,
      boolean releaseRunningSlot) {}

  /**
   * One-shot responder for the original asynchronous POST request.
   *
   * <p>Query completion and the submit timeout race to use this waiter. The atomic guard guarantees
   * that exactly one path invokes the external listener.
   */
  private static final class SubmitWaiter {
    private final ActionListener<JobSnapshot> listener;
    private final AtomicBoolean responded = new AtomicBoolean();
    private volatile TimeoutHandle timeout;

    private SubmitWaiter(ActionListener<JobSnapshot> listener) {
      this.listener = Objects.requireNonNull(listener);
    }

    private void setTimeout(TimeoutHandle timeout) {
      this.timeout = timeout;
      if (responded.get()) {
        timeout.cancel();
      }
    }

    private void respond(JobSnapshot snapshot) {
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

  /**
   * Mutable state machine for one asynchronous PPL query.
   *
   * <p>Every lifecycle and lease transition is synchronized on this object. Methods mutate only
   * job-owned state and return immutable transition values; they never call listeners, mutate the
   * service map, update capacity counters, or cancel tasks while holding the lock.
   *
   * <p>After attachment, the job owns one {@link AsyncQueryExecution}. Reads borrow it through a
   * {@link JobView}; removal transitions detach it so the service can close it outside the lock.
   */
  private static final class Job {
    private final String id;
    private final PPLAsyncQueryUser owner;
    private final long startTimeMillis;
    private JobTask task;

    private long keepAliveMillis;
    private long expirationTimeMillis;
    private Status status = Status.RUNNING;
    private AsyncQueryExecution execution;
    private Failure failure;
    private long completionTimeMillis = -1L;
    private SubmitWaiter submitWaiter;
    private boolean removed;

    private Job(
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

    private synchronized SubmitRegistration registerSubmitWaiter(
        SubmitWaiter waiter, boolean returnImmediately) {
      ensurePresent();
      if (submitWaiter != null) {
        throw new IllegalStateException("PPL asynchronous submit waiter is already registered");
      }
      if (status != Status.RUNNING) {
        removed = true;
        JobView view = view(false);
        AsyncQueryExecution executionToClose = detachExecution();
        return SubmitRegistration.respondImmediately(
            JobTransition.respond(
                view, waiter, JobRetention.REMOVE, RunningSlotAction.KEEP, executionToClose, null));
      }
      if (returnImmediately) {
        return SubmitRegistration.respondImmediately(
            JobTransition.respond(
                view(true), waiter, JobRetention.RETAIN, RunningSlotAction.KEEP, null, null));
      }
      submitWaiter = waiter;
      return SubmitRegistration.waiting();
    }

    private synchronized JobTransition timeout(SubmitWaiter waiter, long now) {
      if (removed || submitWaiter != waiter) {
        return null;
      }
      submitWaiter = null;
      if (status == Status.RUNNING) {
        expirationTimeMillis = addWithoutOverflow(now, keepAliveMillis);
        return JobTransition.respond(
            view(true), waiter, JobRetention.RETAIN, RunningSlotAction.KEEP, null, null);
      }
      removed = true;
      JobView view = view(false);
      AsyncQueryExecution executionToClose = detachExecution();
      return JobTransition.respond(
          view, waiter, JobRetention.REMOVE, RunningSlotAction.KEEP, executionToClose, null);
    }

    private synchronized AsyncQueryExecution attachExecution(AsyncQueryExecution execution) {
      Objects.requireNonNull(execution);
      if (removed || status != Status.RUNNING || this.execution != null) {
        return execution;
      }
      this.execution = execution;
      return null;
    }

    private synchronized JobTransition complete(long now) {
      if (removed || status != Status.RUNNING) {
        return null;
      }
      if (execution == null) {
        throw new IllegalStateException(
            "PPL asynchronous execution must be attached before successful completion");
      }
      JobTask taskToClose = detachTask();
      status = Status.SUCCEEDED;
      completionTimeMillis = now;
      return terminalTransition(taskToClose);
    }

    private synchronized JobTransition fail(Failure failure, long now) {
      if (removed || status != Status.RUNNING) {
        return null;
      }
      this.failure = failure;
      JobTask taskToClose = detachTask();
      status = Status.FAILED;
      completionTimeMillis = now;
      return terminalTransition(taskToClose);
    }

    private JobTransition terminalTransition(JobTask taskToClose) {
      SubmitWaiter waiter = submitWaiter;
      submitWaiter = null;
      if (waiter != null) {
        removed = true;
        JobView view = view(false);
        AsyncQueryExecution executionToClose = detachExecution();
        return JobTransition.respond(
            view,
            waiter,
            JobRetention.REMOVE,
            RunningSlotAction.RELEASE,
            executionToClose,
            taskToClose);
      }
      if (status == Status.FAILED) {
        return JobTransition.withoutSubmitResponse(
            JobRetention.RETAIN, RunningSlotAction.RELEASE, detachExecution(), taskToClose);
      }
      return JobTransition.withoutSubmitResponse(
          JobRetention.RETAIN, RunningSlotAction.RELEASE, null, taskToClose);
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
      return Access.view(view(true));
    }

    private synchronized Removal delete(PPLAsyncQueryUser caller, long now) {
      ensurePresent();
      owner.authorize(caller);
      if (now >= expirationTimeMillis) {
        return expireLocked("PPL asynchronous query expired");
      }
      boolean wasRunning = status == Status.RUNNING;
      Status responseStatus = wasRunning ? Status.CANCELLED : status;
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

    private synchronized Removal expire(long now) {
      if (removed || submitWaiter != null || now < expirationTimeMillis) {
        return null;
      }
      return expireLocked("PPL asynchronous query expired");
    }

    private Removal expireLocked(String reason) {
      boolean wasRunning = status == Status.RUNNING;
      JobTask taskToCancel = wasRunning ? detachTask() : null;
      AsyncQueryExecution executionToClose = detachExecution();
      removed = true;
      submitWaiter = null;
      return new Removal(status, taskToCancel, executionToClose, reason, true, wasRunning);
    }

    private synchronized Removal abort() {
      if (removed) {
        return null;
      }
      boolean wasRunning = status == Status.RUNNING;
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

    private synchronized Removal close(String reason) {
      if (removed) {
        return null;
      }
      boolean wasRunning = status == Status.RUNNING;
      JobTask taskToCancel = wasRunning ? detachTask() : null;
      AsyncQueryExecution executionToClose = detachExecution();
      removed = true;
      submitWaiter = null;
      return new Removal(status, taskToCancel, executionToClose, reason, false, wasRunning);
    }

    private JobView view(boolean includeId) {
      long tookMillis =
          completionTimeMillis < 0 ? -1L : Math.max(0L, completionTimeMillis - startTimeMillis);
      return new JobView(includeId ? id : null, status, execution, failure, tookMillis);
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
