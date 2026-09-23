/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport.asyncquery;

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
import org.opensearch.sql.plugin.transport.PPLQueryAction;
import org.opensearch.sql.plugin.transport.PPLQueryTask;
import org.opensearch.sql.plugin.transport.TransportPPLQueryRequest;
import org.opensearch.sql.plugin.transport.asyncquery.PPLAsyncQueryJob.Access;
import org.opensearch.sql.plugin.transport.asyncquery.PPLAsyncQueryJob.JobTask;
import org.opensearch.sql.plugin.transport.asyncquery.PPLAsyncQueryJob.Removal;
import org.opensearch.sql.plugin.transport.asyncquery.PPLAsyncQueryJob.Retention;
import org.opensearch.sql.plugin.transport.asyncquery.PPLAsyncQueryJob.RunningSlotAction;
import org.opensearch.sql.plugin.transport.asyncquery.PPLAsyncQueryJob.SubmitRegistration;
import org.opensearch.sql.plugin.transport.asyncquery.PPLAsyncQueryJob.Transition;
import org.opensearch.sql.plugin.transport.asyncquery.PPLAsyncQueryJob.View;
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

  /** Lifecycle state exposed in asynchronous PPL responses. */
  public enum Status {
    /** Query execution is still running. */
    RUNNING,

    /** Query execution completed successfully. */
    SUCCEEDED,

    /** Query execution failed. */
    FAILED,

    /** Query execution was cancelled by DELETE. */
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
   * the mutable {@link PPLAsyncQueryJob}, so response formatting never reads live job state.
   *
   * @param id opaque job ID, or {@code null} for a terminal response returned directly by submit
   * @param status lifecycle state captured with the result
   * @param response current query result, or {@code null} before a result is available
   * @param failure sanitized failure for {@link Status#FAILED}, otherwise {@code null}
   * @param tookMillis elapsed execution time, available for a completed job
   */
  public record JobSnapshot(
      String id, Status status, QueryResponse response, Failure failure, long tookMillis) {}

  /** Response model returned after DELETE removes a retained job. */
  record DeleteResult(String id, Status status) {}

  /**
   * Admitted asynchronous query whose execution has not yet been attached.
   *
   * <p>This capability keeps the internal job object hidden from the transport layer while ensuring
   * that execution is attached to the exact job created by {@link #submit}.
   */
  public final class Submission {
    private final PPLAsyncQueryJob job;
    private final CancellableTask task;

    private Submission(PPLAsyncQueryJob job, CancellableTask task) {
      this.job = job;
      this.task = task;
    }

    /**
     * Starts query execution with the retained task and transfers the returned handle to the job.
     *
     * @param executionStarter creates the execution handle using the retained cancellable task
     */
    public void start(Function<CancellableTask, AsyncQueryExecution> executionStarter) {
      try {
        AsyncQueryExecution execution = Objects.requireNonNull(executionStarter.apply(task));
        attachExecution(job, execution);
      } catch (RuntimeException e) {
        PPLAsyncQueryService.this.fail(job, e);
      }
    }
  }

  /**
   * Sanitized failure retained by a job; raw exception messages are not stored.
   *
   * @param type exception type without sensitive query data
   * @param reason stable client-facing failure reason
   */
  public record Failure(String type, String reason) {
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
  private final ConcurrentMap<String, PPLAsyncQueryJob> jobs = new ConcurrentHashMap<>();
  private final Object admissionLock = new Object();

  private int runningQueries;
  private int retainedJobs;
  private volatile boolean acceptingSubmissions = true;
  private volatile Scheduler.Cancellable reaper;
  private volatile TaskManager taskManager;

  /**
   * Creates the owner-node lifecycle service.
   *
   * @param ownerNodeIdSupplier supplies the current local node ID
   * @param threadPool schedules submit timeouts and expiration reaping
   * @param settings supplies asynchronous query capacity and duration limits
   */
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
  public Submission submit(
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
    PPLAsyncQueryJob job;
    try {
      job = create(owner, keepAlive, jobTask);
    } catch (RuntimeException | Error e) {
      jobTask.close();
      throw e;
    }
    try {
      registerSubmitWaiter(job, waitForCompletion, responseListener);
      return new Submission(job, jobTask.task());
    } catch (RuntimeException | Error e) {
      applyRemoval(job, job.abort());
      throw e;
    }
  }

  PPLAsyncQueryJob create(PPLAsyncQueryUser owner, TimeValue keepAlive, CancellableTask task) {
    return create(owner, keepAlive, new JobTask(task, () -> {}));
  }

  private PPLAsyncQueryJob create(PPLAsyncQueryUser owner, TimeValue keepAlive, JobTask task) {
    Objects.requireNonNull(owner);
    validateKeepAlive(keepAlive);
    reserveCapacity();
    try {
      String ownerNodeId =
          Objects.requireNonNull(ownerNodeIdSupplier.get(), "Local node ID is not initialized");
      long now = currentTimeMillis.getAsLong();
      while (true) {
        PPLAsyncQueryJobId jobId = PPLAsyncQueryJobId.create(ownerNodeId);
        String encodedId = jobId.encode();
        PPLAsyncQueryJob job =
            new PPLAsyncQueryJob(encodedId, owner, now, keepAlive.millis(), task);
        if (jobs.putIfAbsent(encodedId, job) == null) {
          return job;
        }
      }
    } catch (RuntimeException | Error e) {
      releaseCapacity();
      throw e;
    }
  }

  void registerSubmitWaiter(
      PPLAsyncQueryJob job,
      TimeValue waitForCompletion,
      ActionListener<JobSnapshot> responseListener) {
    validateWaitForCompletion(waitForCompletion);
    SubmitWaiter waiter = new SubmitWaiter(responseListener);
    SubmitRegistration registration =
        job.registerSubmitWaiter(waiter, waitForCompletion.millis() == 0);
    if (registration.immediateTransition() != null) {
      applyTransition(job, registration.immediateTransition());
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
  void attachExecution(PPLAsyncQueryJob job, AsyncQueryExecution execution) {
    Objects.requireNonNull(job);
    Objects.requireNonNull(execution);
    if (!job.tryAttachExecution(execution)) {
      closeExecution(execution);
      return;
    }
    execution
        .completion()
        .whenComplete(
            (ignored, failure) -> {
              if (failure == null) {
                complete(job);
              } else {
                fail(job, asException(failure));
              }
            });
  }

  void complete(PPLAsyncQueryJob job) {
    applyTransition(job, job.complete(currentTimeMillis.getAsLong()));
  }

  void fail(PPLAsyncQueryJob job, Exception failure) {
    applyTransition(
        job,
        job.fail(Failure.from(Objects.requireNonNull(failure)), currentTimeMillis.getAsLong()));
  }

  JobSnapshot get(String id, PPLAsyncQueryUser caller, TimeValue requestedKeepAlive) {
    if (requestedKeepAlive != null) {
      validateKeepAlive(requestedKeepAlive);
    }
    PPLAsyncQueryJob job = findLocal(id);
    Access access = job.get(caller, currentTimeMillis.getAsLong(), requestedKeepAlive);
    if (access.removal() != null) {
      applyRemoval(job, access.removal());
      throw notFound();
    }
    return materialize(access.view());
  }

  DeleteResult delete(String id, PPLAsyncQueryUser caller) {
    PPLAsyncQueryJob job = findLocal(id);
    Removal removal = job.delete(caller, currentTimeMillis.getAsLong());
    applyRemoval(job, removal);
    if (removal.expired()) {
      throw notFound();
    }
    return new DeleteResult(id, removal.responseStatus());
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

  /**
   * Attaches the node task manager after transport actions have been initialized.
   *
   * @param taskManager task manager used to register and cancel retained query tasks
   */
  public void attachTaskManager(TaskManager taskManager) {
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

  private void releaseCapacity() {
    synchronized (admissionLock) {
      if (runningQueries > 0) {
        runningQueries--;
      }
      if (retainedJobs > 0) {
        retainedJobs--;
      }
    }
  }

  /**
   * Applies side effects selected by a {@link PPLAsyncQueryJob} transition.
   *
   * <p>The job decides its state change while holding the job lock, then returns a value describing
   * the required side effects. Map mutation, capacity accounting, and listener callbacks happen
   * here after the lock has been released.
   */
  private void applyTransition(PPLAsyncQueryJob job, Transition transition) {
    if (transition == null) {
      return;
    }
    if (transition.runningSlotAction() == RunningSlotAction.RELEASE) {
      releaseRunning();
    }
    if (transition.retention() == Retention.REMOVE && jobs.remove(job.id(), job)) {
      releaseRetained();
    }

    JobSnapshot snapshot = null;
    RuntimeException materializationFailure = null;
    try {
      if (transition.submitResponse() != null) {
        snapshot = materialize(transition.submitResponse().view());
      }
    } catch (RuntimeException e) {
      materializationFailure = e;
    } finally {
      closeExecution(transition.executionToClose());
      closeTask(transition.taskToClose());
    }

    if (transition.submitResponse() != null) {
      if (materializationFailure == null) {
        transition.submitResponse().waiter().respond(snapshot);
      } else {
        if (transition.retention() == Retention.RETAIN) {
          applyRemoval(job, job.abort());
        }
        transition.submitResponse().waiter().fail(materializationFailure);
      }
    }
  }

  private void applyRemoval(PPLAsyncQueryJob job, Removal removal) {
    if (removal == null) {
      return;
    }
    if (jobs.remove(job.id(), job)) {
      if (removal.releaseRunningSlot()) {
        releaseRunning();
      }
      releaseRetained();
    }
    cancel(removal.task(), removal.reason());
    closeExecution(removal.execution());
  }

  private JobSnapshot materialize(View view) {
    QueryResponse response = null;
    if (view.status() == Status.SUCCEEDED || view.status() == Status.RUNNING) {
      response =
          view.execution() == null
              ? null
              : view.execution().currentResult().map(PPLAsyncQueryService::copy).orElse(null);
    }
    if (view.status() == Status.SUCCEEDED && response == null) {
      throw new IllegalStateException(
          "Successful PPL asynchronous execution completed without a final result");
    }
    return new JobSnapshot(view.id(), view.status(), response, view.failure(), view.tookMillis());
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

  private PPLAsyncQueryJob findLocal(String encodedId) {
    PPLAsyncQueryJobId jobId = PPLAsyncQueryJobId.parse(encodedId);
    String localNodeId =
        Objects.requireNonNull(ownerNodeIdSupplier.get(), "Local node ID is not initialized");
    if (!localNodeId.equals(jobId.ownerNodeId())) {
      throw new IllegalArgumentException("PPL asynchronous query is not owned by this node");
    }
    PPLAsyncQueryJob job = jobs.get(encodedId);
    if (job == null) {
      throw notFound();
    }
    return job;
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

  /**
   * One-shot responder for the original asynchronous POST request.
   *
   * <p>Query completion and the submit timeout race to use this waiter. The atomic guard guarantees
   * that exactly one path invokes the external listener.
   */
  static final class SubmitWaiter {
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
}
