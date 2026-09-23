/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport.asyncquery;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Test;
import org.mockito.InOrder;
import org.opensearch.OpenSearchSecurityException;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.ResourceNotFoundException;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.executor.AsyncQueryExecution;
import org.opensearch.sql.executor.ExecutionEngine.QueryResponse;
import org.opensearch.sql.executor.ExecutionEngine.Schema;
import org.opensearch.sql.executor.ExecutionEngine.Schema.Column;
import org.opensearch.sql.plugin.transport.PPLQueryAction;
import org.opensearch.sql.plugin.transport.PPLQueryTask;
import org.opensearch.sql.plugin.transport.TransportPPLQueryRequest;
import org.opensearch.tasks.CancellableTask;
import org.opensearch.tasks.TaskManager;

public class PPLAsyncQueryServiceTest {
  private static final PPLAsyncQueryUser OWNER =
      new PPLAsyncQueryUser(false, null, null, List.of());

  private final AtomicLong now = new AtomicLong(1_000);
  private final AtomicReference<Runnable> timeoutTask = new AtomicReference<>();
  private final PPLAsyncQueryService service = service(20, 100);

  @Test
  public void fastSuccessReturnsDirectResultWithoutRetainingJob() {
    String id = createJob(null);
    AtomicReference<PPLAsyncQueryService.JobSnapshot> result = new AtomicReference<>();
    AtomicInteger responses = new AtomicInteger();

    service.registerSubmitWaiter(
        id,
        TimeValue.timeValueSeconds(5),
        listener(
            snapshot -> {
              result.set(snapshot);
              responses.incrementAndGet();
            }));
    TrackingExecution execution = new TrackingExecution(response(2));
    service.attachExecution(id, execution);
    now.addAndGet(25);
    service.complete(id);

    assertEquals(1, responses.get());
    assertNull(result.get().id());
    assertEquals(PPLAsyncQueryService.Status.SUCCEEDED, result.get().status());
    assertEquals(2, result.get().response().getResults().size());
    assertEquals(25, result.get().tookMillis());
    assertEquals(0, service.runningQueryCount());
    assertEquals(0, service.retainedJobCount());
    assertEquals(1, execution.reads.get());
    assertEquals(1, execution.closes.get());
    assertThrows(ResourceNotFoundException.class, () -> service.get(id, OWNER, null));

    timeoutTask.get().run();
    assertEquals(1, responses.get());
  }

  @Test
  public void timeoutReturnsIdAndLaterGetReturnsCompleteResult() {
    String id = createJob(null);
    AtomicReference<PPLAsyncQueryService.JobSnapshot> submit = new AtomicReference<>();
    service.registerSubmitWaiter(id, TimeValue.timeValueSeconds(5), listener(submit::set));

    timeoutTask.get().run();

    assertEquals(id, submit.get().id());
    assertEquals(PPLAsyncQueryService.Status.RUNNING, submit.get().status());
    assertNull(submit.get().response());
    assertEquals(1, service.runningQueryCount());
    assertEquals(1, service.retainedJobCount());

    now.addAndGet(25);
    TrackingExecution execution = new TrackingExecution(response(2));
    service.attachExecution(id, execution);
    service.complete(id);
    PPLAsyncQueryService.JobSnapshot completed = service.get(id, OWNER, null);

    assertEquals(id, completed.id());
    assertEquals(PPLAsyncQueryService.Status.SUCCEEDED, completed.status());
    assertEquals(2, completed.response().getResults().size());
    assertEquals(0, service.runningQueryCount());
    assertEquals(1, service.retainedJobCount());
    assertEquals(0, execution.closes.get());
  }

  @Test
  public void submitWaitPreventsExpiryAndLeaseStartsWhenIdIsReturned() {
    String id = service.create(OWNER, TimeValue.timeValueSeconds(1), null);
    AtomicReference<PPLAsyncQueryService.JobSnapshot> submit = new AtomicReference<>();
    service.registerSubmitWaiter(id, TimeValue.timeValueSeconds(5), listener(submit::set));

    now.addAndGet(TimeValue.timeValueSeconds(2).millis());
    service.reapExpired();

    assertNull(submit.get());
    assertEquals(1, service.runningQueryCount());
    assertEquals(1, service.retainedJobCount());

    timeoutTask.get().run();
    assertEquals(id, submit.get().id());
    assertEquals(PPLAsyncQueryService.Status.RUNNING, submit.get().status());

    now.addAndGet(TimeValue.timeValueSeconds(1).millis() + 1);
    service.reapExpired();
    assertEquals(0, service.runningQueryCount());
    assertEquals(0, service.retainedJobCount());
  }

  @Test
  public void fastFailureReturnsDirectFailureWithoutId() {
    String id = createJob(null);
    AtomicReference<PPLAsyncQueryService.JobSnapshot> result = new AtomicReference<>();
    service.registerSubmitWaiter(id, TimeValue.timeValueSeconds(5), listener(result::set));
    TrackingExecution execution = new TrackingExecution(response(1));
    service.attachExecution(id, execution);

    service.fail(id, new IllegalStateException("boom"));

    assertNull(result.get().id());
    assertEquals(PPLAsyncQueryService.Status.FAILED, result.get().status());
    assertEquals("IllegalStateException", result.get().failure().type());
    assertEquals("query execution failed", result.get().failure().reason());
    assertNull(result.get().response());
    assertEquals(0, execution.reads.get());
    assertEquals(1, execution.closes.get());
    assertEquals(0, service.retainedJobCount());
  }

  @Test
  public void getRenewsLeaseAndExpiryCancelsRunningTask() {
    CancellableTask task = mock(CancellableTask.class);
    when(task.isCancelled()).thenReturn(false);
    String id = createJob(task);
    service.registerSubmitWaiter(id, TimeValue.ZERO, listener(snapshot -> {}));
    TrackingExecution execution = new TrackingExecution(null);
    service.attachExecution(id, execution);

    now.addAndGet(TimeValue.timeValueMinutes(4).millis());
    PPLAsyncQueryService.JobSnapshot renewed = service.get(id, OWNER, null);
    assertEquals(PPLAsyncQueryService.Status.RUNNING, renewed.status());

    now.addAndGet(TimeValue.timeValueMinutes(4).millis());
    assertEquals(PPLAsyncQueryService.Status.RUNNING, service.get(id, OWNER, null).status());

    now.addAndGet(TimeValue.timeValueMinutes(5).millis() + 1);
    assertThrows(ResourceNotFoundException.class, () -> service.get(id, OWNER, null));
    verify(task).cancel("PPL asynchronous query expired");
    assertEquals(1, execution.closes.get());
    assertEquals(0, service.runningQueryCount());
    assertEquals(0, service.retainedJobCount());
  }

  @Test
  public void deleteCancelsRunningJobAndReleasesState() {
    CancellableTask task = mock(CancellableTask.class);
    when(task.isCancelled()).thenReturn(false);
    String id = createJob(task);
    service.registerSubmitWaiter(id, TimeValue.ZERO, listener(snapshot -> {}));
    TrackingExecution execution = new TrackingExecution(null);
    service.attachExecution(id, execution);

    PPLAsyncQueryService.DeleteResult result = service.delete(id, OWNER);

    assertEquals(id, result.id());
    assertEquals(PPLAsyncQueryService.Status.CANCELLED, result.status());
    verify(task).cancel("PPL asynchronous query cancelled by user");
    assertEquals(1, execution.closes.get());
    assertThrows(ResourceNotFoundException.class, () -> service.get(id, OWNER, null));
    assertEquals(0, service.runningQueryCount());
    assertEquals(0, service.retainedJobCount());
  }

  @Test
  public void deleteReturnsExistingTerminalStatus() {
    CancellableTask task = mock(CancellableTask.class);
    String id = createJob(task);
    service.registerSubmitWaiter(id, TimeValue.ZERO, listener(snapshot -> {}));
    TrackingExecution execution = new TrackingExecution(response(1));
    service.attachExecution(id, execution);
    service.complete(id);

    PPLAsyncQueryService.DeleteResult result = service.delete(id, OWNER);

    assertEquals(PPLAsyncQueryService.Status.SUCCEEDED, result.status());
    verify(task, never()).cancel(org.mockito.ArgumentMatchers.anyString());
    assertEquals(1, execution.closes.get());
    assertEquals(0, service.retainedJobCount());
  }

  @Test
  public void cancellationUsesTaskManagerWhenAttached() {
    TaskManager taskManager = mock(TaskManager.class);
    CancellableTask task = mock(CancellableTask.class);
    when(task.isCancelled()).thenReturn(false);
    service.attachTaskManager(taskManager);
    String id = createJob(task);
    service.registerSubmitWaiter(id, TimeValue.ZERO, listener(snapshot -> {}));

    service.delete(id, OWNER);

    verify(taskManager)
        .cancelTaskAndDescendants(
            org.mockito.ArgumentMatchers.eq(task),
            org.mockito.ArgumentMatchers.eq("PPL asynchronous query cancelled by user"),
            org.mockito.ArgumentMatchers.eq(false),
            org.mockito.ArgumentMatchers.any());
  }

  @Test
  public void submitRegistersTaskAndCompletionReleasesIt() {
    TaskManager taskManager = mock(TaskManager.class);
    PPLQueryTask task = mock(PPLQueryTask.class);
    TransportPPLQueryRequest request =
        new TransportPPLQueryRequest("source=t", new org.json.JSONObject(), "/_plugins/_ppl");
    SubmitTaskRegistration registration = registerSubmitTask(taskManager, request, task);
    service.attachTaskManager(taskManager);

    PPLAsyncQueryService.Submission submission =
        service.submit(
            OWNER,
            "5m",
            "0s",
            request,
            registration.submitTask(),
            listener(
                snapshot -> assertEquals(PPLAsyncQueryService.Status.RUNNING, snapshot.status())));
    TrackingExecution execution = new TrackingExecution(null);
    submission.start(ignored -> execution);
    execution.succeed(response(1));

    InOrder registrationOrder = inOrder(taskManager);
    registrationOrder
        .verify(taskManager)
        .registerChildNode(registration.submitTask().getId(), registration.localNode());
    registrationOrder.verify(taskManager).register("transport", PPLQueryAction.NAME, request);
    assertEquals(TaskId.EMPTY_TASK_ID, request.getParentTask());
    verify(taskManager).unregister(task);
    verify(registration.childNodeRegistration()).close();
    assertEquals(0, service.runningQueryCount());
    assertEquals(1, service.retainedJobCount());
  }

  @Test
  public void submissionStartFailureCompletesJobAndReleasesTask() {
    TaskManager taskManager = mock(TaskManager.class);
    PPLQueryTask task = mock(PPLQueryTask.class);
    TransportPPLQueryRequest request =
        new TransportPPLQueryRequest("source=t", new org.json.JSONObject(), "/_plugins/_ppl");
    SubmitTaskRegistration registration = registerSubmitTask(taskManager, request, task);
    service.attachTaskManager(taskManager);
    AtomicReference<PPLAsyncQueryService.JobSnapshot> response = new AtomicReference<>();

    PPLAsyncQueryService.Submission submission =
        service.submit(
            OWNER, "5m", "5s", request, registration.submitTask(), listener(response::set));
    submission.start(
        ignored -> {
          throw new IllegalStateException("execution did not start");
        });

    assertEquals(PPLAsyncQueryService.Status.FAILED, response.get().status());
    assertNull(response.get().id());
    verify(taskManager, times(1)).unregister(task);
    verify(registration.childNodeRegistration()).close();
    assertEquals(0, service.runningQueryCount());
    assertEquals(0, service.retainedJobCount());
  }

  @Test
  public void deleteCancelsAndReleasesServiceOwnedTask() {
    TaskManager taskManager = mock(TaskManager.class);
    PPLQueryTask task = mock(PPLQueryTask.class);
    when(task.isCancelled()).thenReturn(false);
    TransportPPLQueryRequest request =
        new TransportPPLQueryRequest("source=t", new org.json.JSONObject(), "/_plugins/_ppl");
    SubmitTaskRegistration registration = registerSubmitTask(taskManager, request, task);
    doAnswer(
            invocation -> {
              ActionListener<Void> listener = invocation.getArgument(3);
              listener.onResponse(null);
              return null;
            })
        .when(taskManager)
        .cancelTaskAndDescendants(
            org.mockito.ArgumentMatchers.eq(task),
            org.mockito.ArgumentMatchers.anyString(),
            org.mockito.ArgumentMatchers.eq(false),
            org.mockito.ArgumentMatchers.any());
    service.attachTaskManager(taskManager);
    AtomicReference<String> id = new AtomicReference<>();

    PPLAsyncQueryService.Submission submission =
        service.submit(
            OWNER,
            "5m",
            "0s",
            request,
            registration.submitTask(),
            listener(snapshot -> id.set(snapshot.id())));
    submission.start(ignored -> new TrackingExecution(null));

    service.delete(id.get(), OWNER);

    verify(taskManager)
        .cancelTaskAndDescendants(
            org.mockito.ArgumentMatchers.eq(task),
            org.mockito.ArgumentMatchers.eq("PPL asynchronous query cancelled by user"),
            org.mockito.ArgumentMatchers.eq(false),
            org.mockito.ArgumentMatchers.any());
    verify(taskManager, times(1)).unregister(task);
    verify(registration.childNodeRegistration()).close();
  }

  @Test
  public void submitFailureAfterJobCreationReleasesServiceOwnedTask() {
    PPLAsyncQueryService abortingService =
        new PPLAsyncQueryService(
            "node-a",
            now::get,
            (delay, task) -> {
              throw new IllegalStateException("scheduler unavailable");
            },
            () -> 20,
            () -> 100,
            () -> TimeValue.timeValueSeconds(60),
            () -> TimeValue.timeValueHours(24));
    TaskManager taskManager = mock(TaskManager.class);
    PPLQueryTask task = mock(PPLQueryTask.class);
    when(task.isCancelled()).thenReturn(true);
    TransportPPLQueryRequest request =
        new TransportPPLQueryRequest("source=t", new org.json.JSONObject(), "/_plugins/_ppl");
    SubmitTaskRegistration registration = registerSubmitTask(taskManager, request, task);
    abortingService.attachTaskManager(taskManager);

    IllegalStateException failure =
        assertThrows(
            IllegalStateException.class,
            () ->
                abortingService.submit(
                    OWNER,
                    "5m",
                    "5s",
                    request,
                    registration.submitTask(),
                    listener(snapshot -> {})));

    assertEquals("scheduler unavailable", failure.getMessage());
    verify(taskManager, times(1)).unregister(task);
    verify(registration.childNodeRegistration()).close();
    assertEquals(0, abortingService.runningQueryCount());
    assertEquals(0, abortingService.retainedJobCount());
  }

  @Test
  public void childTrackingFailurePreventsRetainedTaskRegistration() {
    TaskManager taskManager = mock(TaskManager.class);
    PPLQueryTask submitTask = mock(PPLQueryTask.class);
    DiscoveryNode localNode = mock(DiscoveryNode.class);
    TransportPPLQueryRequest request =
        new TransportPPLQueryRequest("source=t", new org.json.JSONObject(), "/_plugins/_ppl");
    when(submitTask.getId()).thenReturn(42L);
    when(taskManager.localNode()).thenReturn(localNode);
    when(taskManager.registerChildNode(42L, localNode))
        .thenThrow(new IllegalStateException("channel closed"));
    service.attachTaskManager(taskManager);

    assertThrows(
        IllegalStateException.class,
        () -> service.submit(OWNER, "5m", "5s", request, submitTask, listener(snapshot -> {})));

    verify(taskManager, never()).register("transport", PPLQueryAction.NAME, request);
    assertEquals(0, service.runningQueryCount());
    assertEquals(0, service.retainedJobCount());
  }

  @Test
  public void rejectsUnauthorizedCallerWithoutRenewingOrDeleting() {
    PPLAsyncQueryUser securedOwner =
        new PPLAsyncQueryUser(true, "alice", "tenant", List.of("role-a"));
    PPLAsyncQueryUser otherUser = new PPLAsyncQueryUser(true, "bob", "tenant", List.of("role-a"));
    String id = service.create(securedOwner, PPLAsyncQueryService.DEFAULT_KEEP_ALIVE, null);
    service.registerSubmitWaiter(id, TimeValue.ZERO, listener(snapshot -> {}));

    assertThrows(OpenSearchSecurityException.class, () -> service.get(id, otherUser, null));
    assertThrows(OpenSearchSecurityException.class, () -> service.delete(id, otherUser));
    assertEquals(PPLAsyncQueryService.Status.RUNNING, service.get(id, securedOwner, null).status());
  }

  @Test
  public void enforcesRunningAndRetainedCapacity() {
    PPLAsyncQueryService limited = service(1, 1);
    limited.create(OWNER, PPLAsyncQueryService.DEFAULT_KEEP_ALIVE, null);

    OpenSearchStatusException exception =
        assertThrows(
            OpenSearchStatusException.class,
            () -> limited.create(OWNER, PPLAsyncQueryService.DEFAULT_KEEP_ALIVE, null));

    assertEquals(429, exception.status().getStatus());
  }

  @Test
  public void finalSnapshotDefensivelyCopiesRows() {
    String id = createJob(null);
    AtomicReference<PPLAsyncQueryService.JobSnapshot> result = new AtomicReference<>();
    service.registerSubmitWaiter(id, TimeValue.timeValueSeconds(5), listener(result::set));
    List<org.opensearch.sql.data.model.ExprValue> rows = new ArrayList<>();
    rows.add(ExprValueUtils.stringValue("first"));
    QueryResponse response =
        new QueryResponse(
            new Schema(List.of(new Column("state", null, ExprCoreType.STRING))), rows, null);

    service.attachExecution(id, new TrackingExecution(response));
    service.complete(id);
    rows.add(ExprValueUtils.stringValue("second"));

    assertEquals(1, result.get().response().getResults().size());
  }

  @Test
  public void completedExecutionCanBeAttachedBeforeCompletionIsObserved() {
    String id = createJob(null);
    AtomicReference<PPLAsyncQueryService.JobSnapshot> result = new AtomicReference<>();
    service.registerSubmitWaiter(id, TimeValue.timeValueSeconds(5), listener(result::set));
    TrackingExecution execution = new TrackingExecution(null);

    execution.succeed(response(2));
    service.attachExecution(id, execution);

    assertEquals(PPLAsyncQueryService.Status.SUCCEEDED, result.get().status());
    assertEquals(2, result.get().response().getResults().size());
    assertEquals(1, execution.closes.get());
  }

  @Test
  public void runningGetMaterializesCurrentResultOutsideJob() {
    String id = createJob(null);
    service.registerSubmitWaiter(id, TimeValue.ZERO, listener(snapshot -> {}));
    TrackingExecution execution = new TrackingExecution(response(1));
    service.attachExecution(id, execution);

    PPLAsyncQueryService.JobSnapshot first = service.get(id, OWNER, null);
    execution.setCurrent(response(3));
    PPLAsyncQueryService.JobSnapshot second = service.get(id, OWNER, null);

    assertEquals(PPLAsyncQueryService.Status.RUNNING, first.status());
    assertEquals(1, first.response().getResults().size());
    assertEquals(3, second.response().getResults().size());
  }

  @Test
  public void failedRetainedJobReturnsNoProvisionalRowsAndClosesExecution() {
    String id = createJob(null);
    service.registerSubmitWaiter(id, TimeValue.ZERO, listener(snapshot -> {}));
    TrackingExecution execution = new TrackingExecution(response(1));
    service.attachExecution(id, execution);

    service.fail(id, new IllegalStateException("boom"));
    PPLAsyncQueryService.JobSnapshot failed = service.get(id, OWNER, null);

    assertEquals(PPLAsyncQueryService.Status.FAILED, failed.status());
    assertNull(failed.response());
    assertEquals(0, execution.reads.get());
    assertEquals(1, execution.closes.get());
  }

  @Test
  public void deleteBeforeExecutionAttachmentClosesLateHandle() {
    String id = createJob(null);
    service.registerSubmitWaiter(id, TimeValue.ZERO, listener(snapshot -> {}));
    service.delete(id, OWNER);
    TrackingExecution execution = new TrackingExecution(response(1));

    service.attachExecution(id, execution);

    assertEquals(1, execution.closes.get());
    assertThrows(ResourceNotFoundException.class, () -> service.get(id, OWNER, null));
  }

  @Test
  public void concurrentGetDoesNotBlockDeleteOnResultMaterialization() throws Exception {
    String id = createJob(null);
    service.registerSubmitWaiter(id, TimeValue.ZERO, listener(snapshot -> {}));
    BlockingExecution execution = new BlockingExecution(response(1));
    service.attachExecution(id, execution);

    CompletableFuture<PPLAsyncQueryService.JobSnapshot> get =
        CompletableFuture.supplyAsync(() -> service.get(id, OWNER, null));
    assertTrue(execution.readStarted.await(5, TimeUnit.SECONDS));
    CompletableFuture<PPLAsyncQueryService.DeleteResult> delete =
        CompletableFuture.supplyAsync(() -> service.delete(id, OWNER));

    try {
      assertEquals(PPLAsyncQueryService.Status.CANCELLED, delete.get(5, TimeUnit.SECONDS).status());
    } finally {
      execution.allowRead.countDown();
    }

    assertEquals(PPLAsyncQueryService.Status.RUNNING, get.get(5, TimeUnit.SECONDS).status());
    assertEquals(1, execution.closes.get());
    assertThrows(ResourceNotFoundException.class, () -> service.get(id, OWNER, null));
  }

  @Test
  public void shutdownClosesRetainedExecutionExactlyOnce() throws Exception {
    String id = createJob(null);
    service.registerSubmitWaiter(id, TimeValue.ZERO, listener(snapshot -> {}));
    TrackingExecution execution = new TrackingExecution(response(1));
    service.attachExecution(id, execution);

    service.close();
    service.close();

    assertEquals(1, execution.closes.get());
    assertEquals(0, service.runningQueryCount());
    assertEquals(0, service.retainedJobCount());
  }

  @Test
  public void submissionAbortClosesAttachedExecution() {
    PPLAsyncQueryService abortingService =
        new PPLAsyncQueryService(
            "node-a",
            now::get,
            (delay, task) -> {
              throw new IllegalStateException("scheduler unavailable");
            },
            () -> 20,
            () -> 100,
            () -> TimeValue.timeValueSeconds(60),
            () -> TimeValue.timeValueHours(24));
    String id = abortingService.create(OWNER, PPLAsyncQueryService.DEFAULT_KEEP_ALIVE, null);
    TrackingExecution execution = new TrackingExecution(null);
    abortingService.attachExecution(id, execution);

    IllegalStateException failure =
        assertThrows(
            IllegalStateException.class,
            () ->
                abortingService.registerSubmitWaiter(
                    id, TimeValue.timeValueSeconds(5), listener(snapshot -> {})));

    assertEquals("scheduler unavailable", failure.getMessage());
    assertEquals(1, execution.closes.get());
    assertEquals(0, abortingService.runningQueryCount());
    assertEquals(0, abortingService.retainedJobCount());
  }

  @Test
  public void successfulCompletionRequiresFinalResultToBeVisible() {
    String id = createJob(null);
    AtomicReference<Exception> failure = new AtomicReference<>();
    service.registerSubmitWaiter(
        id, TimeValue.timeValueSeconds(5), ActionListener.wrap(snapshot -> {}, failure::set));
    TrackingExecution execution = new TrackingExecution(null);
    service.attachExecution(id, execution);

    service.complete(id);

    assertTrue(failure.get() instanceof IllegalStateException);
    assertEquals(1, execution.closes.get());
    assertEquals(0, service.retainedJobCount());
  }

  @Test
  public void submitMaterializationFailureAbortsUndeliverableRetainedJob() {
    CancellableTask task = mock(CancellableTask.class);
    when(task.isCancelled()).thenReturn(false);
    String id = createJob(task);
    AtomicReference<Exception> failure = new AtomicReference<>();
    service.registerSubmitWaiter(
        id, TimeValue.timeValueSeconds(5), ActionListener.wrap(snapshot -> {}, failure::set));
    ThrowingExecution execution = new ThrowingExecution();
    service.attachExecution(id, execution);

    timeoutTask.get().run();

    assertTrue(failure.get() instanceof IllegalStateException);
    verify(task).cancel("PPL asynchronous query submission failed");
    assertEquals(1, execution.closes.get());
    assertEquals(0, service.runningQueryCount());
    assertEquals(0, service.retainedJobCount());
    assertThrows(ResourceNotFoundException.class, () -> service.get(id, OWNER, null));
  }

  @Test
  public void validatesDurationBounds() {
    assertThrows(IllegalArgumentException.class, () -> service.validateKeepAlive(TimeValue.ZERO));
    assertThrows(
        IllegalArgumentException.class,
        () -> service.validateKeepAlive(TimeValue.timeValueHours(25)));
    assertThrows(
        IllegalArgumentException.class,
        () -> service.validateWaitForCompletion(TimeValue.timeValueSeconds(61)));

    service.validateWaitForCompletion(TimeValue.ZERO);
    service.validateWaitForCompletion(TimeValue.timeValueSeconds(60));
    service.validateKeepAlive(TimeValue.timeValueHours(24));
  }

  private PPLAsyncQueryService service(int maxRunning, int maxRetained) {
    return new PPLAsyncQueryService(
        "node-a",
        now::get,
        (delay, task) -> {
          timeoutTask.set(task);
          AtomicReference<Boolean> cancelled = new AtomicReference<>(false);
          return () -> cancelled.set(true);
        },
        () -> maxRunning,
        () -> maxRetained,
        () -> TimeValue.timeValueSeconds(60),
        () -> TimeValue.timeValueHours(24));
  }

  private String createJob(CancellableTask task) {
    return service.create(OWNER, PPLAsyncQueryService.DEFAULT_KEEP_ALIVE, task);
  }

  private static SubmitTaskRegistration registerSubmitTask(
      TaskManager taskManager, TransportPPLQueryRequest request, PPLQueryTask retainedTask) {
    PPLQueryTask submitTask = mock(PPLQueryTask.class);
    DiscoveryNode localNode = mock(DiscoveryNode.class);
    Releasable childNodeRegistration = mock(Releasable.class);
    when(submitTask.getId()).thenReturn(42L);
    when(localNode.getId()).thenReturn("node-a");
    when(taskManager.localNode()).thenReturn(localNode);
    when(taskManager.registerChildNode(42L, localNode)).thenReturn(childNodeRegistration);
    when(taskManager.register("transport", PPLQueryAction.NAME, request))
        .thenAnswer(
            invocation -> {
              assertEquals(new TaskId("node-a", 42L), request.getParentTask());
              return retainedTask;
            });
    return new SubmitTaskRegistration(submitTask, localNode, childNodeRegistration);
  }

  private record SubmitTaskRegistration(
      PPLQueryTask submitTask, DiscoveryNode localNode, Releasable childNodeRegistration) {}

  private static ActionListener<PPLAsyncQueryService.JobSnapshot> listener(
      java.util.function.Consumer<PPLAsyncQueryService.JobSnapshot> consumer) {
    return ActionListener.wrap(
        snapshot -> consumer.accept(snapshot),
        failure -> {
          throw new AssertionError(failure);
        });
  }

  private static QueryResponse response(int rowCount) {
    Schema schema = new Schema(List.of(new Column("state", null, ExprCoreType.STRING)));
    return new QueryResponse(
        schema,
        java.util.stream.IntStream.range(0, rowCount)
            .mapToObj(i -> ExprValueUtils.stringValue("state-" + i))
            .toList(),
        null);
  }

  private static final class TrackingExecution implements AsyncQueryExecution {
    private final AtomicReference<QueryResponse> current;
    private final CompletableFuture<Void> completion = new CompletableFuture<>();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicInteger reads = new AtomicInteger();
    private final AtomicInteger closes = new AtomicInteger();

    private TrackingExecution(QueryResponse current) {
      this.current = new AtomicReference<>(current);
    }

    private void setCurrent(QueryResponse response) {
      current.set(response);
    }

    private void succeed(QueryResponse response) {
      current.set(response);
      completion.complete(null);
    }

    @Override
    public Optional<QueryResponse> currentResult() {
      reads.incrementAndGet();
      return Optional.ofNullable(current.get());
    }

    @Override
    public CompletionStage<Void> completion() {
      return completion;
    }

    @Override
    public void close() {
      if (closed.compareAndSet(false, true)) {
        closes.incrementAndGet();
      }
    }
  }

  private static final class BlockingExecution implements AsyncQueryExecution {
    private final QueryResponse response;
    private final CountDownLatch readStarted = new CountDownLatch(1);
    private final CountDownLatch allowRead = new CountDownLatch(1);
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicInteger closes = new AtomicInteger();

    private BlockingExecution(QueryResponse response) {
      this.response = response;
    }

    @Override
    public Optional<QueryResponse> currentResult() {
      readStarted.countDown();
      try {
        assertTrue(allowRead.await(5, TimeUnit.SECONDS));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new AssertionError(e);
      }
      return Optional.of(response);
    }

    @Override
    public CompletionStage<Void> completion() {
      return new CompletableFuture<>();
    }

    @Override
    public void close() {
      if (closed.compareAndSet(false, true)) {
        closes.incrementAndGet();
      }
    }
  }

  private static final class ThrowingExecution implements AsyncQueryExecution {
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicInteger closes = new AtomicInteger();

    @Override
    public Optional<QueryResponse> currentResult() {
      throw new IllegalStateException("materialization failed");
    }

    @Override
    public CompletionStage<Void> completion() {
      return new CompletableFuture<>();
    }

    @Override
    public void close() {
      if (closed.compareAndSet(false, true)) {
        closes.incrementAndGet();
      }
    }
  }
}
