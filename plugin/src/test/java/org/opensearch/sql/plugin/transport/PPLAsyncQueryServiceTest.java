/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Test;
import org.opensearch.OpenSearchSecurityException;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.ResourceNotFoundException;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.executor.ExecutionEngine.QueryResponse;
import org.opensearch.sql.executor.ExecutionEngine.Schema;
import org.opensearch.sql.executor.ExecutionEngine.Schema.Column;
import org.opensearch.sql.executor.ProgressiveQueryExecution;
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
    TrackingExecution execution = new TrackingExecution(response(2));

    service.awaitSubmit(
        id,
        TimeValue.timeValueSeconds(5),
        listener(
            snapshot -> {
              result.set(snapshot);
              responses.incrementAndGet();
            }));
    now.addAndGet(25);
    service.attachExecution(id, execution);
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
    service.awaitSubmit(id, TimeValue.timeValueSeconds(5), listener(submit::set));

    timeoutTask.get().run();

    assertEquals(id, submit.get().id());
    assertEquals(PPLAsyncQueryService.Status.RUNNING, submit.get().status());
    assertNull(submit.get().response());
    assertEquals(1, service.runningQueryCount());
    assertEquals(1, service.retainedJobCount());

    now.addAndGet(25);
    service.attachExecution(id, execution(response(2)));
    service.complete(id);
    PPLAsyncQueryService.JobSnapshot completed = service.get(id, OWNER, null);

    assertEquals(id, completed.id());
    assertEquals(PPLAsyncQueryService.Status.SUCCEEDED, completed.status());
    assertEquals(2, completed.response().getResults().size());
    assertEquals(0, service.runningQueryCount());
    assertEquals(1, service.retainedJobCount());
  }

  @Test
  public void submitWaitPreventsExpiryAndLeaseStartsWhenIdIsReturned() {
    String id = service.create(OWNER, TimeValue.timeValueSeconds(1), null);
    AtomicReference<PPLAsyncQueryService.JobSnapshot> submit = new AtomicReference<>();
    service.awaitSubmit(id, TimeValue.timeValueSeconds(5), listener(submit::set));

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
    service.awaitSubmit(id, TimeValue.timeValueSeconds(5), listener(result::set));

    service.fail(id, new IllegalStateException("boom"));

    assertNull(result.get().id());
    assertEquals(PPLAsyncQueryService.Status.FAILED, result.get().status());
    assertEquals("IllegalStateException", result.get().failure().type());
    assertEquals("query execution failed", result.get().failure().reason());
    assertEquals(0, service.retainedJobCount());
  }

  @Test
  public void getRenewsLeaseAndExpiryCancelsRunningTask() {
    CancellableTask task = mock(CancellableTask.class);
    when(task.isCancelled()).thenReturn(false);
    String id = createJob(task);
    service.awaitSubmit(id, TimeValue.ZERO, listener(snapshot -> {}));

    now.addAndGet(TimeValue.timeValueMinutes(4).millis());
    PPLAsyncQueryService.JobSnapshot renewed = service.get(id, OWNER, null);
    assertEquals(PPLAsyncQueryService.Status.RUNNING, renewed.status());

    now.addAndGet(TimeValue.timeValueMinutes(4).millis());
    assertEquals(PPLAsyncQueryService.Status.RUNNING, service.get(id, OWNER, null).status());

    now.addAndGet(TimeValue.timeValueMinutes(5).millis() + 1);
    assertThrows(ResourceNotFoundException.class, () -> service.get(id, OWNER, null));
    verify(task).cancel("PPL asynchronous query expired");
    assertEquals(0, service.runningQueryCount());
    assertEquals(0, service.retainedJobCount());
  }

  @Test
  public void deleteCancelsRunningJobAndReleasesState() {
    CancellableTask task = mock(CancellableTask.class);
    when(task.isCancelled()).thenReturn(false);
    String id = createJob(task);
    service.awaitSubmit(id, TimeValue.ZERO, listener(snapshot -> {}));

    PPLAsyncQueryService.DeleteResult result = service.delete(id, OWNER);

    assertEquals(id, result.id());
    assertEquals(PPLAsyncQueryService.Status.CANCELLED, result.status());
    verify(task).cancel("PPL asynchronous query cancelled by user");
    assertThrows(ResourceNotFoundException.class, () -> service.get(id, OWNER, null));
    assertEquals(0, service.runningQueryCount());
    assertEquals(0, service.retainedJobCount());
  }

  @Test
  public void deleteReturnsExistingTerminalStatus() {
    CancellableTask task = mock(CancellableTask.class);
    String id = createJob(task);
    service.awaitSubmit(id, TimeValue.ZERO, listener(snapshot -> {}));
    service.attachExecution(id, execution(response(1)));
    service.complete(id);

    PPLAsyncQueryService.DeleteResult result = service.delete(id, OWNER);

    assertEquals(PPLAsyncQueryService.Status.SUCCEEDED, result.status());
    verify(task, never()).cancel(org.mockito.ArgumentMatchers.anyString());
    assertEquals(0, service.retainedJobCount());
  }

  @Test
  public void cancellationUsesTaskManagerWhenAttached() {
    TaskManager taskManager = mock(TaskManager.class);
    CancellableTask task = mock(CancellableTask.class);
    when(task.isCancelled()).thenReturn(false);
    service.attachTaskManager(taskManager);
    String id = createJob(task);
    service.awaitSubmit(id, TimeValue.ZERO, listener(snapshot -> {}));

    service.delete(id, OWNER);

    verify(taskManager)
        .cancelTaskAndDescendants(
            org.mockito.ArgumentMatchers.eq(task),
            org.mockito.ArgumentMatchers.eq("PPL asynchronous query cancelled by user"),
            org.mockito.ArgumentMatchers.eq(false),
            org.mockito.ArgumentMatchers.any());
  }

  @Test
  public void rejectsUnauthorizedCallerWithoutRenewingOrDeleting() {
    PPLAsyncQueryUser securedOwner =
        new PPLAsyncQueryUser(true, "alice", "tenant", List.of("role-a"));
    PPLAsyncQueryUser otherUser = new PPLAsyncQueryUser(true, "bob", "tenant", List.of("role-a"));
    String id = service.create(securedOwner, PPLAsyncQueryService.DEFAULT_KEEP_ALIVE, null);
    service.awaitSubmit(id, TimeValue.ZERO, listener(snapshot -> {}));

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
    service.awaitSubmit(id, TimeValue.timeValueSeconds(5), listener(result::set));
    List<org.opensearch.sql.data.model.ExprValue> rows = new ArrayList<>();
    rows.add(ExprValueUtils.stringValue("first"));
    QueryResponse response =
        new QueryResponse(
            new Schema(List.of(new Column("state", null, ExprCoreType.STRING))), rows, null);

    service.attachExecution(id, execution(response));
    service.complete(id);
    rows.add(ExprValueUtils.stringValue("second"));

    assertEquals(1, result.get().response().getResults().size());
  }

  @Test
  public void runningSnapshotReadsTheCurrentContextResult() {
    String id = createJob(null);
    AtomicReference<QueryResponse> current = new AtomicReference<>(response(1));
    service.attachExecution(id, execution(current));
    AtomicReference<PPLAsyncQueryService.JobSnapshot> submit = new AtomicReference<>();
    service.awaitSubmit(id, TimeValue.ZERO, listener(submit::set));

    assertEquals(PPLAsyncQueryService.Status.RUNNING, submit.get().status());
    assertEquals(1, submit.get().response().getResults().size());

    current.set(response(3));
    PPLAsyncQueryService.JobSnapshot polled = service.get(id, OWNER, null);
    assertEquals(PPLAsyncQueryService.Status.RUNNING, polled.status());
    assertEquals(3, polled.response().getResults().size());
  }

  @Test
  public void retainedJobOwnsContextUntilDelete() {
    String id = createJob(null);
    service.awaitSubmit(id, TimeValue.ZERO, listener(snapshot -> {}));
    TrackingExecution execution = new TrackingExecution(response(2));

    service.attachExecution(id, execution);
    service.complete(id);

    assertEquals(0, execution.closes.get());
    assertEquals(2, service.get(id, OWNER, null).response().getResults().size());
    assertEquals(1, execution.reads.get());

    service.delete(id, OWNER);

    assertEquals(1, execution.closes.get());
  }

  @Test
  public void contextRejectedAfterJobRemovalIsClosedByService() {
    String id = createJob(null);
    service.awaitSubmit(id, TimeValue.ZERO, listener(snapshot -> {}));
    service.delete(id, OWNER);
    TrackingExecution execution = new TrackingExecution(response(1));

    service.attachExecution(id, execution);

    assertEquals(0, execution.reads.get());
    assertEquals(1, execution.closes.get());
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

  private static ProgressiveQueryExecution execution(QueryResponse response) {
    return execution(new AtomicReference<>(response));
  }

  private static ProgressiveQueryExecution execution(AtomicReference<QueryResponse> response) {
    return new ProgressiveQueryExecution() {
      @Override
      public Optional<QueryResponse> currentResult() {
        return Optional.of(response.get());
      }

      @Override
      public CompletionStage<Void> completion() {
        return CompletableFuture.completedFuture(null);
      }

      @Override
      public void close() {}
    };
  }

  private static final class TrackingExecution implements ProgressiveQueryExecution {
    private final QueryResponse response;
    private final AtomicInteger reads = new AtomicInteger();
    private final AtomicInteger closes = new AtomicInteger();

    private TrackingExecution(QueryResponse response) {
      this.response = response;
    }

    @Override
    public Optional<QueryResponse> currentResult() {
      reads.incrementAndGet();
      return Optional.of(response);
    }

    @Override
    public CompletionStage<Void> completion() {
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public void close() {
      closes.incrementAndGet();
    }
  }
}
