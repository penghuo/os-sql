/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.job;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.executor.pagination.Cursor;
import org.opensearch.sql.job.exceptions.QueryJobNotFoundException;
import org.opensearch.threadpool.ThreadPool;

/** Capacity enforcement end to end through {@link OpenSearchQueryJobService}. */
class AccountingQueryJobStoreTest {

  private static final Principal ALICE = new Principal("alice", null, List.of());
  private static final Duration KEEP_ALIVE = Duration.ofMinutes(5);
  private static final QueryResult.Rows ROWS =
      new QueryResult.Rows(
          new ExecutionEngine.Schema(List.of()),
          List.of(ExprValueUtils.tupleValue(Map.of("host", "host-1"))),
          Cursor.None,
          List.of(),
          0);
  private static final long ROWS_BYTES = QueryResultSizeEstimator.estimate(ROWS);

  private QueryJobQuotaTest.MutableSettings settings;
  private QueryJobQuota quota;
  private InMemoryQueryJobStore delegate;
  private AccountingQueryJobStore store;
  private ThreadPool threadPool;
  private OpenSearchQueryJobScheduler scheduler;
  private OpenSearchQueryJobService service;

  @BeforeEach
  void setUp() {
    settings = QueryJobQuotaTest.settings(2, 3, 10 * ROWS_BYTES);
    quota = new QueryJobQuota(settings);
    delegate = new InMemoryQueryJobStore();
    store = new AccountingQueryJobStore(delegate, quota);
    threadPool = mock(ThreadPool.class);
    ClusterService clusterService = mock(ClusterService.class);
    DiscoveryNode localNode = mock(DiscoveryNode.class);
    when(localNode.getId()).thenReturn("node-a");
    when(clusterService.localNode()).thenReturn(localNode);
    scheduler = new OpenSearchQueryJobScheduler(settings, threadPool);
    service =
        new OpenSearchQueryJobService(
            store,
            clusterService,
            Clock.systemUTC(),
            new RetentionPolicy(store, threadPool),
            null,
            quota,
            scheduler);
  }

  @Test
  void submitOverRunningLimitWaitsAsPendingUntilASlotFrees() {
    Runner first = new Runner();
    submitRunning(first);
    submitRunning(new Runner());
    Runner queued = new Runner();

    QueryResult.Running pending =
        assertInstanceOf(
            QueryResult.Running.class,
            service.submit(queued, ALICE, Duration.ZERO, KEEP_ALIVE).toCompletableFuture().join());

    assertEquals(QueryJobState.PENDING, pending.state());
    assertEquals(QueryJobState.PENDING, service.get(pending.id(), ALICE).state());
    assertFalse(queued.ran);
    assertEquals(1, scheduler.queued());

    first.future.complete(ROWS);

    assertTrue(queued.ran);
    assertEquals(QueryJobState.RUNNING, service.get(pending.id(), ALICE).state());
    assertEquals(2, scheduler.running());
    assertEquals(0, scheduler.queued());
  }

  @Test
  void queuedJobsCountTowardTheRetainedJobLimit() {
    submitRunning(new Runner());
    submitRunning(new Runner());
    submitRunning(new Runner());

    OpenSearchStatusException e =
        assertThrows(
            OpenSearchStatusException.class,
            () -> service.submit(new Runner(), ALICE, Duration.ZERO, KEEP_ALIVE));

    assertEquals(RestStatus.TOO_MANY_REQUESTS, e.status());
    assertEquals(1, scheduler.queued());
    assertEquals(3, quota.jobs());
  }

  @Test
  void retainedJobsHoldTheirSlotUntilDeleted() {
    QueryResult.Running first = completeRetained();
    completeRetained();
    completeRetained();
    assertThrows(
        OpenSearchStatusException.class,
        () -> service.submit(new Runner(), ALICE, Duration.ZERO, KEEP_ALIVE));

    service.delete(first.id(), ALICE);

    submitRunning(new Runner());
    assertEquals(3, quota.jobs());
  }

  @Test
  void inlineResultIsNeverChargedAndFreesItsSlots() {
    Runner runner = new Runner();
    runner.future.complete(ROWS);

    QueryResult result =
        service
            .submit(runner, ALICE, Duration.ofSeconds(5), KEEP_ALIVE)
            .toCompletableFuture()
            .join();

    assertInstanceOf(QueryResult.Rows.class, result);
    assertEquals(0, scheduler.running());
    assertEquals(0, quota.jobs());
    assertEquals(0L, quota.resultBytes());
  }

  @Test
  void retainedResultIsChargedUntilDeleted() {
    QueryResult.Running running = completeRetained();
    assertEquals(ROWS_BYTES, quota.resultBytes());
    assertEquals(QueryJobState.SUCCEEDED, service.get(running.id(), ALICE).state());

    service.delete(running.id(), ALICE);

    assertEquals(0L, quota.resultBytes());
    assertEquals(0, quota.jobs());
  }

  @Test
  void retainedResultIsReleasedOnRetentionEviction() {
    completeRetained();
    ArgumentCaptor<Runnable> eviction = ArgumentCaptor.forClass(Runnable.class);
    verify(threadPool).schedule(eviction.capture(), any(), eq(ThreadPool.Names.GENERIC));

    eviction.getValue().run();

    assertEquals(0L, quota.resultBytes());
    assertEquals(0, quota.jobs());
    assertTrue(delegate.jobs().isEmpty());
  }

  @Test
  void resultOverBudgetFailsTheJobWithoutRetainingRows() {
    settings.put(
        org.opensearch.sql.common.setting.Settings.Key.PPL_ASYNC_MAX_RETAINED_RESULT_SIZE,
        org.opensearch.core.common.unit.ByteSizeValue.parseBytesSizeValue(
            (ROWS_BYTES - 1) + "b", "test"));

    QueryResult.Running running = completeRetained();

    QueryJobStatus status = service.get(running.id(), ALICE);
    assertEquals(QueryJobState.FAILED, status.state());
    assertTrue(status.result().isEmpty());
    assertEquals("OpenSearchStatusException", status.failure().orElseThrow().type());
    assertEquals(0L, quota.resultBytes());
    assertEquals(0, scheduler.running());
  }

  @Test
  void exhaustedResultBudgetRejectsTheNextSubmit() {
    settings.put(
        org.opensearch.sql.common.setting.Settings.Key.PPL_ASYNC_MAX_RETAINED_RESULT_SIZE,
        org.opensearch.core.common.unit.ByteSizeValue.parseBytesSizeValue(
            ROWS_BYTES + "b", "test"));
    QueryResult.Running running = completeRetained();

    assertThrows(
        OpenSearchStatusException.class,
        () -> service.submit(new Runner(), ALICE, Duration.ZERO, KEEP_ALIVE));

    service.delete(running.id(), ALICE);
    submitRunning(new Runner());
  }

  @Test
  void removeReleasesOnlyOnce() {
    QueryResult.Running running = completeRetained();
    QueryJob job = delegate.find(running.id()).orElseThrow();

    assertTrue(store.remove(running.id(), job));
    assertFalse(store.remove(running.id(), job));

    assertEquals(0, quota.jobs());
    assertEquals(0L, quota.resultBytes());
    assertThrows(QueryJobNotFoundException.class, () -> service.get(running.id(), ALICE));
  }

  @Test
  void duplicateIdRollsBackTheAdmission() {
    QueryJob job =
        new QueryJob(new QueryJobId("node-a", "ctx"), ALICE, new Runner(), Clock.systemUTC());
    QueryJob duplicate =
        new QueryJob(new QueryJobId("node-a", "ctx"), ALICE, new Runner(), Clock.systemUTC());
    assertNull(store.register(job));

    assertEquals(job, store.register(duplicate));

    assertEquals(1, quota.jobs());
  }

  @Test
  void closedDelegateRollsBackTheAdmission() {
    store.close();
    QueryJob job =
        new QueryJob(new QueryJobId("node-a", "ctx"), ALICE, new Runner(), Clock.systemUTC());

    assertThrows(IllegalStateException.class, () -> store.register(job));

    assertEquals(0, scheduler.running());
    assertEquals(0, quota.jobs());
  }

  @Test
  void closeCancelsAndReleasesEveryJob() {
    Runner runner = new Runner();
    submitRunning(runner);
    completeRetained();

    store.close();

    assertEquals(1, runner.cancels);
    assertEquals(0, scheduler.running());
    assertEquals(0, quota.jobs());
    assertEquals(0L, quota.resultBytes());
  }

  private QueryResult.Running completeRetained() {
    Runner runner = new Runner();
    QueryResult.Running running = submitRunning(runner);
    runner.future.complete(ROWS);
    return running;
  }

  private QueryResult.Running submitRunning(Runner runner) {
    CompletionStage<QueryResult> stage = service.submit(runner, ALICE, Duration.ZERO, KEEP_ALIVE);
    return assertInstanceOf(QueryResult.Running.class, stage.toCompletableFuture().join());
  }

  private static final class Runner implements QueryRunner {
    private final CompletableFuture<QueryResult> future = new CompletableFuture<>();
    private boolean ran;
    private int cancels;

    @Override
    public CompletionStage<QueryResult> run() {
      ran = true;
      return future;
    }

    @Override
    public void cancel() {
      cancels++;
    }
  }
}
