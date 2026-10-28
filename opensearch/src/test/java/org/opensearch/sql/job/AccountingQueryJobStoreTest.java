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
import java.util.HashMap;
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
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.sql.common.setting.Settings;
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

  private MutableSettings settings;
  private InMemoryQueryJobStore delegate;
  private AccountingQueryJobStore store;
  private ThreadPool threadPool;
  private OpenSearchQueryJobScheduler scheduler;
  private OpenSearchQueryJobService service;

  @BeforeEach
  void setUp() {
    settings = settings(2, 3);
    delegate = new InMemoryQueryJobStore();
    store = new AccountingQueryJobStore(delegate, settings);
    threadPool = mock(ThreadPool.class);
    when(threadPool.getThreadContext())
        .thenReturn(
            new org.opensearch.common.util.concurrent.ThreadContext(
                org.opensearch.common.settings.Settings.EMPTY));
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
  void fullQueueRejectsTheSubmitAndDropsTheJob() {
    settings.put(Settings.Key.PPL_ASYNC_MAX_QUEUED_QUERIES, 1);
    submitRunning(new Runner());
    submitRunning(new Runner());
    submitRunning(new Runner());
    Runner rejected = new Runner();

    OpenSearchStatusException e =
        assertThrows(
            OpenSearchStatusException.class,
            () -> service.submit(rejected, ALICE, Duration.ZERO, KEEP_ALIVE));

    assertEquals(RestStatus.TOO_MANY_REQUESTS, e.status());
    assertEquals(
        "Async query queue on this node is full [plugins.ppl.async.max_queued_queries]; retry"
            + " later",
        e.getMessage());
    assertFalse(rejected.ran);
    assertEquals(1, scheduler.queued());
    assertEquals(3, delegate.jobs().size());
  }

  @Test
  void runningAndQueuedJobsDoNotCountAsRetained() {
    for (int i = 0; i < 5; i++) {
      submitRunning(new Runner());
    }

    assertEquals(2, scheduler.running());
    assertEquals(3, scheduler.queued());
    assertEquals(0, store.finished());
  }

  @Test
  void retainedFinishedJobsHoldTheirSlotUntilDeleted() {
    QueryResult.Running first = completeRetained();
    completeRetained();
    completeRetained();
    assertEquals(3, store.finished());
    OpenSearchStatusException e =
        assertThrows(
            OpenSearchStatusException.class,
            () -> service.submit(new Runner(), ALICE, Duration.ZERO, KEEP_ALIVE));
    assertEquals(RestStatus.TOO_MANY_REQUESTS, e.status());
    assertEquals(
        "Async query capacity on this node is exhausted"
            + " [plugins.ppl.async.max_retained_jobs]; retry later",
        e.getMessage());

    service.delete(first.id(), ALICE);

    submitRunning(new Runner());
    assertEquals(2, store.finished());
  }

  @Test
  void loweredLimitAppliesToTheNextSubmit() {
    completeRetained();
    settings.put(Settings.Key.PPL_ASYNC_MAX_RETAINED_JOBS, 1);

    assertThrows(
        OpenSearchStatusException.class,
        () -> service.submit(new Runner(), ALICE, Duration.ZERO, KEEP_ALIVE));
  }

  @Test
  void inlineResultIsNeverCountedAndFreesItsSlot() {
    Runner runner = new Runner();
    runner.future.complete(ROWS);

    QueryResult result =
        service
            .submit(runner, ALICE, Duration.ofSeconds(5), KEEP_ALIVE)
            .toCompletableFuture()
            .join();

    assertInstanceOf(QueryResult.Rows.class, result);
    assertEquals(0, scheduler.running());
    assertEquals(0, store.finished());
  }

  @Test
  void jobDeletedBeforeItFinishesIsNotCounted() {
    QueryResult.Running running = submitRunning(new Runner());

    service.delete(running.id(), ALICE);

    assertEquals(0, store.finished());
  }

  @Test
  void retainedJobIsReleasedOnRetentionEviction() {
    completeRetained();
    ArgumentCaptor<Runnable> eviction = ArgumentCaptor.forClass(Runnable.class);
    verify(threadPool).schedule(eviction.capture(), any(), eq(ThreadPool.Names.GENERIC));

    eviction.getValue().run();

    assertEquals(0, store.finished());
    assertTrue(delegate.jobs().isEmpty());
  }

  @Test
  void removeReleasesOnlyOnce() {
    QueryResult.Running running = completeRetained();
    QueryJob job = delegate.find(running.id()).orElseThrow();

    assertTrue(store.remove(running.id(), job));
    assertFalse(store.remove(running.id(), job));

    assertEquals(0, store.finished());
    assertThrows(QueryJobNotFoundException.class, () -> service.get(running.id(), ALICE));
  }

  @Test
  void duplicateIdReturnsTheRegisteredJob() {
    QueryJob job =
        new QueryJob(new QueryJobId("node-a", "ctx"), ALICE, new Runner(), Clock.systemUTC());
    QueryJob duplicate =
        new QueryJob(new QueryJobId("node-a", "ctx"), ALICE, new Runner(), Clock.systemUTC());
    assertNull(store.register(job));

    assertEquals(job, store.register(duplicate));
  }

  @Test
  void closedDelegateRejectsRegistration() {
    store.close();
    QueryJob job =
        new QueryJob(new QueryJobId("node-a", "ctx"), ALICE, new Runner(), Clock.systemUTC());

    assertThrows(IllegalStateException.class, () -> store.register(job));

    assertEquals(0, scheduler.running());
    assertEquals(0, store.finished());
  }

  @Test
  void closeCancelsAndReleasesEveryJob() {
    Runner runner = new Runner();
    submitRunning(runner);
    completeRetained();

    store.close();

    assertEquals(1, runner.cancels);
    assertEquals(0, scheduler.running());
    assertEquals(0, store.finished());
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

  static MutableSettings settings(int maxRunning, int maxRetained) {
    MutableSettings settings = new MutableSettings();
    settings.put(Settings.Key.PPL_ASYNC_NODE_CONCURRENT_RUNNING_QUERIES, maxRunning);
    settings.put(Settings.Key.PPL_ASYNC_MAX_RETAINED_JOBS, maxRetained);
    settings.put(Settings.Key.PPL_ASYNC_MAX_QUEUED_QUERIES, 100);
    settings.put(Settings.Key.PPL_ASYNC_MAX_QUEUED_TIME, TimeValue.timeValueMinutes(5));
    return settings;
  }

  /** {@link Settings} backed by a map the test can change between calls. */
  static final class MutableSettings extends Settings {
    private final Map<Key, Object> values = new HashMap<>();

    void put(Key key, Object value) {
      values.put(key, value);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T getSettingValue(Key key) {
      return (T) values.get(key);
    }

    @Override
    public List<?> getSettings() {
      return List.of();
    }
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
