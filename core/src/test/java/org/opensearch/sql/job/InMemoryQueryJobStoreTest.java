/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.job;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.executor.ExecutionEngine.Schema;
import org.opensearch.sql.executor.pagination.Cursor;
import org.opensearch.sql.job.exceptions.QueryJobCapacityException;

class InMemoryQueryJobStoreTest {

  private static final QueryResult WIDE_RESULT =
      new QueryResult.Rows(
          new Schema(List.of()),
          List.of(
              ExprTupleValue.fromExprValueMap(Map.of("f", new ExprStringValue("x".repeat(64))))),
          Cursor.None,
          List.of(),
          0);

  @Test
  void register_returnsNullOnFirstInsertion() {
    InMemoryQueryJobStore store = new InMemoryQueryJobStore();
    QueryJob job = newJob("ctx-1");
    assertEquals(null, store.register(job));
    assertSame(job, store.find(job.id()).orElseThrow());
  }

  @Test
  void register_returnsExistingJobOnDuplicateId() {
    InMemoryQueryJobStore store = new InMemoryQueryJobStore();
    QueryJob first = newJob("ctx-duplicate");
    QueryJob second =
        new QueryJob(first.id(), Principal.UNSECURED, new NoopRunner(), Clock.systemUTC());
    assertEquals(null, store.register(first));
    assertSame(first, store.register(second));
  }

  @Test
  void remove_isConditional() {
    InMemoryQueryJobStore store = new InMemoryQueryJobStore();
    QueryJob job = newJob("ctx-remove");
    store.register(job);
    QueryJob other =
        new QueryJob(job.id(), Principal.UNSECURED, new NoopRunner(), Clock.systemUTC());
    assertFalse(store.remove(job.id(), other));
    assertTrue(store.remove(job.id(), job));
    assertEquals(Optional.empty(), store.find(job.id()));
  }

  @Test
  void jobs_returnsDefensiveCopy() {
    InMemoryQueryJobStore store = new InMemoryQueryJobStore();
    QueryJob job = newJob("ctx-jobs");
    store.register(job);
    assertNotNull(store.jobs());
    assertEquals(1, store.jobs().size());
    store.remove(job.id(), job);
    assertEquals(0, store.jobs().size());
  }

  @Test
  void close_rejectsFurtherRegistration() {
    InMemoryQueryJobStore store = new InMemoryQueryJobStore();
    store.close();
    QueryJob job = newJob("ctx-after-close");
    assertThrows(IllegalStateException.class, () -> store.register(job));
  }

  @Test
  void close_cancelsResidualJobs() {
    InMemoryQueryJobStore store = new InMemoryQueryJobStore();
    QueryJob job = newJob("ctx-shutdown");
    store.register(job);
    store.close();
    assertEquals(QueryJobState.CANCELLED, job.status().state());
  }

  @Test
  void register_refusesWhileTheJobLimitIsReached() {
    InMemoryQueryJobStore store = new InMemoryQueryJobStore(() -> 1, () -> Long.MAX_VALUE);
    QueryJob first = newJob("ctx-first");
    store.register(first);
    QueryJobCapacityException e =
        assertThrows(QueryJobCapacityException.class, () -> store.register(newJob("ctx-second")));
    assertTrue(e.getMessage().contains("plugins.ppl.async.max_retained_jobs"), e.getMessage());
    assertEquals(1, store.rejectedSubmits());
    store.remove(first.id(), first);
    assertEquals(null, store.register(newJob("ctx-second")));
  }

  @Test
  void register_readsTheJobLimitOnEveryCall() {
    AtomicInteger limit = new AtomicInteger(2);
    InMemoryQueryJobStore store = new InMemoryQueryJobStore(limit::get, () -> Long.MAX_VALUE);
    store.register(newJob("ctx-1"));
    limit.set(1);
    assertThrows(QueryJobCapacityException.class, () -> store.register(newJob("ctx-2")));
    assertEquals(1, store.size());
  }

  @Test
  void register_refusesWhileRetainedResultsFillTheBudget() {
    InMemoryQueryJobStore store = new InMemoryQueryJobStore(() -> 10, () -> 100L);
    store.reserve(100L);
    QueryJobCapacityException e =
        assertThrows(QueryJobCapacityException.class, () -> store.register(newJob("ctx-full")));
    assertTrue(
        e.getMessage().contains("plugins.ppl.async.max_retained_result_size"), e.getMessage());
    store.release(100L);
    assertEquals(null, store.register(newJob("ctx-full")));
  }

  @Test
  void reserve_refusesBytesBeyondTheBudget() {
    InMemoryQueryJobStore store = new InMemoryQueryJobStore(() -> 10, () -> 100L);
    store.reserve(60L);
    assertThrows(QueryJobCapacityException.class, () -> store.reserve(41L));
    assertEquals(60L, store.resultBytes());
    assertEquals(1, store.rejectedResults());
    store.reserve(40L);
    assertEquals(100L, store.resultBytes());
  }

  @Test
  void remove_returnsTheRemovedJobsCharge() {
    InMemoryQueryJobStore store = new InMemoryQueryJobStore(() -> 10, () -> Long.MAX_VALUE);
    CompletableFuture<QueryResult> result = new CompletableFuture<>();
    QueryJob job = retainedJob(store, "ctx-charged", result);
    result.complete(WIDE_RESULT);
    assertEquals(QueryResultSizeEstimator.estimate(WIDE_RESULT), store.resultBytes());
    assertTrue(store.remove(job.id(), job));
    assertFalse(store.remove(job.id(), job));
    assertEquals(0L, store.resultBytes());
  }

  @Test
  void close_returnsChargesAndCancelsUnfinishedJobs() {
    InMemoryQueryJobStore store = new InMemoryQueryJobStore(() -> 10, () -> Long.MAX_VALUE);
    CompletableFuture<QueryResult> finished = new CompletableFuture<>();
    QueryJob succeeded = retainedJob(store, "ctx-succeeded", finished);
    finished.complete(WIDE_RESULT);
    QueryJob running = retainedJob(store, "ctx-running", new CompletableFuture<>());
    store.close();
    assertEquals(0L, store.resultBytes());
    assertEquals(0, store.size());
    assertEquals(QueryJobState.SUCCEEDED, succeeded.status().state());
    assertEquals(QueryJobState.CANCELLED, running.status().state());
  }

  @Test
  void concurrentJobs_neverExceedTheLimitsAndReturnEveryCharge() throws Exception {
    int maxJobs = 4;
    long resultBytes = QueryResultSizeEstimator.estimate(WIDE_RESULT);
    InMemoryQueryJobStore store = new InMemoryQueryJobStore(() -> maxJobs, () -> 3 * resultBytes);
    AtomicInteger peakJobs = new AtomicInteger();
    AtomicLong peakBytes = new AtomicLong();
    ExecutorService pool = Executors.newFixedThreadPool(8);
    try {
      List<Future<?>> tasks = new ArrayList<>();
      for (int t = 0; t < 8; t++) {
        int thread = t;
        tasks.add(
            pool.submit(
                () -> {
                  for (int i = 0; i < 500; i++) {
                    CompletableFuture<QueryResult> result = new CompletableFuture<>();
                    QueryJob job;
                    try {
                      job = retainedJob(store, thread + "-" + i, result);
                    } catch (QueryJobCapacityException e) {
                      continue;
                    }
                    peakJobs.accumulateAndGet(store.size(), Math::max);
                    result.complete(WIDE_RESULT);
                    peakBytes.accumulateAndGet(store.resultBytes(), Math::max);
                    store.remove(job.id(), job);
                  }
                }));
      }
      for (Future<?> task : tasks) {
        task.get(30, TimeUnit.SECONDS);
      }
    } finally {
      pool.shutdownNow();
    }
    assertEquals(0, store.size());
    assertEquals(0L, store.resultBytes());
    assertTrue(peakJobs.get() <= maxJobs, "peak jobs " + peakJobs.get());
    assertTrue(peakBytes.get() <= 3 * resultBytes, "peak bytes " + peakBytes.get());
  }

  /** Registers and starts a job, then retains it the way an expired submit wait does. */
  private static QueryJob retainedJob(
      InMemoryQueryJobStore store, String contextId, CompletableFuture<QueryResult> result) {
    QueryJob job =
        new QueryJob(
            new QueryJobId("node-1", contextId),
            Principal.UNSECURED,
            new FutureRunner(result),
            Clock.systemUTC(),
            null,
            store);
    assertEquals(null, store.register(job));
    job.startRunner();
    job.await(Duration.ZERO);
    return job;
  }

  private static QueryJob newJob(String contextId) {
    QueryJobId id = new QueryJobId("node-1", contextId);
    return new QueryJob(id, Principal.UNSECURED, new NoopRunner(), Clock.systemUTC());
  }

  private record FutureRunner(CompletableFuture<QueryResult> result) implements QueryRunner {
    @Override
    public CompletionStage<QueryResult> run() {
      return result;
    }

    @Override
    public void cancel() {
      result.cancel(false);
    }
  }

  private static final class NoopRunner implements QueryRunner {
    @Override
    public CompletionStage<QueryResult> run() {
      return new CompletableFuture<>();
    }

    @Override
    public void cancel() {}
  }
}
