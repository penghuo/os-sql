/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.trino.execution;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.Duration;
import io.trino.execution.SplitRunner;
import io.trino.execution.StageId;
import io.trino.execution.TaskId;
import io.trino.execution.executor.TaskHandle;
import io.trino.spi.QueryId;
import java.util.List;
import java.util.OptionalInt;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.DoubleSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class OpenSearchTaskExecutorTest {

  private ExecutorService threadPool;
  private OpenSearchTaskExecutor taskExecutor;

  @BeforeEach
  void setUp() {
    threadPool = Executors.newFixedThreadPool(4);
    taskExecutor = new OpenSearchTaskExecutor(threadPool);
    taskExecutor.start();
  }

  @AfterEach
  void tearDown() {
    taskExecutor.stop();
  }

  @Test
  void addAndRemoveTask() {
    TaskId taskId = createTaskId("add_remove_0");
    TaskHandle handle =
        taskExecutor.addTask(
            taskId,
            () -> 0.5,
            1,
            new Duration(1, TimeUnit.SECONDS),
            OptionalInt.empty());

    assertNotNull(handle);
    assertFalse(handle.isDestroyed());

    taskExecutor.removeTask(handle);
    assertTrue(handle.isDestroyed());
  }

  @Test
  void splitRunsToCompletion() throws Exception {
    TaskHandle handle = addTestTask("completion_0");

    // Create a split that finishes immediately
    SplitRunner split = mock(SplitRunner.class);
    when(split.isFinished()).thenReturn(false, true);
    when(split.processFor(any(Duration.class)))
        .thenReturn(Futures.immediateVoidFuture());

    List<ListenableFuture<Void>> futures =
        taskExecutor.enqueueSplits(handle, false, List.of(split));

    // Wait for completion
    futures.get(0).get(5, TimeUnit.SECONDS);

    verify(split, atLeastOnce()).processFor(any(Duration.class));
    verify(split).close();
  }

  @Test
  void multipleSplitsRunInParallel() throws Exception {
    TaskHandle handle = addTestTask("parallel_0");

    AtomicInteger concurrency = new AtomicInteger(0);
    AtomicInteger maxConcurrency = new AtomicInteger(0);

    // Create two splits that each take a brief time
    SplitRunner split1 = createConcurrencySplit(concurrency, maxConcurrency);
    SplitRunner split2 = createConcurrencySplit(concurrency, maxConcurrency);

    List<ListenableFuture<Void>> futures =
        taskExecutor.enqueueSplits(handle, false, List.of(split1, split2));

    // Wait for both to complete
    for (ListenableFuture<Void> f : futures) {
      f.get(5, TimeUnit.SECONDS);
    }

    // With a 4-thread pool, both splits should have run concurrently
    assertTrue(maxConcurrency.get() >= 2, "Expected concurrent execution, max was "
        + maxConcurrency.get());
  }

  @Test
  void cooperativeResubmission() throws Exception {
    TaskHandle handle = addTestTask("coop_0");

    // Create a split that needs 3 rounds of processFor before finishing
    SplitRunner split = mock(SplitRunner.class);
    AtomicInteger processCount = new AtomicInteger(0);
    when(split.isFinished())
        .thenAnswer(invocation -> processCount.get() >= 3);
    when(split.processFor(any(Duration.class)))
        .thenAnswer(
            invocation -> {
              processCount.incrementAndGet();
              return Futures.immediateVoidFuture();
            });

    List<ListenableFuture<Void>> futures =
        taskExecutor.enqueueSplits(handle, false, List.of(split));

    futures.get(0).get(5, TimeUnit.SECONDS);

    // processFor should have been called at least 3 times
    verify(split, atLeast(3)).processFor(any(Duration.class));
    verify(split).close();
  }

  private TaskHandle addTestTask(String queryId) {
    TaskId taskId = createTaskId(queryId);
    return taskExecutor.addTask(
        taskId,
        () -> 0.5,
        1,
        new Duration(1, TimeUnit.SECONDS),
        OptionalInt.empty());
  }

  private static TaskId createTaskId(String id) {
    QueryId queryId = new QueryId(id);
    StageId stageId = new StageId(queryId, 0);
    return new TaskId(stageId, 0, 0);
  }

  /**
   * Creates a SplitRunner that tracks concurrency. It finishes after one processFor call.
   */
  private SplitRunner createConcurrencySplit(
      AtomicInteger concurrency, AtomicInteger maxConcurrency) {
    SplitRunner split = mock(SplitRunner.class);
    AtomicInteger called = new AtomicInteger(0);
    when(split.isFinished()).thenAnswer(inv -> called.get() >= 1);
    when(split.processFor(any(Duration.class)))
        .thenAnswer(
            invocation -> {
              int current = concurrency.incrementAndGet();
              maxConcurrency.updateAndGet(max -> Math.max(max, current));
              try {
                // Small delay to allow overlap
                Thread.sleep(100);
              } finally {
                concurrency.decrementAndGet();
              }
              called.incrementAndGet();
              return Futures.immediateVoidFuture();
            });
    return split;
  }
}
