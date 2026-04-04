/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.trino.execution;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.units.Duration;
import io.trino.execution.SplitRunner;
import io.trino.execution.TaskId;
import io.trino.execution.executor.RunningSplitInfo;
import io.trino.execution.executor.TaskExecutor;
import io.trino.execution.executor.TaskHandle;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.DoubleSupplier;
import java.util.function.Predicate;

/**
 * A {@link TaskExecutor} implementation that delegates split execution to a Java {@link
 * ExecutorService}. In production the executor would be backed by an OpenSearch thread pool.
 *
 * <p>Each split is cooperatively scheduled: {@link SplitRunner#processFor(Duration)} is called with
 * a 1-second time quantum. If the split is not finished after processing, it is re-submitted to the
 * executor for another quantum.
 */
public class OpenSearchTaskExecutor implements TaskExecutor {

  private static final Duration TIME_QUANTA = new Duration(1, TimeUnit.SECONDS);

  private final ExecutorService executor;
  private final Set<OpenSearchTaskHandle> activeTasks = ConcurrentHashMap.newKeySet();

  public OpenSearchTaskExecutor(ExecutorService executor) {
    this.executor = executor;
  }

  @Override
  public void start() {
    // No-op: the ExecutorService is assumed to be ready.
  }

  @Override
  public void stop() {
    executor.shutdownNow();
  }

  @Override
  public TaskHandle addTask(
      TaskId taskId,
      DoubleSupplier utilizationSupplier,
      int initialSplitConcurrency,
      Duration splitConcurrencyAdjustFrequency,
      OptionalInt maxDriversPerTask) {
    OpenSearchTaskHandle handle = new OpenSearchTaskHandle(taskId);
    activeTasks.add(handle);
    return handle;
  }

  @Override
  public void removeTask(TaskHandle taskHandle) {
    if (taskHandle instanceof OpenSearchTaskHandle osHandle) {
      osHandle.destroy();
      activeTasks.remove(osHandle);
    }
  }

  @Override
  public List<ListenableFuture<Void>> enqueueSplits(
      TaskHandle taskHandle, boolean intermediate, List<? extends SplitRunner> splits) {
    List<ListenableFuture<Void>> futures = new ArrayList<>(splits.size());
    for (SplitRunner split : splits) {
      SettableFuture<Void> future = SettableFuture.create();
      futures.add(future);
      submitSplit(split, future);
    }
    return Collections.unmodifiableList(futures);
  }

  @Override
  public Set<TaskId> getStuckSplitTaskIds(
      Duration blockedThreshold, Predicate<RunningSplitInfo> filter) {
    return Collections.emptySet();
  }

  private void submitSplit(SplitRunner split, SettableFuture<Void> future) {
    executor.submit(
        () -> {
          try {
            if (split.isFinished()) {
              split.close();
              future.set(null);
              return;
            }
            ListenableFuture<Void> blocked = split.processFor(TIME_QUANTA);
            if (split.isFinished()) {
              split.close();
              future.set(null);
            } else {
              // Cooperative re-submission: wait for any blocked future, then re-queue.
              Futures.addCallback(
                  blocked,
                  new com.google.common.util.concurrent.FutureCallback<Void>() {
                    @Override
                    public void onSuccess(Void result) {
                      submitSplit(split, future);
                    }

                    @Override
                    public void onFailure(Throwable t) {
                      future.setException(t);
                    }
                  },
                  executor);
            }
          } catch (Exception e) {
            try {
              split.close();
            } catch (Exception closeEx) {
              e.addSuppressed(closeEx);
            }
            future.setException(e);
          }
        });
  }

  /** Simple task handle that tracks whether the task has been destroyed. */
  static class OpenSearchTaskHandle implements TaskHandle {

    private final TaskId taskId;
    private volatile boolean destroyed;

    OpenSearchTaskHandle(TaskId taskId) {
      this.taskId = taskId;
    }

    @Override
    public boolean isDestroyed() {
      return destroyed;
    }

    void destroy() {
      destroyed = true;
    }

    TaskId getTaskId() {
      return taskId;
    }
  }
}
