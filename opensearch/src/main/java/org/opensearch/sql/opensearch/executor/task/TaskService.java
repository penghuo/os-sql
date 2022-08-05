/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.task;

import com.google.common.util.concurrent.ListenableFuture;
import java.util.List;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.opensearch.client.OpenSearchClient;
import org.opensearch.sql.opensearch.executor.splits.Split;

/** Each node has a singleton TaskService which is started during node bootstrap. */
@RequiredArgsConstructor
public class TaskService {
  private final static int numberOfThreads = 4;

  private final ConcurrentHashMap<TaskId, TaskExecution> tasks;

  private final OpenSearchClient client;

  private final BlockingDeque<TaskExecution> taskExecutionQueue;

  private final ExecutorService executor;

  // kick off thread runner.
  public void init() {
    for (int i = 0; i < numberOfThreads; i++) {
      executor.submit(new TaskRunner());
    }
  }

  public void addTask(TaskPlan task) {
    // todo. TaskExecution is created from LogicalPlan in TaskPlan.
    TaskExecution taskExecution = null;
    tasks.put(task.getTaskId(), taskExecution);
    taskExecutionQueue.add(taskExecution);
  }

  public void addSplit(List<Split> splitList) {
    // todo;
  }

  private class TaskRunner implements Runnable {
    @Override
    public void run() {
      TaskExecution execution = taskExecutionQueue.pollFirst();

      ListenableFuture<?> blocked = execution.execute();

      blocked.addListener(
          () -> {
            taskExecutionQueue.offer(execution);
          },
          executor);
    }
  }

  public TaskState taskState(TaskPlan task) {
    return tasks.get(task.getTaskId()).taskState();
  }
}
