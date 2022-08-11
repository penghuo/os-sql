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
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.logical.LogicalPlanNodeVisitor;
import org.opensearch.sql.planner.logical.LogicalWrite;
import org.opensearch.sql.planner.splits.Split;
import org.opensearch.sql.planner.splits.SplitManager;
import org.opensearch.sql.storage.Table;

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

  public void addTask(TaskPlan taskPlan) {
    // todo. TaskExecution is created from LogicalPlan in TaskPlan.
    TaskExecution taskExecution = createTaskExecution(taskPlan);
    tasks.put(taskPlan.getTaskId(), taskExecution);
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


  private TaskExecution createTaskExecution(TaskPlan taskPlan) {
    FindTable findTable = new FindTable();
    LogicalPlan logicalPlan = taskPlan.getPlan();
    Table table = logicalPlan.accept(findTable, null);
    return new TaskExecution(table.implement(table.optimize(logicalPlan)));
  }


  private static class FindTable extends LogicalPlanNodeVisitor<Table, Void> {
    @Override
    public Table visitWrite(LogicalWrite plan, Void context) {
      return plan.getTable();
    }
  }
}
