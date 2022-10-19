/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.task;

import com.google.common.util.concurrent.ListenableFuture;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import lombok.Data;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.executor.StateChangeListener;
import org.opensearch.sql.executor.task.Task;
import org.opensearch.sql.executor.task.TaskExecutionInfo;
import org.opensearch.sql.executor.task.TaskExecutionState;
import org.opensearch.sql.executor.task.TaskId;
import org.opensearch.sql.opensearch.client.OpenSearchClient;
import org.opensearch.sql.opensearch.executor.transport.QLTaskRequest;
import org.opensearch.sql.opensearch.executor.transport.dataplane.QLDataService;
import org.opensearch.sql.planner.Planner;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.logical.LogicalPlanNodeVisitor;
import org.opensearch.sql.planner.logical.LogicalRelation;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.storage.Table;

/** Each node has a singleton TaskService which is started during node bootstrap. */
@RequiredArgsConstructor
public class TaskService {
  private static final Logger LOG = LogManager.getLogger();

  private static final int numberOfThreads = 4;

  private final ConcurrentHashMap<TaskId, TaskExecution> tasks;

  private final OpenSearchClient client;

  private final BlockingDeque<TaskExecution> taskExecutionQueue;

  private final ExecutorService executor;

  private final ExecutionEngine executionEngine;

  private final QLDataService qlDataService;

  public TaskService(OpenSearchClient client, ExecutorService executor,
                     ExecutionEngine executionEngine, QLDataService qlDataService) {
    this.tasks = new ConcurrentHashMap<>();
    this.taskExecutionQueue = new LinkedBlockingDeque<>();
    this.client = client;
    this.executor = executor;
    this.executionEngine = executionEngine;
    this.qlDataService = qlDataService;
  }

  // kick off thread runner.
  public void init() {
    for (int i = 0; i < numberOfThreads; i++) {
      executor.execute(new TaskRunner());
    }
  }

  public void addTask(QLTaskRequest taskRequest, StateChangeListener<TaskExecutionState> listener) {
    TaskExecution taskExecution = createTaskExecution(taskRequest, listener);
    tasks.put(taskRequest.getTaskId(), taskExecution);
    try {
      taskExecutionQueue.putLast(taskExecution);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  private class TaskRunner implements Runnable {
    @Override
    public void run() {
      try {
        while (true) {
          LOG.info("TaskRunner execute");
          TaskExecution execution = taskExecutionQueue.takeFirst();

          ListenableFuture<?> blocked = execution.execute();
          if (blocked == null) continue;
          blocked.addListener(
              () -> {
                taskExecutionQueue.offer(execution);
              },
              executor);
        }
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private TaskExecution createTaskExecution(
      QLTaskRequest taskRequest, StateChangeListener<TaskExecutionState> listener) {
    LogicalPlan logicalPlan = taskRequest.getPlan();
    PhysicalPlan physicalPlan = executionEngine.implement(logicalPlan,
        Arrays.asList(taskRequest.getSourceTasks(), client.getNodeClient(), qlDataService,
            taskRequest.getTaskId()));

    return new TaskExecution(physicalPlan, listener, taskRequest.getSplit());
  }

  public TaskExecutionInfo getTaskExecutionInfo(TaskId taskId) {
    return tasks.get(taskId).getTaskExecutionInfo();
  }
}
