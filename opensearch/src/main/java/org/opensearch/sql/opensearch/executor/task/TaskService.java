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
import java.util.concurrent.LinkedBlockingDeque;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.sql.opensearch.client.OpenSearchClient;
import org.opensearch.sql.opensearch.executor.OpenSearchExecutionEngine;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.logical.LogicalPlanNodeVisitor;
import org.opensearch.sql.planner.logical.LogicalRelation;
import org.opensearch.sql.planner.logical.LogicalStageState;
import org.opensearch.sql.planner.logical.LogicalWrite;
import org.opensearch.sql.planner.splits.Split;
import org.opensearch.sql.planner.splits.SplitManager;
import org.opensearch.sql.planner.stage.StageStateTable;
import org.opensearch.sql.storage.Table;
import org.springframework.cglib.core.Local;

/** Each node has a singleton TaskService which is started during node bootstrap. */
@RequiredArgsConstructor
public class TaskService {
  private static final Logger LOG = LogManager.getLogger();

  private final static int numberOfThreads = 4;

  private final ConcurrentHashMap<TaskId, TaskExecution> tasks;

  private final OpenSearchClient client;

  private final BlockingDeque<TaskExecution> taskExecutionQueue;

  private final ExecutorService executor;

  public TaskService(OpenSearchClient client, ExecutorService executor) {
    this.tasks = new ConcurrentHashMap<>();
    this.taskExecutionQueue = new LinkedBlockingDeque<>();
    this.client = client;
    this.executor = executor;
  }

  // kick off thread runner.
  public void init() {
    for (int i = 0; i < numberOfThreads; i++) {
      executor.execute(new TaskRunner());
    }
  }

  public void addTask(TaskPlan taskPlan, TaskExecution.TaskExecutionListener listener) {
    // todo. TaskExecution is created from LogicalPlan in TaskPlan.
    TaskExecution taskExecution = createTaskExecution(taskPlan, listener);
    tasks.put(taskPlan.getTaskId(), taskExecution);
    try {
      taskExecutionQueue.putLast(taskExecution);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  public void addSplit(List<Split> splitList) {
    // todo;
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

  public TaskState taskState(TaskPlan task) {
    return tasks.get(task.getTaskId()).taskState();
  }


  private TaskExecution createTaskExecution(TaskPlan taskPlan,
                                            TaskExecution.TaskExecutionListener listener) {
    TableBuilder findTable = new TableBuilder();
    LogicalPlan logicalPlan = taskPlan.getPlan();
    TableContext tblCtx = new TableContext();
    logicalPlan.accept(findTable, tblCtx);
    Table table = tblCtx.write != null ? tblCtx.getWrite() : tblCtx.getRead();

    if (taskPlan instanceof LocalTransportTaskPlan) {
      return new TaskExecution(table.implement(table.optimize(logicalPlan)),
          ((LocalTransportTaskPlan) taskPlan).getConsumer(), listener);
    } else {
      return new TaskExecution(table.implement(table.optimize(logicalPlan)), v -> {}, listener);
    }
  }

  private static class TableBuilder extends LogicalPlanNodeVisitor<Void, TableContext> {
    @Override
    public Void visitNode(LogicalPlan plan, TableContext context) {
      plan.getChild().forEach(child -> child.accept(this, context));
      return null;
    }

    @Override
    public Void visitWrite(LogicalWrite plan, TableContext context) {
      context.setWrite(plan.getTable());
      return null;
    }

    @Override
    public Void visitRelation(LogicalRelation plan, TableContext context) {
      context.setRead(plan.getTable());
      return null;
    }

    @Override
    public Void visitStageState(LogicalStageState plan, TableContext context) {
      context.setRead(StageStateTable.INSTANCE);
      return null;
    }
  }

  @Data
  static class TableContext {
    Table read;
    Table write;
  }
}
