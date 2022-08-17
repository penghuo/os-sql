/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.stage;

import static org.opensearch.sql.planner.stage.StageState.SCHEDULING;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.opensearch.action.ActionListener;
import org.opensearch.client.node.NodeClient;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.opensearch.executor.scheduler.OpenSearchQueryScheduler;
import org.opensearch.sql.opensearch.executor.scheduler.StageScheduler;
import org.opensearch.sql.opensearch.executor.task.LocalTransportTaskPlan;
import org.opensearch.sql.opensearch.executor.task.TaskId;
import org.opensearch.sql.opensearch.executor.task.TaskNode;
import org.opensearch.sql.opensearch.executor.task.TaskState;
import org.opensearch.sql.opensearch.executor.transport.QLTaskResponse;
import org.opensearch.sql.opensearch.executor.transport.QLTaskType;
import org.opensearch.sql.planner.splits.Split;
import org.opensearch.sql.planner.splits.SplitManager;
import org.opensearch.sql.opensearch.executor.task.TransportTaskPlan;
import org.opensearch.sql.opensearch.executor.transport.QLTaskAction;
import org.opensearch.sql.opensearch.executor.transport.QLTaskRequest;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.stage.StageId;
import org.opensearch.sql.planner.stage.StageState;
import org.opensearch.sql.planner.stage.StageStateTable;

public class StageExecution {
  private final StageId stageId;

  private final LogicalPlan plan;

  private final SplitManager splitManager;

  private final Map<TaskId, TaskState> tasks;

  private final List<Consumer<StageState>> listeners;

  private final NodeClient client;

  private final Optional<StageOutput> output;

  private final AtomicReference<StageState> stageState;

  private final AtomicInteger finishTaskCount;

  private final StageStateTable stageStateTable;

  public StageExecution(StageId stageId, LogicalPlan plan,
                        SplitManager splitManager,
                        NodeClient client) {
    this(stageId, plan, splitManager, client, null);
  }

  public StageExecution(StageId stageId, LogicalPlan plan,
                        SplitManager splitManager,
                        NodeClient client,
                        StageOutput stageOutput) {
    this.stageId = stageId;
    this.plan = plan;
    this.splitManager = splitManager;
    this.tasks = new HashMap<>();
    this.listeners = new ArrayList<>();
    this.client = client;
    this.output = Optional.ofNullable(stageOutput);
    this.stageState = new AtomicReference<>(SCHEDULING());
    this.finishTaskCount = new AtomicInteger(0);
    this.stageStateTable = StageStateTable.INSTANCE;
  }

  public StageScheduler createStageScheduler() {
    try {
      List<Split> splits = splitManager.nextBatch().get();
      if (splits.size() == 1 && splitManager.noMoreSplits()) {
        return new OpenSearchQueryScheduler(this, splitManager);
      } else  {
        throw new RuntimeException("split large than 1 is not supported yet");
      }
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  public void schedule(TaskNode node) {
    TransportTaskPlan tempTaskPlan = null;
    if (node == TaskNode.LOCAL) {
      if (output.isPresent()) {
        tempTaskPlan = new LocalTransportTaskPlan(plan, node, output.get()::consume);
      } else {
        tempTaskPlan = new LocalTransportTaskPlan(plan, node, v -> {});
      }
    } else {
      tempTaskPlan = new TransportTaskPlan(plan, node);
    }
    TransportTaskPlan taskPlan = tempTaskPlan;
    client.execute(
        QLTaskAction.INSTANCE,
        new QLTaskRequest(QLTaskType.CREATE, taskPlan),
        new ActionListener<>() {
          @Override
          public void onResponse(QLTaskResponse resp) {
            TaskState taskState = tasks.get(taskPlan.getTaskId());
            taskState.updateTaskState(resp.getTaskState());

            if (resp.getTaskState().isFinish()) {
              finishTaskCount.addAndGet(1);
              if (finishTaskCount.get() == tasks.size()) {
                int totalSize = tasks.values().stream().mapToInt(TaskState::getRowCount).sum();
                StageState temp = new StageState(0, totalSize, StageState.StageExecutionState.FINISH);
                stageState.set(temp);
                updateListener(temp);
                // update stageStateTable
                stageStateTable.updateStageState(stageId, stageState.get());
              }
            }
          }

          @Override
          public void onFailure(Exception e) {
            // cancel other tasks?
            StageState temp = new StageState(-1, -1, StageState.StageExecutionState.FINISH);
            stageState.set(temp);
            updateListener(temp);
          }
        });
    tasks.put(taskPlan.getTaskId(), TaskState.SCHEDULING());
  }

  public void addListener(Consumer<StageState> listener) {
    listeners.add(listener);
  }

  private void updateListener(StageState stageState) {
    for (Consumer<StageState> listener : listeners) {
      listener.accept(stageState);
    }
  }

  public void addOutputListener(ResponseListener<ExecutionEngine.QueryResponse> listener) {
    if (output.isPresent()) {
      output.get().addListener(listener);
    }
  }

  public StageState getStageState() {
    return stageState.get();
  }
}
