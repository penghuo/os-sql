/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.stage;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import org.opensearch.client.node.NodeClient;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.opensearch.executor.scheduler.OpenSearchQueryScheduler;
import org.opensearch.sql.opensearch.executor.scheduler.StageScheduler;
import org.opensearch.sql.opensearch.executor.transport.QLTaskType;
import org.opensearch.sql.planner.splits.Split;
import org.opensearch.sql.planner.splits.SplitManager;
import org.opensearch.sql.opensearch.executor.task.TransportTaskPlan;
import org.opensearch.sql.opensearch.executor.task.TaskPlan;
import org.opensearch.sql.opensearch.executor.transport.QLTaskAction;
import org.opensearch.sql.opensearch.executor.transport.QLTaskRequest;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.stage.StageId;

public class StageExecution {
  private final StageId stageId;

  private final LogicalPlan plan;

  private final SplitManager splitManager;

  private final Set<TaskPlan> tasks;

  private final List<Consumer<StageState>> listeners;

  private final NodeClient client;

  private final Optional<StageOutput> output;

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
    this.tasks = new HashSet<>();
    this.listeners = new ArrayList<>();
    this.client = client;
    this.output = Optional.of(stageOutput);
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

  public void schedule(DiscoveryNode node) {
    TransportTaskPlan taskPlan = new TransportTaskPlan(plan, node);
    client.execute(QLTaskAction.INSTANCE, new QLTaskRequest(QLTaskType.CREATE, taskPlan));
    tasks.add(taskPlan);
  }

  public void addListener(Consumer<StageState> listener) {
    listeners.add(listener);
  }

  public void addOutputListener(ResponseListener<ExecutionEngine.QueryResponse> listener) {
    if (output.isPresent()) {
      output.get().addListener(listener);
    }
  }
}
