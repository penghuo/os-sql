/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.stage;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import lombok.Data;
import org.opensearch.client.node.NodeClient;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.sql.opensearch.executor.scheduler.OpenSearchQueryScheduler;
import org.opensearch.sql.opensearch.executor.scheduler.StageScheduler;
import org.opensearch.sql.opensearch.executor.splits.Split;
import org.opensearch.sql.opensearch.executor.splits.SplitManager;
import org.opensearch.sql.opensearch.executor.task.TransportTaskPlan;
import org.opensearch.sql.opensearch.executor.task.TaskPlan;
import org.opensearch.sql.opensearch.executor.transport.QLTaskAction;
import org.opensearch.sql.opensearch.executor.transport.QLTaskRequest;
import org.opensearch.sql.planner.logical.LogicalPlan;

@Data
public class StageExecution {
  private final StageId stageId;

  private final LogicalPlan plan;

  private final SplitManager splitManager;

  private final Set<TaskPlan> tasks;

  private final List<Consumer<StageState>> listeners;

  private final NodeClient client;

  private final StageOutput output;

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
    tasks.add(new TransportTaskPlan(plan, node));
    client.execute(QLTaskAction.INSTANCE, new QLTaskRequest());
  }



  public void addListener(Consumer<StageState> listener) {
    listeners.add(listener);
  }
}
