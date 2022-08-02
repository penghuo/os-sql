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
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.sql.opensearch.executor.scheduler.OpenSearchQueryScheduler;
import org.opensearch.sql.opensearch.executor.scheduler.StageScheduler;
import org.opensearch.sql.opensearch.executor.splits.Split;
import org.opensearch.sql.opensearch.executor.splits.SplitManager;
import org.opensearch.sql.opensearch.executor.task.TransportTaskPlan;
import org.opensearch.sql.opensearch.executor.task.TaskPlan;
import org.opensearch.sql.planner.logical.LogicalPlan;

@Data
public class StageExecution {
  private final StageId stageId;

  private final LogicalPlan plan;

  private final SplitManager splitManager;

  private final Set<TaskPlan> tasks;

  private final List<Consumer<StageState>> listeners;

  public StageScheduler createStageScheduler() {
    try {
      List<Split> splits = splitManager.nextBatch().get();
      if (splits.size() == 1 && splitManager.noMoreSplits()) {
        return OpenSearchQueryScheduler()
      } throw {

      }
    } catch (InterruptedException | ExecutionException e) {
      e.printStackTrace();
    }
  }

  public void schedule(DiscoveryNode node) {
    tasks.add(new TransportTaskPlan(plan, node));
  }

  public void addListener(Consumer<StageState> listener) {
    listeners.add(listener);
  }
}
