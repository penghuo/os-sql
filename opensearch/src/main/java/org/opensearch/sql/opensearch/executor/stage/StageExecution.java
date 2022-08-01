/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.stage;

import java.util.List;
import java.util.Set;
import lombok.Data;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.sql.opensearch.executor.Split;
import org.opensearch.sql.opensearch.executor.task.TransportTaskPlan;
import org.opensearch.sql.opensearch.executor.task.TaskPlan;
import org.opensearch.sql.planner.logical.LogicalPlan;

@Data
public class StageExecution {
  private final StageId stageId;
  private final LogicalPlan plan;
  private final Set<TaskPlan> tasks;

  public void schedule(DiscoveryNode node) {
    tasks.add(new TransportTaskPlan(plan, node));
  }

  public void addSplits(List<Split> splitList) {

  }
}
