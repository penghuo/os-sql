/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.scheduler;

import lombok.RequiredArgsConstructor;
import org.opensearch.client.node.NodeClient;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.sql.opensearch.executor.task.TaskNode;
import org.opensearch.sql.planner.splits.Split;
import org.opensearch.sql.planner.splits.SplitManager;
import org.opensearch.sql.opensearch.executor.stage.StageExecution;

/**
 * Schedule the stage on local(coordination) node.
 */
@RequiredArgsConstructor
public class OpenSearchQueryScheduler implements StageScheduler {
  private final StageExecution stageExecution;
  private final SplitManager splitManager;
  private final DiscoveryNode localNode;
  private final Split split;

  @Override
  public void schedule() {
    // todo
    stageExecution.schedule(new TaskNode(localNode, TaskNode.Type.LOCAL, split));
  }

  public static class NoSplit implements Split {

  }
}
