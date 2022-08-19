/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.scheduler;

import java.util.Iterator;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.sql.opensearch.executor.stage.StageExecution;
import org.opensearch.sql.opensearch.executor.task.TaskNode;
import org.opensearch.sql.planner.splits.Split;

@RequiredArgsConstructor
public class ScaleWriterScheduler implements StageScheduler {
  private final StageExecution stageExecution;
  private final List<Split> splits;
  private final DiscoveryNodes nodes;

  @Override
  public void schedule() {
    assert splits.size() == nodes.getSize();

    final Iterator<DiscoveryNode> nodeIterator = nodes.iterator();
    for (Split split : splits) {
      stageExecution.schedule(new TaskNode(nodeIterator.next(), TaskNode.Type.OTHER, split));
    }
  }
}
