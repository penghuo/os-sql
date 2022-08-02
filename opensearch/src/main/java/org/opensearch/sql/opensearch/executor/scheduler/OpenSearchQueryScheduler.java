/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.scheduler;

import lombok.RequiredArgsConstructor;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.sql.opensearch.executor.splits.SplitManager;
import org.opensearch.sql.opensearch.executor.stage.StageExecution;

/**
 * Schedule the stage on local(coordination) node.
 */
@RequiredArgsConstructor
public class OpenSearchQueryScheduler implements StageScheduler {
  private final StageExecution stageExecution;
  private final SplitManager splitManager;

  @Override
  public void schedule() {
    // todo
    stageExecution.schedule(DiscoveryNodes.EMPTY_NODES.getLocalNode());
  }
}
