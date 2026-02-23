/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.exchange.stage;

import java.util.Objects;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.dqe.exchange.action.DqeStageCancelRequest;
import org.opensearch.dqe.exchange.action.DqeStageCancelResponse;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportChannel;
import org.opensearch.transport.TransportRequestHandler;

/**
 * Data-node-side handler for stage cancellation requests.
 *
 * <p>When the coordinator sends a {@code internal:dqe/stage/cancel} request, this handler delegates
 * to the {@link StageExecutionHandler} to cancel the specified stage and sends an acknowledgment
 * back.
 */
public class StageCancelHandler implements TransportRequestHandler<DqeStageCancelRequest> {

  private static final Logger logger = LogManager.getLogger(StageCancelHandler.class);

  private final StageExecutionHandler executionHandler;

  /**
   * Create a stage cancel handler.
   *
   * @param executionHandler the execution handler that tracks active stages
   */
  public StageCancelHandler(StageExecutionHandler executionHandler) {
    this.executionHandler =
        Objects.requireNonNull(executionHandler, "executionHandler must not be null");
  }

  @Override
  public void messageReceived(DqeStageCancelRequest request, TransportChannel channel, Task task)
      throws Exception {

    String queryId = request.getQueryId();
    int stageId = request.getStageId();

    logger.debug("[{}] Received cancel request for stage {}", queryId, stageId);

    boolean wasActive = executionHandler.isStageActive(queryId, stageId);
    executionHandler.cancelStage(queryId, stageId);

    logger.debug("[{}] Stage {} cancel processed (wasActive={})", queryId, stageId, wasActive);

    channel.sendResponse(new DqeStageCancelResponse(queryId, stageId, true));
  }
}
