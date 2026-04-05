/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.trino.transport;

import io.trino.execution.TaskId;
import io.trino.execution.buffer.PipelinedOutputBuffers.OutputBufferId;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.sql.trino.execution.OpenSearchSqlTaskManager;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

/**
 * Handles {@code trino:task/results/ack} on the worker node. Acknowledges consumed pages, allowing
 * the upstream OutputBuffer to release memory.
 */
public class TransportTrinoTaskResultsAckAction
    extends HandledTransportAction<TrinoTaskResultsAckRequest, TrinoTaskResultsAckResponse> {

  private static final Logger LOG =
      LogManager.getLogger(TransportTrinoTaskResultsAckAction.class);

  private final OpenSearchSqlTaskManager taskManager;

  @Inject
  public TransportTrinoTaskResultsAckAction(
      TransportService transportService,
      ActionFilters actionFilters,
      OpenSearchSqlTaskManager taskManager) {
    super(
        TrinoTaskResultsAckAction.NAME,
        transportService,
        actionFilters,
        TrinoTaskResultsAckRequest::new);
    this.taskManager = taskManager;
  }

  @Override
  protected void doExecute(
      Task task,
      TrinoTaskResultsAckRequest request,
      ActionListener<TrinoTaskResultsAckResponse> listener) {
    try {
      taskManager.acknowledgeTaskResults(
          TaskId.valueOf(request.getTaskId()),
          new OutputBufferId(request.getBufferId()),
          request.getToken());
      listener.onResponse(new TrinoTaskResultsAckResponse());
    } catch (Exception e) {
      LOG.error(
          "Failed to ack results for taskId={}, bufferId={}",
          request.getTaskId(),
          request.getBufferId(),
          e);
      listener.onFailure(e);
    }
  }
}
