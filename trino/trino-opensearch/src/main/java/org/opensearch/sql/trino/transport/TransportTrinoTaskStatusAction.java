/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.trino.transport;

import io.trino.execution.TaskId;
import io.trino.execution.TaskInfo;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.sql.trino.plugin.TrinoServiceHolder;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

/** Handles {@code trino:task/status} on the worker node. Returns current TaskInfo. */
public class TransportTrinoTaskStatusAction
    extends HandledTransportAction<TrinoTaskStatusRequest, TrinoTaskStatusResponse> {

  private static final Logger LOG = LogManager.getLogger(TransportTrinoTaskStatusAction.class);

  @Inject
  public TransportTrinoTaskStatusAction(
      TransportService transportService, ActionFilters actionFilters) {
    super(
        TrinoTaskStatusAction.NAME,
        transportService,
        actionFilters,
        TrinoTaskStatusRequest::new);
  }

  @Override
  protected void doExecute(
      Task task,
      TrinoTaskStatusRequest request,
      ActionListener<TrinoTaskStatusResponse> listener) {
    try {
      if (!TrinoServiceHolder.isInitialized()) {
        listener.onFailure(new IllegalStateException("Trino engine not initialized"));
        return;
      }
      TaskInfo info =
          TrinoServiceHolder.getInstance()
              .getTaskManager()
              .getTaskInfo(TaskId.valueOf(request.getTaskId()));
      listener.onResponse(
          new TrinoTaskStatusResponse(
              TrinoServiceHolder.getInstance().getCodec().serializeTaskInfo(info)));
    } catch (Exception e) {
      LOG.error("Failed to get task status for taskId={}", request.getTaskId(), e);
      listener.onFailure(e);
    }
  }
}
