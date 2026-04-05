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

/** Handles {@code trino:task/cancel} on the worker node. Cancels a running task. */
public class TransportTrinoTaskCancelAction
    extends HandledTransportAction<TrinoTaskCancelRequest, TrinoTaskCancelResponse> {

  private static final Logger LOG = LogManager.getLogger(TransportTrinoTaskCancelAction.class);

  @Inject
  public TransportTrinoTaskCancelAction(
      TransportService transportService, ActionFilters actionFilters) {
    super(
        TrinoTaskCancelAction.NAME,
        transportService,
        actionFilters,
        TrinoTaskCancelRequest::new);
  }

  @Override
  protected void doExecute(
      Task task,
      TrinoTaskCancelRequest request,
      ActionListener<TrinoTaskCancelResponse> listener) {
    try {
      if (!TrinoServiceHolder.isInitialized()) {
        listener.onFailure(new IllegalStateException("Trino engine not initialized"));
        return;
      }
      TaskInfo info =
          TrinoServiceHolder.getInstance()
              .getTaskManager()
              .cancelTask(TaskId.valueOf(request.getTaskId()));
      listener.onResponse(
          new TrinoTaskCancelResponse(
              TrinoServiceHolder.getInstance().getCodec().serializeTaskInfo(info)));
    } catch (Exception e) {
      LOG.error("Failed to cancel task for taskId={}", request.getTaskId(), e);
      listener.onFailure(e);
    }
  }
}
