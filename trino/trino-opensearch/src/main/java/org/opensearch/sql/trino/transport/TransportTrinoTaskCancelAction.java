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
import org.opensearch.sql.trino.execution.OpenSearchSqlTaskManager;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

/** Handles {@code trino:task/cancel} on the worker node. Cancels a running task. */
public class TransportTrinoTaskCancelAction
    extends HandledTransportAction<TrinoTaskCancelRequest, TrinoTaskCancelResponse> {

  private static final Logger LOG = LogManager.getLogger(TransportTrinoTaskCancelAction.class);

  private final OpenSearchSqlTaskManager taskManager;
  private final TrinoJsonCodec codec;

  @Inject
  public TransportTrinoTaskCancelAction(
      TransportService transportService,
      ActionFilters actionFilters,
      OpenSearchSqlTaskManager taskManager,
      TrinoJsonCodec codec) {
    super(
        TrinoTaskCancelAction.NAME,
        transportService,
        actionFilters,
        TrinoTaskCancelRequest::new);
    this.taskManager = taskManager;
    this.codec = codec;
  }

  @Override
  protected void doExecute(
      Task task,
      TrinoTaskCancelRequest request,
      ActionListener<TrinoTaskCancelResponse> listener) {
    try {
      TaskInfo info = taskManager.cancelTask(TaskId.valueOf(request.getTaskId()));
      listener.onResponse(new TrinoTaskCancelResponse(codec.serializeTaskInfo(info)));
    } catch (Exception e) {
      LOG.error("Failed to cancel task for taskId={}", request.getTaskId(), e);
      listener.onFailure(e);
    }
  }
}
