/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.trino.transport;

import io.trino.Session;
import io.trino.execution.SplitAssignment;
import io.trino.execution.TaskId;
import io.trino.execution.TaskInfo;
import io.trino.execution.buffer.OutputBuffers;
import io.trino.metadata.SessionPropertyManager;
import io.trino.spi.QueryId;
import io.trino.spi.security.Identity;
import io.trino.sql.planner.PlanFragment;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.sql.trino.execution.OpenSearchSqlTaskManager;
import org.opensearch.sql.trino.plugin.TrinoServiceHolder;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

/**
 * Handles {@code trino:task/update} on the worker node. Deserializes the plan fragment and
 * delegates to {@link OpenSearchSqlTaskManager}.
 */
public class TransportTrinoTaskUpdateAction
    extends HandledTransportAction<TrinoTaskUpdateRequest, TrinoTaskUpdateResponse> {

  private static final Logger LOG = LogManager.getLogger(TransportTrinoTaskUpdateAction.class);

  @Inject
  public TransportTrinoTaskUpdateAction(
      TransportService transportService, ActionFilters actionFilters) {
    super(
        TrinoTaskUpdateAction.NAME,
        transportService,
        actionFilters,
        TrinoTaskUpdateRequest::new);
  }

  @Override
  protected void doExecute(
      Task task, TrinoTaskUpdateRequest request, ActionListener<TrinoTaskUpdateResponse> listener) {
    try {
      if (!TrinoServiceHolder.isInitialized()) {
        listener.onFailure(new IllegalStateException("Trino engine not initialized"));
        return;
      }
      OpenSearchSqlTaskManager taskManager = TrinoServiceHolder.getInstance().getTaskManager();
      TrinoJsonCodec codec = TrinoServiceHolder.getInstance().getCodec();

      byte[] fragmentJson = request.getPlanFragmentJson();
      LOG.info("Task update handler for {}: fragmentJson size={}, splitAssignments size={}",
          request.getTaskId(),
          fragmentJson != null ? fragmentJson.length : "null",
          request.getSplitAssignmentsJson() != null ? request.getSplitAssignmentsJson().length : "null");
      Optional<PlanFragment> fragment =
          (fragmentJson != null && fragmentJson.length > 0)
              ? Optional.of(codec.deserializePlanFragment(fragmentJson))
              : Optional.empty();

      List<SplitAssignment> splits =
          codec.deserializeSplitAssignments(request.getSplitAssignmentsJson());
      OutputBuffers buffers = codec.deserializeOutputBuffers(request.getOutputBuffersJson());

      // Create a session from the task ID's query ID. The Session is required by
      // SqlTaskManager.updateTask() and must be non-null.
      TaskId taskId = TaskId.valueOf(request.getTaskId());
      Session session = createSession(taskId.getQueryId());

      TaskInfo info =
          taskManager.updateTask(
              session,
              taskId,
              fragment,
              splits,
              buffers,
              Map.of());

      if (info.getTaskStatus().getState().isDone()) {
        // Log failure details for diagnosis
        var failures = info.getTaskStatus().getFailures();
        for (var failure : failures) {
          LOG.warn("Task {} FAILED: type={}, message={}, cause={}",
              request.getTaskId(), failure.getType(), failure.getMessage(),
              failure.getCause() != null ? failure.getCause().getMessage() : "none");
        }
      }
      listener.onResponse(new TrinoTaskUpdateResponse(codec.serializeTaskInfo(info)));
    } catch (Exception e) {
      LOG.error("Failed to process task update for taskId={}", request.getTaskId(), e);
      listener.onFailure(e);
    }
  }

  private static Session createSession(QueryId queryId) {
    // Use the real SessionPropertyManager from the coordinator — it knows about
    // all system properties (retry_policy, task_concurrency, etc.)
    SessionPropertyManager spm = TrinoServiceHolder.getInstance().getEngine()
        .getQueryRunner().getSessionPropertyManager();
    Identity identity = Identity.ofUser("opensearch");
    return Session.builder(spm)
        .setIdentity(identity)
        .setOriginalIdentity(identity)
        .setSource("opensearch-sql-transport")
        .setQueryId(queryId)
        .build();
  }
}
