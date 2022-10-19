/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.sql.opensearch.executor.task;

import java.util.Set;
import lombok.SneakyThrows;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.client.node.NodeClient;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.sql.executor.StateChangeListener;
import org.opensearch.sql.executor.task.Task;
import org.opensearch.sql.executor.task.TaskExecutionInfo;
import org.opensearch.sql.executor.task.TaskId;
import org.opensearch.sql.opensearch.executor.transport.QLTaskAction;
import org.opensearch.sql.opensearch.executor.transport.QLTaskRequest;
import org.opensearch.sql.opensearch.executor.transport.QLTaskResponse;
import org.opensearch.sql.opensearch.executor.transport.QLTaskType;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.storage.splits.Split;

public class TransportTask extends Task {

  private static final Logger LOG = LogManager.getLogger();

  private final NodeClient client;

  private final DiscoveryNode node;

  private final Split split;

  private final Set<TaskId> sourceTaskIds;

  public TransportTask(TaskId taskId,
                       LogicalPlan plan,
                       NodeClient client,
                       DiscoveryNode node,
                       Split split,
                       Set<TaskId> sourceTaskIds) {
    super(taskId, plan);
    this.client = client;
    this.node = node;
    this.split = split;
    this.sourceTaskIds = sourceTaskIds;
  }

  @Override
  public void execute(StateChangeListener<TaskExecutionInfo> listener) {
    client.execute(
        QLTaskAction.INSTANCE,
        new QLTaskRequest(
            QLTaskType.CREATE, this.getTaskId(), this.node, this.getPlan(), this.split, sourceTaskIds),
        new ActionListener<>() {
          @SneakyThrows
          @Override
          public void onResponse(QLTaskResponse resp) {
            LOG.info("Task: {} receive response {}", taskId, resp);
            listener.stateChanged(resp.getTaskExecutionInfo());
          }

          @Override
          public void onFailure(Exception e) {
            LOG.error("[Task Service] unexpected exception.", e);
          }
        });
  }

  @Override
  public void cancel() {
    // todo, cancel task.
  }

  @Override
  public TaskExecutionInfo taskExecutionInfo() {
    return null;
  }
}
