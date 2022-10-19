/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.transport;

import static org.opensearch.sql.opensearch.data.value.OpenSearchExprValueFactory.OBJECT_MAPPER;

import com.fasterxml.jackson.core.type.TypeReference;
import java.io.IOException;
import java.util.Set;
import lombok.Getter;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.sql.executor.task.TaskId;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.storage.splits.Split;

public class QLTaskRequest extends ActionRequest {
  @Getter
  private QLTaskType taskType;

  @Getter
  private TaskId taskId;

  @Getter
  private DiscoveryNode node;

  @Getter
  private LogicalPlan plan;

  @Getter
  private Split split;

  @Getter
  private Set<TaskId> sourceTasks;

  public QLTaskRequest(QLTaskType taskType, TaskId taskId, DiscoveryNode node, LogicalPlan plan,
                       Split split, Set<TaskId> sourceTasks) {
    super();
    this.taskType = taskType;
    this.taskId = taskId;
    this.node = node;
    this.plan = plan;
    this.split = split;
    this.sourceTasks = sourceTasks;
  }

  public QLTaskRequest(StreamInput in) throws IOException {
    super();
    this.taskType = in.readEnum(QLTaskType.class);
    this.taskId = OBJECT_MAPPER.readValue(in.readString(), TaskId.class);
    this.node = OBJECT_MAPPER.readValue(in.readString(), DiscoveryNode.class);
    this.plan = OBJECT_MAPPER.readValue(in.readString(), LogicalPlan.class);
    this.split = OBJECT_MAPPER.readValue(in.readString(), Split.class);
    this.sourceTasks = OBJECT_MAPPER.readValue(in.readString(), new TypeReference<>(){});
  }

  @Override
  public void writeTo(StreamOutput out) throws IOException {
    super.writeTo(out);
    out.writeEnum(taskType);
    out.writeString(OBJECT_MAPPER.writeValueAsString(taskId));
    out.writeString(OBJECT_MAPPER.writeValueAsString(node));
    out.writeString(OBJECT_MAPPER.writeValueAsString(plan));
    out.writeString(OBJECT_MAPPER.writeValueAsString(split));
    out.writeString(OBJECT_MAPPER.writeValueAsString(sourceTasks));
  }

  @Override
  public ActionRequestValidationException validate() {
    // do nothing
    return null;
  }
}
