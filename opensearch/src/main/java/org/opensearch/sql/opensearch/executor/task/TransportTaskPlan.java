/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.task;

import static org.opensearch.sql.opensearch.data.value.OpenSearchExprValueFactory.OBJECT_MAPPER;

import java.io.IOException;
import java.util.List;
import java.util.function.Consumer;
import lombok.Getter;
import lombok.Setter;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.io.stream.NamedWriteable;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.planner.splits.Split;
import org.opensearch.sql.planner.logical.LogicalPlan;


public class TransportTaskPlan implements TaskPlan, NamedWriteable {
  @Getter
  private TaskId taskId;

  // todo, how to ser/de logicalPlan
  private final LogicalPlan logicalPlan;

  @Getter
  private final TaskNode node;

  public TransportTaskPlan(LogicalPlan logicalPlan, TaskNode node) {
    this.taskId = TaskId.taskId();
    this.logicalPlan = logicalPlan;
    this.node = node;
  }

  public TransportTaskPlan(StreamInput in) throws IOException {
    this.taskId = in.readNamedWriteable(TaskId.class);
    this.logicalPlan = OBJECT_MAPPER.readValue(in.readString(), LogicalPlan.class);
    this.node = OBJECT_MAPPER.readValue(in.readString(), TaskNode.class);
  }

  @Override
  public void writeTo(StreamOutput out) throws IOException {
    out.writeNamedWriteable(taskId);
    out.writeString(OBJECT_MAPPER.writeValueAsString(logicalPlan));
    out.writeString(OBJECT_MAPPER.writeValueAsString(node));
  }

  public void execute(List<Split> splitList) {

  }

  @Override
  public String getWriteableName() {
    return "TaskPlan";
  }

  @Override
  public LogicalPlan getPlan() {
    return logicalPlan;
  }
}
