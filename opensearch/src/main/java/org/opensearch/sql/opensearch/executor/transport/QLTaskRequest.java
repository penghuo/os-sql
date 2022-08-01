/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.transport;

import java.io.IOException;
import lombok.Getter;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.sql.opensearch.executor.task.TransportTaskPlan;

public class QLTaskRequest extends ActionRequest {
  @Getter
  private QLTaskType taskType;

  @Getter
  private TransportTaskPlan taskPlan;

  public QLTaskRequest(QLTaskType taskType, TransportTaskPlan taskPlan) {
    super();
    this.taskType = taskType;
    this.taskPlan = taskPlan;
  }

  public QLTaskRequest(StreamInput in) throws IOException {
    super();
    this.taskType = in.readEnum(QLTaskType.class);
    this.taskPlan = in.readNamedWriteable(TransportTaskPlan.class);
  }

  @Override
  public void writeTo(StreamOutput out) throws IOException {
    super.writeTo(out);
    out.writeEnum(taskType);
    out.writeNamedWriteable(taskPlan);
  }

  @Override
  public ActionRequestValidationException validate() {
    // do nothing
    return null;
  }
}
