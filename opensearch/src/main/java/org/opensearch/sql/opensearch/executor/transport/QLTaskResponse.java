/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.transport;

import static org.opensearch.sql.opensearch.data.value.OpenSearchExprValueFactory.OBJECT_MAPPER;

import java.io.IOException;
import lombok.Getter;
import org.opensearch.action.ActionResponse;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.sql.opensearch.executor.task.TaskState;

@Getter
public class QLTaskResponse extends ActionResponse {

  private TaskState taskState;

  public QLTaskResponse(TaskState taskState) {
    super();
    this.taskState = taskState;
  }

  public QLTaskResponse(StreamInput in) throws IOException {
    this.taskState = OBJECT_MAPPER.readValue(in.readString(), TaskState.class);
  }

  @Override
  public void writeTo(StreamOutput out) throws IOException {
    out.writeString(OBJECT_MAPPER.writeValueAsString(taskState));
  }
}
