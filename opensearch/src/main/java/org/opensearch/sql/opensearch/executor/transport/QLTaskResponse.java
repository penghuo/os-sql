/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.transport;

import static org.opensearch.sql.opensearch.data.value.OpenSearchExprValueFactory.OBJECT_MAPPER;

import java.io.IOException;
import lombok.Getter;
import lombok.ToString;
import org.opensearch.action.ActionResponse;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.sql.executor.task.TaskExecutionInfo;

@ToString
@Getter
public class QLTaskResponse extends ActionResponse {

  private TaskExecutionInfo taskExecutionInfo;

  public QLTaskResponse(TaskExecutionInfo taskState) {
    super();
    this.taskExecutionInfo = taskState;
  }

  public QLTaskResponse(StreamInput in) throws IOException {
    this.taskExecutionInfo = OBJECT_MAPPER.readValue(in.readString(), TaskExecutionInfo.class);
  }

  @Override
  public void writeTo(StreamOutput out) throws IOException {
    out.writeString(OBJECT_MAPPER.writeValueAsString(taskExecutionInfo));
  }
}
