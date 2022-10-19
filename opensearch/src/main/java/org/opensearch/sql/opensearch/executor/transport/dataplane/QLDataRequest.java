/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.sql.opensearch.executor.transport.dataplane;

import static org.opensearch.sql.opensearch.data.value.OpenSearchExprValueFactory.OBJECT_MAPPER;

import java.io.IOException;
import lombok.Getter;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.sql.executor.task.TaskId;

public class QLDataRequest extends ActionRequest {

  @Getter
  private TaskId taskId;

  public QLDataRequest(TaskId taskId) {
    super();
    this.taskId = taskId;
  }

  public QLDataRequest(StreamInput in) throws IOException {
    super();
    this.taskId = OBJECT_MAPPER.readValue(in.readString(), TaskId.class);
  }

  @Override
  public void writeTo(StreamOutput out) throws IOException {
    super.writeTo(out);
    out.writeString(OBJECT_MAPPER.writeValueAsString(taskId));
  }

  @Override
  public ActionRequestValidationException validate() {
    // do nothing
    return null;
  }
}
