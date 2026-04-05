/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.trino.transport;

import java.io.IOException;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

/** Request to poll the status of a Trino task on a worker node. */
public class TrinoTaskStatusRequest extends ActionRequest {

  private final String taskId;

  public TrinoTaskStatusRequest(String taskId) {
    this.taskId = taskId;
  }

  public TrinoTaskStatusRequest(StreamInput in) throws IOException {
    super(in);
    this.taskId = in.readString();
  }

  @Override
  public void writeTo(StreamOutput out) throws IOException {
    super.writeTo(out);
    out.writeString(taskId);
  }

  @Override
  public ActionRequestValidationException validate() {
    return null;
  }

  public String getTaskId() {
    return taskId;
  }
}
