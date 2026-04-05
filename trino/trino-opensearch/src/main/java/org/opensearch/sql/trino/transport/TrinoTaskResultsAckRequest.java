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

/** Request to acknowledge consumed pages, allowing the upstream OutputBuffer to release memory. */
public class TrinoTaskResultsAckRequest extends ActionRequest {

  private final String taskId;
  private final int bufferId;
  private final long token;

  public TrinoTaskResultsAckRequest(String taskId, int bufferId, long token) {
    this.taskId = taskId;
    this.bufferId = bufferId;
    this.token = token;
  }

  public TrinoTaskResultsAckRequest(StreamInput in) throws IOException {
    super(in);
    this.taskId = in.readString();
    this.bufferId = in.readVInt();
    this.token = in.readVLong();
  }

  @Override
  public void writeTo(StreamOutput out) throws IOException {
    super.writeTo(out);
    out.writeString(taskId);
    out.writeVInt(bufferId);
    out.writeVLong(token);
  }

  @Override
  public ActionRequestValidationException validate() {
    return null;
  }

  public String getTaskId() {
    return taskId;
  }

  public int getBufferId() {
    return bufferId;
  }

  public long getToken() {
    return token;
  }
}
