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

/**
 * Request to fetch pages from an upstream task's output buffer. Part of the token-based
 * exactly-once delivery protocol:
 *
 * <ol>
 *   <li>Request with token N
 *   <li>Receive pages [N, N+K)
 *   <li>Ack token N+K (via {@link TrinoTaskResultsAckRequest})
 *   <li>Next request with token N+K
 * </ol>
 */
public class TrinoTaskResultsRequest extends ActionRequest {

  private final String taskId;
  private final int bufferId;
  private final long token;
  private final long maxSizeBytes;

  public TrinoTaskResultsRequest(String taskId, int bufferId, long token, long maxSizeBytes) {
    this.taskId = taskId;
    this.bufferId = bufferId;
    this.token = token;
    this.maxSizeBytes = maxSizeBytes;
  }

  public TrinoTaskResultsRequest(StreamInput in) throws IOException {
    super(in);
    this.taskId = in.readString();
    this.bufferId = in.readVInt();
    this.token = in.readVLong();
    this.maxSizeBytes = in.readVLong();
  }

  @Override
  public void writeTo(StreamOutput out) throws IOException {
    super.writeTo(out);
    out.writeString(taskId);
    out.writeVInt(bufferId);
    out.writeVLong(token);
    out.writeVLong(maxSizeBytes);
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

  public long getMaxSizeBytes() {
    return maxSizeBytes;
  }
}
