/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.trino.transport;

import java.io.IOException;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

/**
 * Response containing TRINO_PAGES binary from an upstream OutputBuffer.
 *
 * <p>The {@code pages} field contains Trino's internal columnar page format as opaque bytes. It is
 * NEVER deserialized by OpenSearch — passed through as-is to preserve performance.
 */
public class TrinoTaskResultsResponse extends ActionResponse {

  private final String taskInstanceId;
  private final long token;
  private final long nextToken;
  private final boolean bufferComplete;
  private final byte[] pages;

  public TrinoTaskResultsResponse(
      String taskInstanceId, long token, long nextToken, boolean bufferComplete, byte[] pages) {
    this.taskInstanceId = taskInstanceId;
    this.token = token;
    this.nextToken = nextToken;
    this.bufferComplete = bufferComplete;
    this.pages = pages;
  }

  public TrinoTaskResultsResponse(StreamInput in) throws IOException {
    super(in);
    this.taskInstanceId = in.readString();
    this.token = in.readVLong();
    this.nextToken = in.readVLong();
    this.bufferComplete = in.readBoolean();
    this.pages = in.readByteArray();
  }

  @Override
  public void writeTo(StreamOutput out) throws IOException {
    out.writeString(taskInstanceId);
    out.writeVLong(token);
    out.writeVLong(nextToken);
    out.writeBoolean(bufferComplete);
    out.writeByteArray(pages);
  }

  public String getTaskInstanceId() {
    return taskInstanceId;
  }

  public long getToken() {
    return token;
  }

  public long getNextToken() {
    return nextToken;
  }

  public boolean isBufferComplete() {
    return bufferComplete;
  }

  public byte[] getPages() {
    return pages;
  }
}
