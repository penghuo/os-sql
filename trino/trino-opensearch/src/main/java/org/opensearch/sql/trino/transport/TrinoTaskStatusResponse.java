/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.trino.transport;

import java.io.IOException;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

/** Response containing serialized TaskInfo JSON for a status poll. */
public class TrinoTaskStatusResponse extends ActionResponse {

  private final byte[] taskInfoJson;

  public TrinoTaskStatusResponse(byte[] taskInfoJson) {
    this.taskInfoJson = taskInfoJson;
  }

  public TrinoTaskStatusResponse(StreamInput in) throws IOException {
    super(in);
    this.taskInfoJson = in.readByteArray();
  }

  @Override
  public void writeTo(StreamOutput out) throws IOException {
    out.writeByteArray(taskInfoJson);
  }

  public byte[] getTaskInfoJson() {
    return taskInfoJson;
  }
}
