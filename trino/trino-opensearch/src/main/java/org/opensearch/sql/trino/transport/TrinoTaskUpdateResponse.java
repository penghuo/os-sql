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
 * Response from a task update. Contains serialized TaskInfo JSON (task state, stats, error if any).
 */
public class TrinoTaskUpdateResponse extends ActionResponse {

  private final byte[] taskInfoJson;

  public TrinoTaskUpdateResponse(byte[] taskInfoJson) {
    this.taskInfoJson = taskInfoJson;
  }

  public TrinoTaskUpdateResponse(StreamInput in) throws IOException {
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
