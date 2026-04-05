/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.trino.transport;

import java.io.IOException;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

/** Response from a task cancel. Contains serialized TaskInfo JSON with final state. */
public class TrinoTaskCancelResponse extends ActionResponse {

  private final byte[] taskInfoJson;

  public TrinoTaskCancelResponse(byte[] taskInfoJson) {
    this.taskInfoJson = taskInfoJson;
  }

  public TrinoTaskCancelResponse(StreamInput in) throws IOException {
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
