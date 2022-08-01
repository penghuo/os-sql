/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.task;

import java.io.IOException;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import org.opensearch.common.io.stream.NamedWriteable;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;

@RequiredArgsConstructor
public class TaskId implements NamedWriteable {
  private final String id;

  public static TaskId taskId() {
    return new TaskId("task-" + UUID.randomUUID());
  }

  public TaskId(StreamInput in) throws IOException {
    this.id = in.readString();
  }

  @Override
  public String getWriteableName() {
    return "TASKID";
  }

  @Override
  public void writeTo(StreamOutput out) throws IOException {
    out.writeString(id);
  }
}
