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
 * Request to create or update a Trino task on a worker node. Carries PlanFragment, split
 * assignments, output buffer config, and session as JSON bytes serialized by Trino's own
 * ObjectMapper via {@link TrinoJsonCodec}.
 *
 * <p>All Trino objects are serialized to JSON bytes and passed through OpenSearch's transport layer
 * as opaque byte arrays. This avoids implementing custom serialization for every Trino internal
 * type.
 */
public class TrinoTaskUpdateRequest extends ActionRequest {

  private final String taskId;
  private final byte[] planFragmentJson;
  private final byte[] splitAssignmentsJson;
  private final byte[] outputBuffersJson;
  private final byte[] sessionJson;

  public TrinoTaskUpdateRequest(
      String taskId,
      byte[] planFragmentJson,
      byte[] splitAssignmentsJson,
      byte[] outputBuffersJson,
      byte[] sessionJson) {
    this.taskId = taskId;
    this.planFragmentJson = planFragmentJson;
    this.splitAssignmentsJson = splitAssignmentsJson;
    this.outputBuffersJson = outputBuffersJson;
    this.sessionJson = sessionJson;
  }

  public TrinoTaskUpdateRequest(StreamInput in) throws IOException {
    super(in);
    this.taskId = in.readString();
    this.planFragmentJson = in.readByteArray();
    this.splitAssignmentsJson = in.readByteArray();
    this.outputBuffersJson = in.readByteArray();
    this.sessionJson = in.readByteArray();
  }

  @Override
  public void writeTo(StreamOutput out) throws IOException {
    super.writeTo(out);
    out.writeString(taskId);
    out.writeByteArray(planFragmentJson);
    out.writeByteArray(splitAssignmentsJson);
    out.writeByteArray(outputBuffersJson);
    out.writeByteArray(sessionJson);
  }

  @Override
  public ActionRequestValidationException validate() {
    return null;
  }

  public String getTaskId() {
    return taskId;
  }

  public byte[] getPlanFragmentJson() {
    return planFragmentJson;
  }

  public byte[] getSplitAssignmentsJson() {
    return splitAssignmentsJson;
  }

  public byte[] getOutputBuffersJson() {
    return outputBuffersJson;
  }

  public byte[] getSessionJson() {
    return sessionJson;
  }
}
