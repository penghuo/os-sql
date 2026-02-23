/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.exchange.action;

import java.io.IOException;
import java.util.Objects;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

/** Transport response acknowledging that a data node has received a stage execution request. */
public class DqeStageExecuteResponse extends ActionResponse {

  private final String queryId;
  private final int stageId;
  private final boolean accepted;

  public DqeStageExecuteResponse(String queryId, int stageId, boolean accepted) {
    this.queryId = Objects.requireNonNull(queryId, "queryId must not be null");
    this.stageId = stageId;
    this.accepted = accepted;
  }

  public DqeStageExecuteResponse(StreamInput in) throws IOException {
    super(in);
    this.queryId = in.readString();
    this.stageId = in.readVInt();
    this.accepted = in.readBoolean();
  }

  @Override
  public void writeTo(StreamOutput out) throws IOException {
    out.writeString(queryId);
    out.writeVInt(stageId);
    out.writeBoolean(accepted);
  }

  public String getQueryId() {
    return queryId;
  }

  public int getStageId() {
    return stageId;
  }

  public boolean isAccepted() {
    return accepted;
  }
}
