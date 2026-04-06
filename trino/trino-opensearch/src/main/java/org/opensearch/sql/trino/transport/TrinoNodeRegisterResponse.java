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
 * Response to a node registration. Includes the receiver's own Trino HTTP URL so the sender can
 * register it (bidirectional URL exchange in one round trip).
 */
public class TrinoNodeRegisterResponse extends ActionResponse {

  private final boolean success;
  private final String receiverNodeId;
  private final String receiverTrinoHttpUrl;

  public TrinoNodeRegisterResponse(boolean success, String receiverNodeId,
      String receiverTrinoHttpUrl) {
    this.success = success;
    this.receiverNodeId = receiverNodeId != null ? receiverNodeId : "";
    this.receiverTrinoHttpUrl = receiverTrinoHttpUrl != null ? receiverTrinoHttpUrl : "";
  }

  public TrinoNodeRegisterResponse(StreamInput in) throws IOException {
    super(in);
    this.success = in.readBoolean();
    this.receiverNodeId = in.readString();
    this.receiverTrinoHttpUrl = in.readString();
  }

  @Override
  public void writeTo(StreamOutput out) throws IOException {
    out.writeBoolean(success);
    out.writeString(receiverNodeId);
    out.writeString(receiverTrinoHttpUrl);
  }

  public boolean isSuccess() {
    return success;
  }

  public String getReceiverNodeId() {
    return receiverNodeId;
  }

  public String getReceiverTrinoHttpUrl() {
    return receiverTrinoHttpUrl;
  }
}
