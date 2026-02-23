/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.exchange.action;

import java.io.IOException;
import java.util.Objects;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.transport.TransportRequest;

/**
 * Transport request to cancel a running stage on a data node.
 *
 * <p>Sent by the coordinator to all data nodes with active stages for a query. Each data node
 * interrupts driver threads, aborts exchange producers, drains exchange buffers and releases
 * memory, releases PITs, and sends a cancellation ACK.
 */
public class DqeStageCancelRequest extends TransportRequest {

  private final String queryId;
  private final int stageId;

  /**
   * Construct a stage cancel request.
   *
   * @param queryId query identifier
   * @param stageId stage identifier
   */
  public DqeStageCancelRequest(String queryId, int stageId) {
    this.queryId = Objects.requireNonNull(queryId, "queryId must not be null");
    this.stageId = stageId;
  }

  /** Deserialization constructor. */
  public DqeStageCancelRequest(StreamInput in) throws IOException {
    super(in);
    this.queryId = in.readString();
    this.stageId = in.readVInt();
  }

  @Override
  public void writeTo(StreamOutput out) throws IOException {
    super.writeTo(out);
    out.writeString(queryId);
    out.writeVInt(stageId);
  }

  public String getQueryId() {
    return queryId;
  }

  public int getStageId() {
    return stageId;
  }
}
