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
 * Transport request to abort an exchange channel.
 *
 * <p>Sent by either side when a failure occurs. The receiver drains the exchange buffer and
 * releases all associated memory.
 */
public class DqeExchangeAbortRequest extends TransportRequest {

  private final String queryId;
  private final int stageId;
  private final int partitionId;
  private final String reason;

  /**
   * Construct an abort request.
   *
   * @param queryId query identifier
   * @param stageId stage identifier
   * @param partitionId partition to abort
   * @param reason human-readable reason for abort
   */
  public DqeExchangeAbortRequest(String queryId, int stageId, int partitionId, String reason) {
    this.queryId = Objects.requireNonNull(queryId, "queryId must not be null");
    this.stageId = stageId;
    this.partitionId = partitionId;
    this.reason = Objects.requireNonNull(reason, "reason must not be null");
  }

  /** Deserialization constructor. */
  public DqeExchangeAbortRequest(StreamInput in) throws IOException {
    super(in);
    this.queryId = in.readString();
    this.stageId = in.readVInt();
    this.partitionId = in.readVInt();
    this.reason = in.readString();
  }

  @Override
  public void writeTo(StreamOutput out) throws IOException {
    super.writeTo(out);
    out.writeString(queryId);
    out.writeVInt(stageId);
    out.writeVInt(partitionId);
    out.writeString(reason);
  }

  public String getQueryId() {
    return queryId;
  }

  public int getStageId() {
    return stageId;
  }

  public int getPartitionId() {
    return partitionId;
  }

  public String getReason() {
    return reason;
  }
}
