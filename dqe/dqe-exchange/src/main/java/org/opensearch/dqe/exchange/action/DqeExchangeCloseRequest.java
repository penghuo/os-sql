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
 * Transport request signaling that a producer has finished sending data for a given partition.
 *
 * <p>Sent by {@code GatherExchangeSink} after all pages have been pushed and the final chunk sent.
 * The consumer uses this to track how many producers have completed.
 */
public class DqeExchangeCloseRequest extends TransportRequest {

  private final String queryId;
  private final int stageId;
  private final int partitionId;

  /**
   * Construct a close request.
   *
   * @param queryId query identifier
   * @param stageId stage identifier
   * @param partitionId partition that has completed
   */
  public DqeExchangeCloseRequest(String queryId, int stageId, int partitionId) {
    this.queryId = Objects.requireNonNull(queryId, "queryId must not be null");
    this.stageId = stageId;
    this.partitionId = partitionId;
  }

  /** Deserialization constructor. */
  public DqeExchangeCloseRequest(StreamInput in) throws IOException {
    super(in);
    this.queryId = in.readString();
    this.stageId = in.readVInt();
    this.partitionId = in.readVInt();
  }

  @Override
  public void writeTo(StreamOutput out) throws IOException {
    super.writeTo(out);
    out.writeString(queryId);
    out.writeVInt(stageId);
    out.writeVInt(partitionId);
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
}
