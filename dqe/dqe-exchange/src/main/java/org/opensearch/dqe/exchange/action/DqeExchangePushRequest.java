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
 * Transport request carrying an exchange chunk (data pages) from a producer to a consumer.
 *
 * <p>In the gather exchange pattern, shard-side sinks send this request to the coordinator-side
 * exchange buffer.
 */
public class DqeExchangePushRequest extends TransportRequest {

  private final String queryId;
  private final int stageId;
  private final int partitionId;
  private final long sequenceNumber;
  private final byte[] serializedPages;
  private final boolean isLast;
  private final long uncompressedBytes;
  private final int pageCount;

  /**
   * Construct a push request.
   *
   * @param queryId query identifier
   * @param stageId stage identifier
   * @param partitionId partition identifier (0 for gather exchange)
   * @param sequenceNumber monotonically increasing sequence number per channel
   * @param serializedPages compressed serialized page data
   * @param isLast true if this is the final chunk for this partition
   * @param uncompressedBytes uncompressed size for memory accounting
   * @param pageCount number of pages in this chunk
   */
  public DqeExchangePushRequest(
      String queryId,
      int stageId,
      int partitionId,
      long sequenceNumber,
      byte[] serializedPages,
      boolean isLast,
      long uncompressedBytes,
      int pageCount) {
    this.queryId = Objects.requireNonNull(queryId, "queryId must not be null");
    this.stageId = stageId;
    this.partitionId = partitionId;
    this.sequenceNumber = sequenceNumber;
    this.serializedPages =
        Objects.requireNonNull(serializedPages, "serializedPages must not be null");
    this.isLast = isLast;
    this.uncompressedBytes = uncompressedBytes;
    this.pageCount = pageCount;
  }

  public DqeExchangePushRequest(StreamInput in) throws IOException {
    super(in);
    this.queryId = in.readString();
    this.stageId = in.readVInt();
    this.partitionId = in.readVInt();
    this.sequenceNumber = in.readVLong();
    this.serializedPages = in.readByteArray();
    this.isLast = in.readBoolean();
    this.uncompressedBytes = in.readVLong();
    this.pageCount = in.readVInt();
  }

  @Override
  public void writeTo(StreamOutput out) throws IOException {
    super.writeTo(out);
    out.writeString(queryId);
    out.writeVInt(stageId);
    out.writeVInt(partitionId);
    out.writeVLong(sequenceNumber);
    out.writeByteArray(serializedPages);
    out.writeBoolean(isLast);
    out.writeVLong(uncompressedBytes);
    out.writeVInt(pageCount);
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

  public long getSequenceNumber() {
    return sequenceNumber;
  }

  public byte[] getSerializedPages() {
    return serializedPages;
  }

  public boolean isLast() {
    return isLast;
  }

  public long getUncompressedBytes() {
    return uncompressedBytes;
  }

  public int getPageCount() {
    return pageCount;
  }
}
