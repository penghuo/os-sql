/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.exchange.serde;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;

/**
 * A framed chunk of exchange data containing one or more {@link DqeDataPage}s.
 *
 * <p>Chunks are the unit of transport between data nodes and the coordinator. Each chunk carries:
 *
 * <ul>
 *   <li>Query/stage/partition routing metadata
 *   <li>A sequence number for ordering and duplicate detection
 *   <li>One or more serialized pages
 *   <li>An {@code isLast} flag indicating the final chunk for a partition
 *   <li>Uncompressed byte count for memory accounting
 * </ul>
 *
 * <p>The default maximum chunk size is 1MB (configurable via {@code
 * plugins.dqe.exchange_chunk_size}).
 */
public class DqeExchangeChunk implements Writeable {

  /** Default maximum chunk size in bytes (1MB). */
  public static final long DEFAULT_MAX_CHUNK_BYTES = 1024L * 1024L;

  private final String queryId;
  private final int stageId;
  private final int partitionId;
  private final long sequenceNumber;
  private final List<DqeDataPage> pages;
  private final boolean isLast;
  private final long uncompressedBytes;

  /**
   * Create a new exchange chunk.
   *
   * @param queryId query identifier
   * @param stageId stage identifier
   * @param partitionId partition identifier
   * @param sequenceNumber monotonically increasing sequence number
   * @param pages the data pages in this chunk
   * @param isLast whether this is the last chunk for the partition
   * @param uncompressedBytes total uncompressed size of all pages
   */
  public DqeExchangeChunk(
      String queryId,
      int stageId,
      int partitionId,
      long sequenceNumber,
      List<DqeDataPage> pages,
      boolean isLast,
      long uncompressedBytes) {
    this.queryId = Objects.requireNonNull(queryId, "queryId must not be null");
    this.stageId = stageId;
    this.partitionId = partitionId;
    this.sequenceNumber = sequenceNumber;
    this.pages =
        Collections.unmodifiableList(
            new ArrayList<>(Objects.requireNonNull(pages, "pages must not be null")));
    this.isLast = isLast;
    this.uncompressedBytes = uncompressedBytes;
  }

  /**
   * Deserialize from a StreamInput.
   *
   * @param in the stream input
   * @throws IOException if deserialization fails
   */
  public DqeExchangeChunk(StreamInput in) throws IOException {
    this.queryId = in.readString();
    this.stageId = in.readVInt();
    this.partitionId = in.readVInt();
    this.sequenceNumber = in.readVLong();
    int pageCount = in.readVInt();
    List<DqeDataPage> readPages = new ArrayList<>(pageCount);
    for (int i = 0; i < pageCount; i++) {
      readPages.add(new DqeDataPage(in));
    }
    this.pages = Collections.unmodifiableList(readPages);
    this.isLast = in.readBoolean();
    this.uncompressedBytes = in.readVLong();
  }

  @Override
  public void writeTo(StreamOutput out) throws IOException {
    out.writeString(queryId);
    out.writeVInt(stageId);
    out.writeVInt(partitionId);
    out.writeVLong(sequenceNumber);
    out.writeVInt(pages.size());
    for (DqeDataPage page : pages) {
      page.writeTo(out);
    }
    out.writeBoolean(isLast);
    out.writeVLong(uncompressedBytes);
  }

  /** Query identifier. */
  public String getQueryId() {
    return queryId;
  }

  /** Stage identifier. */
  public int getStageId() {
    return stageId;
  }

  /** Partition identifier. */
  public int getPartitionId() {
    return partitionId;
  }

  /** Monotonically increasing sequence number within this partition. */
  public long getSequenceNumber() {
    return sequenceNumber;
  }

  /** The data pages in this chunk. */
  public List<DqeDataPage> getPages() {
    return pages;
  }

  /** Whether this is the last chunk for the partition. */
  public boolean isLast() {
    return isLast;
  }

  /** Total uncompressed size of all pages in bytes. */
  public long getUncompressedBytes() {
    return uncompressedBytes;
  }

  /** Total compressed size of all pages in bytes. */
  public long getCompressedBytes() {
    long total = 0;
    for (DqeDataPage page : pages) {
      total += page.getCompressedSizeInBytes();
    }
    return total;
  }
}
