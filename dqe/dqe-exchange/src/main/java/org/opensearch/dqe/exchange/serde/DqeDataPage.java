/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.exchange.serde;

import io.trino.spi.Page;
import java.io.IOException;
import java.util.Objects;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;

/**
 * A serialized Trino {@link Page} that implements OpenSearch's {@link Writeable} for transport.
 *
 * <p>Wraps a compressed page payload with metadata for memory accounting:
 *
 * <ul>
 *   <li>{@code compressedData} — the Deflate-compressed page bytes
 *   <li>{@code uncompressedSizeInBytes} — original size before compression
 *   <li>{@code positionCount} — number of rows in the page
 * </ul>
 *
 * <p>The page is serialized lazily: constructing from a {@link Page} compresses immediately, while
 * constructing from a {@link StreamInput} defers decompression until {@link #getPage()} is called.
 */
public class DqeDataPage implements Writeable {

  private final byte[] compressedData;
  private final long uncompressedSizeInBytes;
  private final int positionCount;

  // Lazily deserialized; null until getPage() is called on receiver side.
  // Volatile to ensure safe publication across threads.
  private transient volatile Page page;

  /**
   * Create a DqeDataPage from a Trino Page. Serializes and compresses immediately.
   *
   * @param page the Page to wrap
   */
  public DqeDataPage(Page page) {
    Objects.requireNonNull(page, "page must not be null");
    this.page = page;
    this.positionCount = page.getPositionCount();
    this.compressedData = PageSerializer.serialize(page);
    this.uncompressedSizeInBytes = PageSerializer.estimateSerializedSize(page);
  }

  /**
   * Deserialize a DqeDataPage from a StreamInput.
   *
   * @param in the stream input
   * @throws IOException if deserialization fails
   */
  public DqeDataPage(StreamInput in) throws IOException {
    this.uncompressedSizeInBytes = in.readVLong();
    this.positionCount = in.readVInt();
    this.compressedData = in.readByteArray();
    // page is lazily deserialized
  }

  @Override
  public void writeTo(StreamOutput out) throws IOException {
    out.writeVLong(uncompressedSizeInBytes);
    out.writeVInt(positionCount);
    out.writeByteArray(compressedData);
  }

  /**
   * Get the Trino Page. Decompresses and deserializes on first call if constructed from stream.
   *
   * @return the deserialized Page
   */
  public Page getPage() {
    if (page == null) {
      page = PageDeserializer.deserialize(compressedData, uncompressedSizeInBytes);
    }
    return page;
  }

  /** Uncompressed size of the serialized page in bytes. */
  public long getUncompressedSizeInBytes() {
    return uncompressedSizeInBytes;
  }

  /** Compressed size of the serialized page in bytes. */
  public long getCompressedSizeInBytes() {
    return compressedData.length;
  }

  /** Number of rows in this page. */
  public int getPositionCount() {
    return positionCount;
  }
}
