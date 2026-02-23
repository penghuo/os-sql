/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.exchange.serde;

import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.trino.spi.Page;
import io.trino.spi.block.BlockEncodingSerde;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;

/**
 * Serializes Trino {@link Page} objects to compressed byte arrays for transport.
 *
 * <p>The serialization format is:
 *
 * <ol>
 *   <li>int: positionCount
 *   <li>int: channelCount (number of blocks)
 *   <li>For each block: the block encoding name + block data (via {@link BlockEncodingSerde})
 * </ol>
 *
 * <p>The raw bytes are then compressed using JDK Deflate compression (zlib).
 */
public final class PageSerializer {

  private PageSerializer() {}

  /**
   * Serialize a Page to a compressed byte array.
   *
   * @param page the Page to serialize
   * @return compressed bytes
   */
  public static byte[] serialize(Page page) {
    BlockEncodingSerde serde = DqeBlockEncodingSerde.getInstance();

    // Serialize to raw bytes
    int estimatedSize = Math.max(64, (int) Math.min(page.getSizeInBytes(), Integer.MAX_VALUE));
    DynamicSliceOutput output = new DynamicSliceOutput(estimatedSize);

    output.writeInt(page.getPositionCount());
    output.writeInt(page.getChannelCount());

    for (int channel = 0; channel < page.getChannelCount(); channel++) {
      serde.writeBlock(output, page.getBlock(channel));
    }

    Slice rawSlice = output.slice();
    byte[] rawBytes = rawSlice.getBytes();

    // Compress
    return compress(rawBytes);
  }

  /**
   * Estimate the serialized size of a Page without actually serializing it.
   *
   * @param page the Page to estimate
   * @return estimated size in bytes
   */
  public static long estimateSerializedSize(Page page) {
    // Use getSizeInBytes as a rough estimate; actual compressed size will be smaller
    return page.getSizeInBytes();
  }

  private static byte[] compress(byte[] data) {
    Deflater deflater = new Deflater(Deflater.DEFAULT_COMPRESSION);
    try {
      ByteArrayOutputStream baos = new ByteArrayOutputStream(data.length / 2 + 64);
      try (DeflaterOutputStream dos = new DeflaterOutputStream(baos, deflater)) {
        dos.write(data);
      }
      return baos.toByteArray();
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to compress page data", e);
    } finally {
      deflater.end();
    }
  }
}
