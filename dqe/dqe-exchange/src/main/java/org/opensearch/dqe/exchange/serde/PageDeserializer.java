/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.exchange.serde;

import io.airlift.slice.BasicSliceInput;
import io.airlift.slice.Slices;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockEncodingSerde;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.zip.Inflater;
import java.util.zip.InflaterOutputStream;

/**
 * Deserializes compressed byte arrays back to Trino {@link Page} objects.
 *
 * <p>Reverses the format produced by {@link PageSerializer}: decompresses, then reads position
 * count, channel count, and each block via {@link BlockEncodingSerde}.
 */
public final class PageDeserializer {

  private PageDeserializer() {}

  /**
   * Deserialize a compressed byte array to a Page.
   *
   * @param compressedData the compressed page data
   * @param uncompressedSize the expected uncompressed size (used for buffer allocation)
   * @return the deserialized Page
   */
  public static Page deserialize(byte[] compressedData, long uncompressedSize) {
    byte[] rawBytes = decompress(compressedData, uncompressedSize);

    BlockEncodingSerde serde = DqeBlockEncodingSerde.getInstance();
    BasicSliceInput input = Slices.wrappedBuffer(rawBytes).getInput();

    int positionCount = input.readInt();
    int channelCount = input.readInt();

    Block[] blocks = new Block[channelCount];
    for (int i = 0; i < channelCount; i++) {
      blocks[i] = serde.readBlock(input);
    }

    return new Page(positionCount, blocks);
  }

  private static byte[] decompress(byte[] compressedData, long uncompressedSize) {
    Inflater inflater = new Inflater();
    try {
      int initialSize =
          (int) Math.min(uncompressedSize > 0 ? uncompressedSize : 4096, Integer.MAX_VALUE);
      ByteArrayOutputStream baos = new ByteArrayOutputStream(initialSize);
      try (InflaterOutputStream ios = new InflaterOutputStream(baos, inflater)) {
        ios.write(compressedData);
      }
      return baos.toByteArray();
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to decompress page data", e);
    } finally {
      inflater.end();
    }
  }
}
