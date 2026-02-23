/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.exchange.serde;

import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import io.trino.spi.block.ArrayBlockEncoding;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockEncoding;
import io.trino.spi.block.BlockEncodingSerde;
import io.trino.spi.block.ByteArrayBlockEncoding;
import io.trino.spi.block.DictionaryBlockEncoding;
import io.trino.spi.block.Fixed12BlockEncoding;
import io.trino.spi.block.Int128ArrayBlockEncoding;
import io.trino.spi.block.IntArrayBlockEncoding;
import io.trino.spi.block.LongArrayBlockEncoding;
import io.trino.spi.block.MapBlockEncoding;
import io.trino.spi.block.RowBlockEncoding;
import io.trino.spi.block.RunLengthBlockEncoding;
import io.trino.spi.block.ShortArrayBlockEncoding;
import io.trino.spi.block.VariableWidthBlockEncoding;
import io.trino.spi.type.Type;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Lightweight {@link BlockEncodingSerde} for DQE page serialization.
 *
 * <p>Maps block encoding names to their SPI implementations without requiring the full Trino {@code
 * InternalBlockEncodingSerde} or its {@code BlockEncodingManager} / {@code TypeManager}
 * dependencies.
 *
 * <p>Thread-safe and stateless. A single instance should be shared across the node.
 */
public final class DqeBlockEncodingSerde implements BlockEncodingSerde {

  private static final DqeBlockEncodingSerde INSTANCE = new DqeBlockEncodingSerde();

  private final Map<String, BlockEncoding> encodings;

  private DqeBlockEncodingSerde() {
    this.encodings = new ConcurrentHashMap<>();
    register(new ByteArrayBlockEncoding());
    register(new ShortArrayBlockEncoding());
    register(new IntArrayBlockEncoding());
    register(new LongArrayBlockEncoding());
    register(new Int128ArrayBlockEncoding());
    register(new Fixed12BlockEncoding());
    register(new VariableWidthBlockEncoding());
    register(new ArrayBlockEncoding());
    register(new MapBlockEncoding());
    register(new RowBlockEncoding());
    register(new DictionaryBlockEncoding());
    register(new RunLengthBlockEncoding());
  }

  /** Returns the singleton instance. */
  public static DqeBlockEncodingSerde getInstance() {
    return INSTANCE;
  }

  private void register(BlockEncoding encoding) {
    encodings.put(encoding.getName(), encoding);
  }

  @Override
  public Block readBlock(SliceInput input) {
    // Read encoding name (length-prefixed UTF-8)
    int nameLength = input.readInt();
    byte[] nameBytes = new byte[nameLength];
    input.readBytes(nameBytes);
    String encodingName = new String(nameBytes, java.nio.charset.StandardCharsets.UTF_8);

    BlockEncoding encoding = encodings.get(encodingName);
    if (encoding == null) {
      throw new IllegalArgumentException("Unknown block encoding: " + encodingName);
    }
    return encoding.readBlock(this, input);
  }

  @Override
  public void writeBlock(SliceOutput output, Block block) {
    String encodingName = block.getEncodingName();
    BlockEncoding encoding = encodings.get(encodingName);
    if (encoding == null) {
      throw new IllegalArgumentException("Unknown block encoding: " + encodingName);
    }

    // Handle replacement blocks (e.g., lazy -> concrete) BEFORE writing encoding name.
    // The replacement block may have a different encoding, so we must recurse
    // and let it write the correct name for the replacement.
    Block writeBlock = encoding.replacementBlockForWrite(block).orElse(block);
    if (writeBlock != block) {
      writeBlock(output, writeBlock);
      return;
    }

    // Write encoding name (length-prefixed UTF-8) only for the final block
    byte[] nameBytes = encodingName.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    output.writeInt(nameBytes.length);
    output.writeBytes(nameBytes);

    encoding.writeBlock(this, output, block);
  }

  @Override
  public Type readType(SliceInput input) {
    // Type serialization is not needed for DQE page transport.
    // Page serialization only writes block data, not type metadata.
    throw new UnsupportedOperationException("Type serialization not supported in DQE serde");
  }

  @Override
  public void writeType(SliceOutput output, Type type) {
    throw new UnsupportedOperationException("Type serialization not supported in DQE serde");
  }
}
