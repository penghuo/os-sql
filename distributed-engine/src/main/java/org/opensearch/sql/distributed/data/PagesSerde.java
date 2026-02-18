/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.data;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Optional;

/**
 * Serializer/deserializer for Pages. Converts Pages to/from byte arrays for exchange between nodes.
 * Uses DataOutputStream/DataInputStream for portability.
 */
public class PagesSerde {

  // Block type markers
  private static final byte LONG_ARRAY = 0;
  private static final byte INT_ARRAY = 1;
  private static final byte DOUBLE_ARRAY = 2;
  private static final byte BYTE_ARRAY = 3;
  private static final byte SHORT_ARRAY = 4;
  private static final byte BOOLEAN_ARRAY = 5;
  private static final byte VARIABLE_WIDTH = 6;
  private static final byte DICTIONARY = 7;
  private static final byte RUN_LENGTH_ENCODED = 8;

  public byte[] serialize(Page page) {
    try {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      DataOutputStream out = new DataOutputStream(baos);
      writePage(out, page);
      out.flush();
      return baos.toByteArray();
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to serialize Page", e);
    }
  }

  public Page deserialize(byte[] data) {
    try {
      DataInputStream in = new DataInputStream(new ByteArrayInputStream(data));
      return readPage(in);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to deserialize Page", e);
    }
  }

  private void writePage(DataOutputStream out, Page page) throws IOException {
    out.writeInt(page.getPositionCount());
    out.writeInt(page.getChannelCount());
    for (int i = 0; i < page.getChannelCount(); i++) {
      writeBlock(out, page.getBlock(i));
    }
  }

  private Page readPage(DataInputStream in) throws IOException {
    int positionCount = in.readInt();
    int channelCount = in.readInt();
    Block[] blocks = new Block[channelCount];
    for (int i = 0; i < channelCount; i++) {
      blocks[i] = readBlock(in);
    }
    return new Page(positionCount, blocks);
  }

  private void writeBlock(DataOutputStream out, Block block) throws IOException {
    switch (block) {
      case LongArrayBlock b -> writeLongArrayBlock(out, b);
      case IntArrayBlock b -> writeIntArrayBlock(out, b);
      case DoubleArrayBlock b -> writeDoubleArrayBlock(out, b);
      case ByteArrayBlock b -> writeByteArrayBlock(out, b);
      case ShortArrayBlock b -> writeShortArrayBlock(out, b);
      case BooleanArrayBlock b -> writeBooleanArrayBlock(out, b);
      case VariableWidthBlock b -> writeVariableWidthBlock(out, b);
      case DictionaryBlock b -> writeDictionaryBlock(out, b);
      case RunLengthEncodedBlock b -> writeRunLengthEncodedBlock(out, b);
    }
  }

  private Block readBlock(DataInputStream in) throws IOException {
    byte type = in.readByte();
    return switch (type) {
      case LONG_ARRAY -> readLongArrayBlock(in);
      case INT_ARRAY -> readIntArrayBlock(in);
      case DOUBLE_ARRAY -> readDoubleArrayBlock(in);
      case BYTE_ARRAY -> readByteArrayBlock(in);
      case SHORT_ARRAY -> readShortArrayBlock(in);
      case BOOLEAN_ARRAY -> readBooleanArrayBlock(in);
      case VARIABLE_WIDTH -> readVariableWidthBlock(in);
      case DICTIONARY -> readDictionaryBlock(in);
      case RUN_LENGTH_ENCODED -> readRunLengthEncodedBlock(in);
      default -> throw new IOException("Unknown block type: " + type);
    };
  }

  // Long
  private void writeLongArrayBlock(DataOutputStream out, LongArrayBlock block) throws IOException {
    out.writeByte(LONG_ARRAY);
    int count = block.getPositionCount();
    out.writeInt(count);
    out.writeBoolean(block.mayHaveNull());
    if (block.mayHaveNull()) {
      for (int i = 0; i < count; i++) {
        out.writeBoolean(block.isNull(i));
      }
    }
    for (int i = 0; i < count; i++) {
      out.writeLong(block.getLong(i));
    }
  }

  private LongArrayBlock readLongArrayBlock(DataInputStream in) throws IOException {
    int count = in.readInt();
    boolean hasNulls = in.readBoolean();
    boolean[] nulls = null;
    if (hasNulls) {
      nulls = new boolean[count];
      for (int i = 0; i < count; i++) {
        nulls[i] = in.readBoolean();
      }
    }
    long[] values = new long[count];
    for (int i = 0; i < count; i++) {
      values[i] = in.readLong();
    }
    return new LongArrayBlock(count, Optional.ofNullable(nulls), values);
  }

  // Int
  private void writeIntArrayBlock(DataOutputStream out, IntArrayBlock block) throws IOException {
    out.writeByte(INT_ARRAY);
    int count = block.getPositionCount();
    out.writeInt(count);
    out.writeBoolean(block.mayHaveNull());
    if (block.mayHaveNull()) {
      for (int i = 0; i < count; i++) {
        out.writeBoolean(block.isNull(i));
      }
    }
    for (int i = 0; i < count; i++) {
      out.writeInt(block.getInt(i));
    }
  }

  private IntArrayBlock readIntArrayBlock(DataInputStream in) throws IOException {
    int count = in.readInt();
    boolean hasNulls = in.readBoolean();
    boolean[] nulls = null;
    if (hasNulls) {
      nulls = new boolean[count];
      for (int i = 0; i < count; i++) {
        nulls[i] = in.readBoolean();
      }
    }
    int[] values = new int[count];
    for (int i = 0; i < count; i++) {
      values[i] = in.readInt();
    }
    return new IntArrayBlock(count, Optional.ofNullable(nulls), values);
  }

  // Double
  private void writeDoubleArrayBlock(DataOutputStream out, DoubleArrayBlock block)
      throws IOException {
    out.writeByte(DOUBLE_ARRAY);
    int count = block.getPositionCount();
    out.writeInt(count);
    out.writeBoolean(block.mayHaveNull());
    if (block.mayHaveNull()) {
      for (int i = 0; i < count; i++) {
        out.writeBoolean(block.isNull(i));
      }
    }
    for (int i = 0; i < count; i++) {
      out.writeDouble(block.getDouble(i));
    }
  }

  private DoubleArrayBlock readDoubleArrayBlock(DataInputStream in) throws IOException {
    int count = in.readInt();
    boolean hasNulls = in.readBoolean();
    boolean[] nulls = null;
    if (hasNulls) {
      nulls = new boolean[count];
      for (int i = 0; i < count; i++) {
        nulls[i] = in.readBoolean();
      }
    }
    double[] values = new double[count];
    for (int i = 0; i < count; i++) {
      values[i] = in.readDouble();
    }
    return new DoubleArrayBlock(count, Optional.ofNullable(nulls), values);
  }

  // Byte
  private void writeByteArrayBlock(DataOutputStream out, ByteArrayBlock block) throws IOException {
    out.writeByte(BYTE_ARRAY);
    int count = block.getPositionCount();
    out.writeInt(count);
    out.writeBoolean(block.mayHaveNull());
    if (block.mayHaveNull()) {
      for (int i = 0; i < count; i++) {
        out.writeBoolean(block.isNull(i));
      }
    }
    for (int i = 0; i < count; i++) {
      out.writeByte(block.getByte(i));
    }
  }

  private ByteArrayBlock readByteArrayBlock(DataInputStream in) throws IOException {
    int count = in.readInt();
    boolean hasNulls = in.readBoolean();
    boolean[] nulls = null;
    if (hasNulls) {
      nulls = new boolean[count];
      for (int i = 0; i < count; i++) {
        nulls[i] = in.readBoolean();
      }
    }
    byte[] values = new byte[count];
    for (int i = 0; i < count; i++) {
      values[i] = in.readByte();
    }
    return new ByteArrayBlock(count, Optional.ofNullable(nulls), values);
  }

  // Short
  private void writeShortArrayBlock(DataOutputStream out, ShortArrayBlock block)
      throws IOException {
    out.writeByte(SHORT_ARRAY);
    int count = block.getPositionCount();
    out.writeInt(count);
    out.writeBoolean(block.mayHaveNull());
    if (block.mayHaveNull()) {
      for (int i = 0; i < count; i++) {
        out.writeBoolean(block.isNull(i));
      }
    }
    for (int i = 0; i < count; i++) {
      out.writeShort(block.getShort(i));
    }
  }

  private ShortArrayBlock readShortArrayBlock(DataInputStream in) throws IOException {
    int count = in.readInt();
    boolean hasNulls = in.readBoolean();
    boolean[] nulls = null;
    if (hasNulls) {
      nulls = new boolean[count];
      for (int i = 0; i < count; i++) {
        nulls[i] = in.readBoolean();
      }
    }
    short[] values = new short[count];
    for (int i = 0; i < count; i++) {
      values[i] = in.readShort();
    }
    return new ShortArrayBlock(count, Optional.ofNullable(nulls), values);
  }

  // Boolean
  private void writeBooleanArrayBlock(DataOutputStream out, BooleanArrayBlock block)
      throws IOException {
    out.writeByte(BOOLEAN_ARRAY);
    int count = block.getPositionCount();
    out.writeInt(count);
    out.writeBoolean(block.mayHaveNull());
    if (block.mayHaveNull()) {
      for (int i = 0; i < count; i++) {
        out.writeBoolean(block.isNull(i));
      }
    }
    for (int i = 0; i < count; i++) {
      out.writeBoolean(block.getBoolean(i));
    }
  }

  private BooleanArrayBlock readBooleanArrayBlock(DataInputStream in) throws IOException {
    int count = in.readInt();
    boolean hasNulls = in.readBoolean();
    boolean[] nulls = null;
    if (hasNulls) {
      nulls = new boolean[count];
      for (int i = 0; i < count; i++) {
        nulls[i] = in.readBoolean();
      }
    }
    boolean[] values = new boolean[count];
    for (int i = 0; i < count; i++) {
      values[i] = in.readBoolean();
    }
    return new BooleanArrayBlock(count, Optional.ofNullable(nulls), values);
  }

  // VariableWidth
  private void writeVariableWidthBlock(DataOutputStream out, VariableWidthBlock block)
      throws IOException {
    out.writeByte(VARIABLE_WIDTH);
    int count = block.getPositionCount();
    out.writeInt(count);
    out.writeBoolean(block.mayHaveNull());
    if (block.mayHaveNull()) {
      for (int i = 0; i < count; i++) {
        out.writeBoolean(block.isNull(i));
      }
    }
    for (int i = 0; i < count; i++) {
      int len = block.getSliceLength(i);
      out.writeInt(len);
      if (len > 0) {
        byte[] data = block.getSlice(i);
        out.write(data);
      }
    }
  }

  private VariableWidthBlock readVariableWidthBlock(DataInputStream in) throws IOException {
    int count = in.readInt();
    boolean hasNulls = in.readBoolean();
    boolean[] nulls = null;
    if (hasNulls) {
      nulls = new boolean[count];
      for (int i = 0; i < count; i++) {
        nulls[i] = in.readBoolean();
      }
    }
    int[] offsets = new int[count + 1];
    ByteArrayOutputStream sliceBuilder = new ByteArrayOutputStream();
    int offset = 0;
    for (int i = 0; i < count; i++) {
      offsets[i] = offset;
      int len = in.readInt();
      if (len > 0) {
        byte[] data = new byte[len];
        in.readFully(data);
        sliceBuilder.write(data);
      }
      offset += len;
    }
    offsets[count] = offset;
    return new VariableWidthBlock(
        count, sliceBuilder.toByteArray(), offsets, Optional.ofNullable(nulls));
  }

  // Dictionary
  private void writeDictionaryBlock(DataOutputStream out, DictionaryBlock block)
      throws IOException {
    out.writeByte(DICTIONARY);
    int count = block.getPositionCount();
    out.writeInt(count);
    writeBlock(out, block.getDictionary());
    for (int i = 0; i < count; i++) {
      out.writeInt(block.getId(i));
    }
  }

  private DictionaryBlock readDictionaryBlock(DataInputStream in) throws IOException {
    int count = in.readInt();
    Block dictBlock = readBlock(in);
    if (!(dictBlock instanceof ValueBlock valueBlock)) {
      throw new IOException("Dictionary must be a ValueBlock");
    }
    int[] ids = new int[count];
    for (int i = 0; i < count; i++) {
      ids[i] = in.readInt();
    }
    return new DictionaryBlock(count, valueBlock, ids);
  }

  // RunLengthEncoded
  private void writeRunLengthEncodedBlock(DataOutputStream out, RunLengthEncodedBlock block)
      throws IOException {
    out.writeByte(RUN_LENGTH_ENCODED);
    out.writeInt(block.getPositionCount());
    writeBlock(out, block.getValue());
  }

  private RunLengthEncodedBlock readRunLengthEncodedBlock(DataInputStream in) throws IOException {
    int count = in.readInt();
    Block value = readBlock(in);
    return new RunLengthEncodedBlock(value, count);
  }
}
