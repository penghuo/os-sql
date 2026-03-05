/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.common.serde;

import io.airlift.slice.Slices;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

/**
 * Serializes and deserializes Trino {@link Page} objects via OpenSearch's {@link StreamOutput} /
 * {@link StreamInput}. This enables shard results to flow as serialized Trino Pages over the
 * transport layer, instead of JSON strings.
 */
public final class PageSerializer {

  private static final Map<String, Type> TYPE_REGISTRY =
      Map.ofEntries(
          Map.entry("bigint", BigintType.BIGINT),
          Map.entry("varchar", VarcharType.VARCHAR),
          Map.entry(
              "varchar(2147483646)", VarcharType.VARCHAR), // Trino's unbounded varchar signature
          Map.entry("double", DoubleType.DOUBLE),
          Map.entry("boolean", BooleanType.BOOLEAN),
          Map.entry("integer", IntegerType.INTEGER),
          Map.entry("smallint", SmallintType.SMALLINT),
          Map.entry("tinyint", TinyintType.TINYINT),
          Map.entry("real", RealType.REAL),
          Map.entry("varbinary", VarbinaryType.VARBINARY),
          Map.entry("timestamp(3)", TimestampType.TIMESTAMP_MILLIS));

  private PageSerializer() {}

  /**
   * Resolve a type signature string back to a Trino {@link Type} singleton.
   *
   * @param typeSignature the type signature string (e.g., "bigint", "varchar")
   * @return the corresponding Type singleton
   * @throws IllegalArgumentException if the type signature is not recognized
   */
  public static Type resolveType(String typeSignature) {
    Type type = TYPE_REGISTRY.get(typeSignature);
    if (type == null) {
      throw new IllegalArgumentException("Unsupported type signature: " + typeSignature);
    }
    return type;
  }

  /**
   * Serialize a Page with its column types.
   *
   * @param out the stream to write to
   * @param page the Page to serialize
   * @param columnTypes the Trino types for each column
   */
  public static void writePage(StreamOutput out, Page page, List<Type> columnTypes)
      throws IOException {
    out.writeVInt(page.getChannelCount());
    out.writeVInt(page.getPositionCount());
    // Write column type signatures
    for (Type type : columnTypes) {
      out.writeString(type.getTypeSignature().toString());
    }
    // Write each value: null flag + typed value
    for (int pos = 0; pos < page.getPositionCount(); pos++) {
      for (int ch = 0; ch < page.getChannelCount(); ch++) {
        Block block = page.getBlock(ch);
        out.writeBoolean(block.isNull(pos));
        if (!block.isNull(pos)) {
          writeTypedValue(out, columnTypes.get(ch), block, pos);
        }
      }
    }
  }

  /**
   * Deserialize a Page.
   *
   * @param in the stream to read from
   * @return the deserialized Page
   */
  public static Page readPage(StreamInput in) throws IOException {
    int channels = in.readVInt();
    int positions = in.readVInt();
    List<Type> types = new ArrayList<>();
    for (int i = 0; i < channels; i++) {
      types.add(resolveType(in.readString()));
    }
    BlockBuilder[] builders = new BlockBuilder[channels];
    for (int i = 0; i < channels; i++) {
      builders[i] = types.get(i).createBlockBuilder(null, positions);
    }
    for (int pos = 0; pos < positions; pos++) {
      for (int ch = 0; ch < channels; ch++) {
        boolean isNull = in.readBoolean();
        if (isNull) {
          builders[ch].appendNull();
        } else {
          readTypedValue(in, types.get(ch), builders[ch]);
        }
      }
    }
    Block[] blocks = new Block[channels];
    for (int i = 0; i < channels; i++) {
      blocks[i] = builders[i].build();
    }
    return new Page(blocks);
  }

  /**
   * Serialize a list of Pages.
   *
   * @param out the stream to write to
   * @param pages the pages to serialize
   * @param columnTypes the Trino types for each column
   */
  public static void writePages(StreamOutput out, List<Page> pages, List<Type> columnTypes)
      throws IOException {
    out.writeVInt(pages.size());
    for (Page page : pages) {
      writePage(out, page, columnTypes);
    }
  }

  /**
   * Deserialize a list of Pages.
   *
   * @param in the stream to read from
   * @return the deserialized pages
   */
  public static List<Page> readPages(StreamInput in) throws IOException {
    int count = in.readVInt();
    List<Page> pages = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      pages.add(readPage(in));
    }
    return pages;
  }

  /**
   * Write a typed value from a Block at the given position to the output stream.
   *
   * @param out the stream to write to
   * @param type the Trino type of the column
   * @param block the block containing the value
   * @param position the row position within the block
   */
  private static void writeTypedValue(StreamOutput out, Type type, Block block, int position)
      throws IOException {
    if (type instanceof BigintType) {
      out.writeLong(BigintType.BIGINT.getLong(block, position));
    } else if (type instanceof IntegerType) {
      out.writeInt((int) IntegerType.INTEGER.getLong(block, position));
    } else if (type instanceof SmallintType) {
      out.writeShort((short) SmallintType.SMALLINT.getLong(block, position));
    } else if (type instanceof TinyintType) {
      out.writeByte((byte) TinyintType.TINYINT.getLong(block, position));
    } else if (type instanceof DoubleType) {
      out.writeDouble(DoubleType.DOUBLE.getDouble(block, position));
    } else if (type instanceof RealType) {
      out.writeInt((int) RealType.REAL.getLong(block, position));
    } else if (type instanceof BooleanType) {
      out.writeBoolean(BooleanType.BOOLEAN.getBoolean(block, position));
    } else if (type instanceof VarcharType) {
      out.writeString(VarcharType.VARCHAR.getSlice(block, position).toStringUtf8());
    } else if (type instanceof VarbinaryType) {
      byte[] bytes = VarbinaryType.VARBINARY.getSlice(block, position).getBytes();
      out.writeByteArray(bytes);
    } else if (type instanceof TimestampType) {
      out.writeLong(type.getLong(block, position));
    } else {
      throw new IOException("Unsupported type for serialization: " + type);
    }
  }

  /**
   * Read a typed value from the input stream and append it to the block builder.
   *
   * @param in the stream to read from
   * @param type the Trino type of the column
   * @param builder the block builder to append the value to
   */
  private static void readTypedValue(StreamInput in, Type type, BlockBuilder builder)
      throws IOException {
    if (type instanceof BigintType) {
      BigintType.BIGINT.writeLong(builder, in.readLong());
    } else if (type instanceof IntegerType) {
      IntegerType.INTEGER.writeLong(builder, in.readInt());
    } else if (type instanceof SmallintType) {
      SmallintType.SMALLINT.writeLong(builder, in.readShort());
    } else if (type instanceof TinyintType) {
      TinyintType.TINYINT.writeLong(builder, in.readByte());
    } else if (type instanceof DoubleType) {
      DoubleType.DOUBLE.writeDouble(builder, in.readDouble());
    } else if (type instanceof RealType) {
      RealType.REAL.writeLong(builder, in.readInt());
    } else if (type instanceof BooleanType) {
      BooleanType.BOOLEAN.writeBoolean(builder, in.readBoolean());
    } else if (type instanceof VarcharType) {
      VarcharType.VARCHAR.writeSlice(builder, Slices.utf8Slice(in.readString()));
    } else if (type instanceof VarbinaryType) {
      byte[] bytes = in.readByteArray();
      VarbinaryType.VARBINARY.writeSlice(builder, Slices.wrappedBuffer(bytes));
    } else if (type instanceof TimestampType) {
      type.writeLong(builder, in.readLong());
    } else {
      throw new IOException("Unsupported type for deserialization: " + type);
    }
  }
}
