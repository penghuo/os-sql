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

  /** Format flag: row-major with per-cell null flags (original format). */
  private static final byte FORMAT_ROW_MAJOR = 0;

  /** Format flag: columnar with per-column null bitmap (fast path for numeric-dense pages). */
  private static final byte FORMAT_COLUMNAR = 1;

  /**
   * Serialize a Page with its column types. Uses columnar format when all columns are fixed-width
   * numeric types (bigint, integer, double, timestamp, smallint, tinyint, boolean) which eliminates
   * per-cell null flag overhead and enables tight type-specific write loops. Falls back to
   * row-major format for pages with VARCHAR or other variable-width columns.
   *
   * @param out the stream to write to
   * @param page the Page to serialize
   * @param columnTypes the Trino types for each column
   */
  public static void writePage(StreamOutput out, Page page, List<Type> columnTypes)
      throws IOException {
    int channelCount = page.getChannelCount();
    int positionCount = page.getPositionCount();

    // Check if columnar format is applicable: all columns must be fixed-width numeric
    boolean canUseColumnar = positionCount > 0;
    if (canUseColumnar) {
      for (Type type : columnTypes) {
        if (!isFixedWidthNumeric(type)) {
          canUseColumnar = false;
          break;
        }
      }
    }

    if (canUseColumnar) {
      writePageColumnar(out, page, columnTypes, channelCount, positionCount);
    } else {
      writePageRowMajor(out, page, columnTypes, channelCount, positionCount);
    }
  }

  /** Row-major serialization (original format). */
  private static void writePageRowMajor(
      StreamOutput out, Page page, List<Type> columnTypes, int channelCount, int positionCount)
      throws IOException {
    out.writeByte(FORMAT_ROW_MAJOR);
    out.writeVInt(channelCount);
    out.writeVInt(positionCount);
    for (Type type : columnTypes) {
      out.writeString(type.getTypeSignature().toString());
    }
    for (int pos = 0; pos < positionCount; pos++) {
      for (int ch = 0; ch < channelCount; ch++) {
        Block block = page.getBlock(ch);
        out.writeBoolean(block.isNull(pos));
        if (!block.isNull(pos)) {
          writeTypedValue(out, columnTypes.get(ch), block, pos);
        }
      }
    }
  }

  /**
   * Columnar serialization for fixed-width numeric pages. For each column, writes a "has nulls"
   * flag. If no nulls, writes all values contiguously in a type-specific tight loop. If has nulls,
   * writes a null bitmap followed by non-null values. This eliminates per-cell boolean writes and
   * type dispatch for the common case of dense numeric data (e.g., GROUP BY aggregation results).
   *
   * <p>For Q9-style shard output (10K rows, 3 numeric columns), this saves ~30KB of null flags and
   * ~30K virtual dispatch calls compared to row-major format.
   */
  private static void writePageColumnar(
      StreamOutput out, Page page, List<Type> columnTypes, int channelCount, int positionCount)
      throws IOException {
    out.writeByte(FORMAT_COLUMNAR);
    out.writeVInt(channelCount);
    out.writeVInt(positionCount);
    for (Type type : columnTypes) {
      out.writeString(type.getTypeSignature().toString());
    }

    for (int ch = 0; ch < channelCount; ch++) {
      Block block = page.getBlock(ch);
      Type type = columnTypes.get(ch);

      // Check if column has any nulls
      boolean hasNulls = block.mayHaveNull();
      if (hasNulls) {
        // Verify actual nulls (mayHaveNull can return true conservatively)
        hasNulls = false;
        for (int pos = 0; pos < positionCount; pos++) {
          if (block.isNull(pos)) {
            hasNulls = true;
            break;
          }
        }
      }

      out.writeBoolean(hasNulls);

      if (!hasNulls) {
        // Fast path: no nulls, write all values contiguously
        writeColumnBulk(out, block, type, positionCount);
      } else {
        // Write null bitmap (1 bit per position, packed into bytes)
        int numBytes = (positionCount + 7) >>> 3;
        byte[] nullBitmap = new byte[numBytes];
        for (int pos = 0; pos < positionCount; pos++) {
          if (block.isNull(pos)) {
            nullBitmap[pos >>> 3] |= (1 << (pos & 7));
          }
        }
        out.writeByteArray(nullBitmap);

        // Write non-null values
        for (int pos = 0; pos < positionCount; pos++) {
          if (!block.isNull(pos)) {
            writeTypedValue(out, type, block, pos);
          }
        }
      }
    }
  }

  /** Write all values of a non-null column in a type-specific tight loop. */
  private static void writeColumnBulk(StreamOutput out, Block block, Type type, int positionCount)
      throws IOException {
    if (type instanceof BigintType) {
      for (int pos = 0; pos < positionCount; pos++) {
        out.writeLong(BigintType.BIGINT.getLong(block, pos));
      }
    } else if (type instanceof IntegerType) {
      for (int pos = 0; pos < positionCount; pos++) {
        out.writeInt((int) IntegerType.INTEGER.getLong(block, pos));
      }
    } else if (type instanceof DoubleType) {
      for (int pos = 0; pos < positionCount; pos++) {
        out.writeDouble(DoubleType.DOUBLE.getDouble(block, pos));
      }
    } else if (type instanceof TimestampType) {
      for (int pos = 0; pos < positionCount; pos++) {
        out.writeLong(type.getLong(block, pos));
      }
    } else if (type instanceof SmallintType) {
      for (int pos = 0; pos < positionCount; pos++) {
        out.writeShort((short) SmallintType.SMALLINT.getLong(block, pos));
      }
    } else if (type instanceof TinyintType) {
      for (int pos = 0; pos < positionCount; pos++) {
        out.writeByte((byte) TinyintType.TINYINT.getLong(block, pos));
      }
    } else if (type instanceof BooleanType) {
      for (int pos = 0; pos < positionCount; pos++) {
        out.writeBoolean(BooleanType.BOOLEAN.getBoolean(block, pos));
      }
    } else if (type instanceof RealType) {
      for (int pos = 0; pos < positionCount; pos++) {
        out.writeInt((int) RealType.REAL.getLong(block, pos));
      }
    }
  }

  /** Check if a type is fixed-width numeric (eligible for columnar serialization). */
  private static boolean isFixedWidthNumeric(Type type) {
    return type instanceof BigintType
        || type instanceof IntegerType
        || type instanceof DoubleType
        || type instanceof TimestampType
        || type instanceof SmallintType
        || type instanceof TinyintType
        || type instanceof BooleanType
        || type instanceof RealType;
  }

  /**
   * Deserialize a Page. Reads the format flag and dispatches to the appropriate reader.
   *
   * @param in the stream to read from
   * @return the deserialized Page
   */
  public static Page readPage(StreamInput in) throws IOException {
    byte format = in.readByte();
    if (format == FORMAT_COLUMNAR) {
      return readPageColumnar(in);
    }
    // FORMAT_ROW_MAJOR (0) — original format
    return readPageRowMajor(in);
  }

  /** Read a page in row-major format (original). */
  private static Page readPageRowMajor(StreamInput in) throws IOException {
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

  /** Read a page in columnar format. */
  private static Page readPageColumnar(StreamInput in) throws IOException {
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

    for (int ch = 0; ch < channels; ch++) {
      boolean hasNulls = in.readBoolean();
      Type type = types.get(ch);
      BlockBuilder builder = builders[ch];

      if (!hasNulls) {
        // Fast path: read all values contiguously
        readColumnBulk(in, builder, type, positions);
      } else {
        // Read null bitmap
        byte[] nullBitmap = in.readByteArray();
        // Read non-null values, interleaving with null markers
        for (int pos = 0; pos < positions; pos++) {
          boolean isNull = (nullBitmap[pos >>> 3] & (1 << (pos & 7))) != 0;
          if (isNull) {
            builder.appendNull();
          } else {
            readTypedValue(in, type, builder);
          }
        }
      }
    }

    Block[] blocks = new Block[channels];
    for (int i = 0; i < channels; i++) {
      blocks[i] = builders[i].build();
    }
    return new Page(blocks);
  }

  /** Read all values of a non-null column in a type-specific tight loop. */
  private static void readColumnBulk(
      StreamInput in, BlockBuilder builder, Type type, int positionCount) throws IOException {
    if (type instanceof BigintType) {
      for (int pos = 0; pos < positionCount; pos++) {
        BigintType.BIGINT.writeLong(builder, in.readLong());
      }
    } else if (type instanceof IntegerType) {
      for (int pos = 0; pos < positionCount; pos++) {
        IntegerType.INTEGER.writeLong(builder, in.readInt());
      }
    } else if (type instanceof DoubleType) {
      for (int pos = 0; pos < positionCount; pos++) {
        DoubleType.DOUBLE.writeDouble(builder, in.readDouble());
      }
    } else if (type instanceof TimestampType) {
      for (int pos = 0; pos < positionCount; pos++) {
        type.writeLong(builder, in.readLong());
      }
    } else if (type instanceof SmallintType) {
      for (int pos = 0; pos < positionCount; pos++) {
        SmallintType.SMALLINT.writeLong(builder, in.readShort());
      }
    } else if (type instanceof TinyintType) {
      for (int pos = 0; pos < positionCount; pos++) {
        TinyintType.TINYINT.writeLong(builder, in.readByte());
      }
    } else if (type instanceof BooleanType) {
      for (int pos = 0; pos < positionCount; pos++) {
        BooleanType.BOOLEAN.writeBoolean(builder, in.readBoolean());
      }
    } else if (type instanceof RealType) {
      for (int pos = 0; pos < positionCount; pos++) {
        RealType.REAL.writeLong(builder, in.readInt());
      }
    }
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
