/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.shard.source;

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
import java.util.List;
import java.util.Map;

/** Converts row-oriented data (List of Maps) into Trino's columnar {@link Page} format. */
public final class PageBuilder {

  private PageBuilder() {}

  /**
   * Build a {@link Page} from the given column definitions and row data.
   *
   * @param columns ordered list of column handles describing name and Trino type
   * @param rows list of row maps where keys are column names and values are field values
   * @return a Trino {@link Page} containing one {@link Block} per column
   */
  public static Page build(List<ColumnHandle> columns, List<Map<String, Object>> rows) {
    BlockBuilder[] builders = new BlockBuilder[columns.size()];
    for (int i = 0; i < columns.size(); i++) {
      builders[i] = columns.get(i).type().createBlockBuilder(null, rows.size());
    }

    for (Map<String, Object> row : rows) {
      for (int col = 0; col < columns.size(); col++) {
        Object value = row.get(columns.get(col).name());
        appendValue(builders[col], columns.get(col).type(), value);
      }
    }

    Block[] blocks = new Block[builders.length];
    for (int i = 0; i < builders.length; i++) {
      blocks[i] = builders[i].build();
    }
    return new Page(blocks);
  }

  private static void appendValue(BlockBuilder builder, Type type, Object value) {
    if (value == null) {
      builder.appendNull();
      return;
    }
    try {
      if (type instanceof VarcharType) {
        VarcharType.VARCHAR.writeSlice(builder, Slices.utf8Slice(value.toString()));
      } else if (type instanceof BigintType) {
        BigintType.BIGINT.writeLong(builder, toNumber(value).longValue());
      } else if (type instanceof IntegerType) {
        IntegerType.INTEGER.writeLong(builder, toNumber(value).intValue());
      } else if (type instanceof SmallintType) {
        SmallintType.SMALLINT.writeLong(builder, toNumber(value).shortValue());
      } else if (type instanceof TinyintType) {
        TinyintType.TINYINT.writeLong(builder, toNumber(value).byteValue());
      } else if (type instanceof DoubleType) {
        DoubleType.DOUBLE.writeDouble(builder, toNumber(value).doubleValue());
      } else if (type instanceof RealType) {
        RealType.REAL.writeLong(builder, Float.floatToIntBits(toNumber(value).floatValue()));
      } else if (type instanceof BooleanType) {
        boolean boolVal =
            value instanceof Boolean ? (Boolean) value : Boolean.parseBoolean(value.toString());
        BooleanType.BOOLEAN.writeBoolean(builder, boolVal);
      } else if (type instanceof TimestampType) {
        long epochMillis = toNumber(value).longValue();
        TimestampType.TIMESTAMP_MILLIS.writeLong(builder, epochMillis * 1000);
      } else if (type instanceof VarbinaryType) {
        if (value instanceof byte[]) {
          VarbinaryType.VARBINARY.writeSlice(builder, Slices.wrappedBuffer((byte[]) value));
        } else {
          // Base64-encoded string from OpenSearch
          byte[] bytes = java.util.Base64.getDecoder().decode(value.toString());
          VarbinaryType.VARBINARY.writeSlice(builder, Slices.wrappedBuffer(bytes));
        }
      } else {
        // Fallback: write as VARCHAR
        VarcharType.VARCHAR.writeSlice(builder, Slices.utf8Slice(value.toString()));
      }
    } catch (Exception e) {
      // If conversion fails, write null rather than crash the whole query
      builder.appendNull();
    }
  }

  private static Number toNumber(Object value) {
    if (value instanceof Number) {
      return (Number) value;
    }
    return Long.parseLong(value.toString());
  }
}
