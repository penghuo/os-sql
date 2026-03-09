/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.shard.source;

import io.airlift.slice.Slices;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import java.io.IOException;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.BytesRef;

/**
 * Per-segment utility that reads Lucene doc values for a batch of doc IDs and writes directly into
 * Trino {@link BlockBuilder} arrays. Supports numeric (BIGINT, INTEGER, SMALLINT, TINYINT),
 * floating point (DOUBLE), timestamp, boolean, and keyword (VARCHAR) field types.
 */
public final class DocValuesReader {

  private DocValuesReader() {}

  /**
   * Read doc values for a column starting at offset 0.
   *
   * @param leaf the segment's leaf reader context
   * @param column column handle with name and Trino type
   * @param docIds array of segment-relative doc IDs to read
   * @param count number of doc IDs to read from the array
   * @param builder block builder to write values into
   */
  public static void readColumn(
      LeafReaderContext leaf, ColumnHandle column, int[] docIds, int count, BlockBuilder builder)
      throws IOException {
    readColumn(leaf, column, docIds, 0, count, builder);
  }

  /**
   * Read doc values for a column from the given leaf reader context and write them into the block
   * builder.
   *
   * @param leaf the segment's leaf reader context
   * @param column column handle with name and Trino type
   * @param docIds array of segment-relative doc IDs to read
   * @param offset starting index in the docIds array
   * @param count number of doc IDs to read starting from offset
   * @param builder block builder to write values into
   */
  public static void readColumn(
      LeafReaderContext leaf,
      ColumnHandle column,
      int[] docIds,
      int offset,
      int count,
      BlockBuilder builder)
      throws IOException {
    Type type = column.type();
    String field = column.name();

    if (type instanceof BigintType
        || type instanceof IntegerType
        || type instanceof SmallintType
        || type instanceof TinyintType) {
      readNumeric(leaf, field, type, docIds, offset, count, builder);
    } else if (type instanceof DoubleType) {
      readDouble(leaf, field, docIds, offset, count, builder);
    } else if (type instanceof TimestampType) {
      readTimestamp(leaf, field, docIds, offset, count, builder);
    } else if (type instanceof BooleanType) {
      readBoolean(leaf, field, docIds, offset, count, builder);
    } else if (type instanceof VarcharType) {
      readKeyword(leaf, field, docIds, offset, count, builder);
    } else {
      // Unsupported type: write nulls
      for (int i = 0; i < count; i++) {
        builder.appendNull();
      }
    }
  }

  private static void readNumeric(
      LeafReaderContext leaf,
      String field,
      Type type,
      int[] docIds,
      int offset,
      int count,
      BlockBuilder builder)
      throws IOException {
    SortedNumericDocValues dv = leaf.reader().getSortedNumericDocValues(field);
    for (int i = offset; i < offset + count; i++) {
      if (dv != null && dv.advanceExact(docIds[i])) {
        type.writeLong(builder, dv.nextValue());
      } else {
        builder.appendNull();
      }
    }
  }

  private static void readDouble(
      LeafReaderContext leaf,
      String field,
      int[] docIds,
      int offset,
      int count,
      BlockBuilder builder)
      throws IOException {
    SortedNumericDocValues dv = leaf.reader().getSortedNumericDocValues(field);
    for (int i = offset; i < offset + count; i++) {
      if (dv != null && dv.advanceExact(docIds[i])) {
        DoubleType.DOUBLE.writeDouble(builder, Double.longBitsToDouble(dv.nextValue()));
      } else {
        builder.appendNull();
      }
    }
  }

  private static void readTimestamp(
      LeafReaderContext leaf,
      String field,
      int[] docIds,
      int offset,
      int count,
      BlockBuilder builder)
      throws IOException {
    SortedNumericDocValues dv = leaf.reader().getSortedNumericDocValues(field);
    for (int i = offset; i < offset + count; i++) {
      if (dv != null && dv.advanceExact(docIds[i])) {
        long epochMillis = dv.nextValue();
        // Convert to Trino microseconds
        TimestampType.TIMESTAMP_MILLIS.writeLong(builder, epochMillis * 1000L);
      } else {
        builder.appendNull();
      }
    }
  }

  private static void readBoolean(
      LeafReaderContext leaf,
      String field,
      int[] docIds,
      int offset,
      int count,
      BlockBuilder builder)
      throws IOException {
    SortedNumericDocValues dv = leaf.reader().getSortedNumericDocValues(field);
    for (int i = offset; i < offset + count; i++) {
      if (dv != null && dv.advanceExact(docIds[i])) {
        BooleanType.BOOLEAN.writeBoolean(builder, dv.nextValue() == 1);
      } else {
        builder.appendNull();
      }
    }
  }

  private static void readKeyword(
      LeafReaderContext leaf,
      String field,
      int[] docIds,
      int offset,
      int count,
      BlockBuilder builder)
      throws IOException {
    SortedSetDocValues dv = leaf.reader().getSortedSetDocValues(field);
    for (int i = offset; i < offset + count; i++) {
      if (dv != null && dv.advanceExact(docIds[i])) {
        long ord = dv.nextOrd();
        BytesRef bytes = dv.lookupOrd(ord);
        VarcharType.VARCHAR.writeSlice(
            builder, Slices.wrappedBuffer(bytes.bytes, bytes.offset, bytes.length));
      } else {
        builder.appendNull();
      }
    }
  }
}
