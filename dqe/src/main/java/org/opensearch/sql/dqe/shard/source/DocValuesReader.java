/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.shard.source;

import io.airlift.slice.Slices;
import io.trino.spi.block.Block;
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
import java.util.List;
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

  /**
   * Read all columns for all doc IDs in a single interleaved pass. Opens each column's DocValues
   * iterator once and advances all iterators together for each doc ID. This avoids the cost of
   * re-acquiring and re-seeking DocValues iterators per column when reading column-by-column.
   *
   * @param leaf the segment's leaf reader context
   * @param columns list of column handles to read
   * @param docIds array of segment-relative doc IDs (must be sorted ascending)
   * @param offset starting index in the docIds array
   * @param count number of doc IDs to read
   * @return array of built Blocks, one per column
   */
  public static Block[] readColumnsInterleaved(
      LeafReaderContext leaf, List<ColumnHandle> columns, int[] docIds, int offset, int count)
      throws IOException {
    int numCols = columns.size();
    BlockBuilder[] builders = new BlockBuilder[numCols];
    for (int c = 0; c < numCols; c++) {
      builders[c] = columns.get(c).type().createBlockBuilder(null, count);
    }

    // Pre-open all DocValues iterators once
    Object[] dvIterators = new Object[numCols];
    int[] colCategory =
        new int[numCols]; // 0=numeric,1=double,2=timestamp,3=boolean,4=varchar,5=null
    Type[] types = new Type[numCols];

    for (int c = 0; c < numCols; c++) {
      ColumnHandle col = columns.get(c);
      types[c] = col.type();
      String field = col.name();

      if (types[c] instanceof BigintType
          || types[c] instanceof IntegerType
          || types[c] instanceof SmallintType
          || types[c] instanceof TinyintType) {
        dvIterators[c] = leaf.reader().getSortedNumericDocValues(field);
        colCategory[c] = 0;
      } else if (types[c] instanceof DoubleType) {
        dvIterators[c] = leaf.reader().getSortedNumericDocValues(field);
        colCategory[c] = 1;
      } else if (types[c] instanceof TimestampType) {
        dvIterators[c] = leaf.reader().getSortedNumericDocValues(field);
        colCategory[c] = 2;
      } else if (types[c] instanceof BooleanType) {
        dvIterators[c] = leaf.reader().getSortedNumericDocValues(field);
        colCategory[c] = 3;
      } else if (types[c] instanceof VarcharType) {
        SortedSetDocValues ssv = leaf.reader().getSortedSetDocValues(field);
        dvIterators[c] = ssv; // may be null for text fields
        colCategory[c] = 4;
      } else {
        dvIterators[c] = null;
        colCategory[c] = 5;
      }
    }

    // Single pass: for each doc, advance all iterators and write values
    for (int i = offset; i < offset + count; i++) {
      int docId = docIds[i];
      for (int c = 0; c < numCols; c++) {
        switch (colCategory[c]) {
          case 0: // numeric (bigint, integer, smallint, tinyint)
            {
              SortedNumericDocValues dv = (SortedNumericDocValues) dvIterators[c];
              if (dv != null && dv.advanceExact(docId)) {
                types[c].writeLong(builders[c], dv.nextValue());
              } else {
                builders[c].appendNull();
              }
              break;
            }
          case 1: // double
            {
              SortedNumericDocValues dv = (SortedNumericDocValues) dvIterators[c];
              if (dv != null && dv.advanceExact(docId)) {
                DoubleType.DOUBLE.writeDouble(builders[c], Double.longBitsToDouble(dv.nextValue()));
              } else {
                builders[c].appendNull();
              }
              break;
            }
          case 2: // timestamp
            {
              SortedNumericDocValues dv = (SortedNumericDocValues) dvIterators[c];
              if (dv != null && dv.advanceExact(docId)) {
                TimestampType.TIMESTAMP_MILLIS.writeLong(builders[c], dv.nextValue() * 1000L);
              } else {
                builders[c].appendNull();
              }
              break;
            }
          case 3: // boolean
            {
              SortedNumericDocValues dv = (SortedNumericDocValues) dvIterators[c];
              if (dv != null && dv.advanceExact(docId)) {
                BooleanType.BOOLEAN.writeBoolean(builders[c], dv.nextValue() == 1);
              } else {
                builders[c].appendNull();
              }
              break;
            }
          case 4: // varchar (keyword)
            {
              SortedSetDocValues dv = (SortedSetDocValues) dvIterators[c];
              if (dv == null) {
                VarcharType.VARCHAR.writeSlice(builders[c], Slices.EMPTY_SLICE);
              } else if (dv.advanceExact(docId)) {
                long ord = dv.nextOrd();
                BytesRef bytes = dv.lookupOrd(ord);
                VarcharType.VARCHAR.writeSlice(
                    builders[c], Slices.wrappedBuffer(bytes.bytes, bytes.offset, bytes.length));
              } else {
                builders[c].appendNull();
              }
              break;
            }
          default: // unsupported
            builders[c].appendNull();
            break;
        }
      }
    }

    Block[] blocks = new Block[numCols];
    for (int c = 0; c < numCols; c++) {
      blocks[c] = builders[c].build();
    }
    return blocks;
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
    if (dv == null) {
      // Field has no doc values at all (e.g., text type). Write empty strings to match
      // the behavior of _source-based reading where missing text fields are empty strings.
      for (int i = 0; i < count; i++) {
        VarcharType.VARCHAR.writeSlice(builder, Slices.EMPTY_SLICE);
      }
      return;
    }
    for (int i = offset; i < offset + count; i++) {
      if (dv.advanceExact(docIds[i])) {
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
