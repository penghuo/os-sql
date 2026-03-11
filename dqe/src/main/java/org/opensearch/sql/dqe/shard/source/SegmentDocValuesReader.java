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
import java.util.List;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.BytesRef;

/**
 * Per-segment doc values reader that caches Lucene doc values iterators across batches. Instead of
 * re-opening iterators for each batch (which requires re-seeking from the start), this class opens
 * iterators once per segment and reuses them, taking advantage of the fact that doc IDs are
 * monotonically increasing within a segment.
 *
 * <p>This avoids the overhead of repeatedly calling {@code leaf.reader().getSortedNumericDocValues}
 * or {@code getSortedSetDocValues} per batch.
 */
public final class SegmentDocValuesReader {

  private final LeafReaderContext leaf;
  private final List<ColumnHandle> columns;

  /**
   * Cached iterators, one per column. For numeric/boolean/timestamp/double types, the entry is a
   * SortedNumericDocValues. For VARCHAR, it's a SortedSetDocValues (or null if no doc values
   * exist).
   */
  private final Object[] cachedIterators;

  /**
   * Create a reader for the given segment and column list. Eagerly opens doc values iterators for
   * all columns.
   *
   * @param leaf the segment's leaf reader context
   * @param columns columns to read
   */
  public SegmentDocValuesReader(LeafReaderContext leaf, List<ColumnHandle> columns)
      throws IOException {
    this.leaf = leaf;
    this.columns = columns;
    this.cachedIterators = new Object[columns.size()];

    // Eagerly open iterators for all columns
    for (int c = 0; c < columns.size(); c++) {
      ColumnHandle col = columns.get(c);
      Type type = col.type();
      if (type instanceof VarcharType) {
        cachedIterators[c] = leaf.reader().getSortedSetDocValues(col.name());
      } else {
        cachedIterators[c] = leaf.reader().getSortedNumericDocValues(col.name());
      }
    }
  }

  /**
   * Read a batch of doc values for all columns.
   *
   * @param docIds array of segment-relative doc IDs (must be in ascending order)
   * @param offset starting index in the docIds array
   * @param count number of doc IDs to read
   * @param builders block builders, one per column
   */
  public void readBatch(int[] docIds, int offset, int count, BlockBuilder[] builders)
      throws IOException {
    for (int c = 0; c < columns.size(); c++) {
      ColumnHandle col = columns.get(c);
      Type type = col.type();

      if (type instanceof BigintType
          || type instanceof IntegerType
          || type instanceof SmallintType
          || type instanceof TinyintType) {
        readNumericCached(c, type, docIds, offset, count, builders[c]);
      } else if (type instanceof DoubleType) {
        readDoubleCached(c, docIds, offset, count, builders[c]);
      } else if (type instanceof TimestampType) {
        readTimestampCached(c, docIds, offset, count, builders[c]);
      } else if (type instanceof BooleanType) {
        readBooleanCached(c, docIds, offset, count, builders[c]);
      } else if (type instanceof VarcharType) {
        readKeywordCached(c, docIds, offset, count, builders[c]);
      } else {
        // Unsupported type: write nulls
        for (int i = 0; i < count; i++) {
          builders[c].appendNull();
        }
      }
    }
  }

  private void readNumericCached(
      int colIdx, Type type, int[] docIds, int offset, int count, BlockBuilder builder)
      throws IOException {
    SortedNumericDocValues dv = (SortedNumericDocValues) cachedIterators[colIdx];
    for (int i = offset; i < offset + count; i++) {
      if (dv != null && dv.advanceExact(docIds[i])) {
        type.writeLong(builder, dv.nextValue());
      } else {
        builder.appendNull();
      }
    }
  }

  private void readDoubleCached(
      int colIdx, int[] docIds, int offset, int count, BlockBuilder builder) throws IOException {
    SortedNumericDocValues dv = (SortedNumericDocValues) cachedIterators[colIdx];
    for (int i = offset; i < offset + count; i++) {
      if (dv != null && dv.advanceExact(docIds[i])) {
        DoubleType.DOUBLE.writeDouble(builder, Double.longBitsToDouble(dv.nextValue()));
      } else {
        builder.appendNull();
      }
    }
  }

  private void readTimestampCached(
      int colIdx, int[] docIds, int offset, int count, BlockBuilder builder) throws IOException {
    SortedNumericDocValues dv = (SortedNumericDocValues) cachedIterators[colIdx];
    for (int i = offset; i < offset + count; i++) {
      if (dv != null && dv.advanceExact(docIds[i])) {
        long epochMillis = dv.nextValue();
        TimestampType.TIMESTAMP_MILLIS.writeLong(builder, epochMillis * 1000L);
      } else {
        builder.appendNull();
      }
    }
  }

  private void readBooleanCached(
      int colIdx, int[] docIds, int offset, int count, BlockBuilder builder) throws IOException {
    SortedNumericDocValues dv = (SortedNumericDocValues) cachedIterators[colIdx];
    for (int i = offset; i < offset + count; i++) {
      if (dv != null && dv.advanceExact(docIds[i])) {
        BooleanType.BOOLEAN.writeBoolean(builder, dv.nextValue() == 1);
      } else {
        builder.appendNull();
      }
    }
  }

  private void readKeywordCached(
      int colIdx, int[] docIds, int offset, int count, BlockBuilder builder) throws IOException {
    SortedSetDocValues dv = (SortedSetDocValues) cachedIterators[colIdx];
    if (dv == null) {
      // Field has no doc values. Write empty strings to match _source behavior.
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
