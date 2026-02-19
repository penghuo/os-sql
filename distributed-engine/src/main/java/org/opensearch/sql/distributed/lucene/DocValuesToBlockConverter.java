/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.lucene;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.util.List;
import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.BytesRef;
import org.opensearch.sql.distributed.data.Block;
import org.opensearch.sql.distributed.data.BlockBuilder;
import org.opensearch.sql.distributed.data.Page;

/**
 * Converts Lucene DocValues from a {@link LeafReaderContext} into {@link Block} arrays that form a
 * {@link Page}. This is the core bridge between Lucene's per-segment storage and the distributed
 * engine's columnar data format.
 *
 * <p>Each column in the output Page is produced by reading the appropriate DocValues type for that
 * field and writing the values into a BlockBuilder. Documents that have no value for a field get a
 * null entry in the corresponding Block.
 *
 * <p>Supported DocValues types:
 *
 * <ul>
 *   <li>{@link NumericDocValues} -> LongBlock, IntBlock, DoubleBlock, FloatBlock, BooleanBlock
 *   <li>{@link SortedDocValues} -> VariableWidthBlock (BytesRef)
 *   <li>{@link SortedNumericDocValues} -> LongBlock (first value for multi-valued)
 *   <li>{@link SortedSetDocValues} -> VariableWidthBlock (first value for multi-valued)
 *   <li>{@link BinaryDocValues} -> VariableWidthBlock
 * </ul>
 */
public final class DocValuesToBlockConverter {

  /** Maximum number of positions to batch into a single Page. */
  public static final int DEFAULT_BATCH_SIZE = 1024;

  private DocValuesToBlockConverter() {}

  /**
   * Reads a batch of documents from the given segment and produces a Page.
   *
   * @param leafCtx the Lucene segment reader context
   * @param columns the column mappings describing which fields to read
   * @param docIds array of document IDs to read (segment-relative)
   * @param docCount number of valid entries in docIds
   * @return a Page with one Block per column, containing the document values
   * @throws IOException if reading DocValues fails
   */
  public static Page readDocValues(
      LeafReaderContext leafCtx, List<ColumnMapping> columns, int[] docIds, int docCount)
      throws IOException {
    if (docCount == 0) {
      return createEmptyPage(columns);
    }

    Block[] blocks = new Block[columns.size()];
    for (int colIdx = 0; colIdx < columns.size(); colIdx++) {
      ColumnMapping col = columns.get(colIdx);
      blocks[colIdx] = readColumn(leafCtx, col, docIds, docCount);
    }
    return new Page(docCount, blocks);
  }

  /**
   * Reads all live documents in a segment for the given columns. Skips soft-deleted documents by
   * consulting the segment's liveDocs bitset.
   *
   * @param leafCtx the Lucene segment reader context
   * @param columns the column mappings
   * @return a Page containing all live documents in the segment
   * @throws IOException if reading DocValues fails
   */
  public static Page readAllDocs(LeafReaderContext leafCtx, List<ColumnMapping> columns)
      throws IOException {
    int maxDoc = leafCtx.reader().maxDoc();
    if (maxDoc == 0) {
      return createEmptyPage(columns);
    }
    org.apache.lucene.util.Bits liveDocs = leafCtx.reader().getLiveDocs();
    int[] allDocIds = new int[maxDoc];
    int count = 0;
    for (int i = 0; i < maxDoc; i++) {
      if (liveDocs == null || liveDocs.get(i)) {
        allDocIds[count++] = i;
      }
    }
    if (count == 0) {
      return createEmptyPage(columns);
    }
    return readDocValues(leafCtx, columns, allDocIds, count);
  }

  private static Block readColumn(
      LeafReaderContext leafCtx, ColumnMapping col, int[] docIds, int docCount) throws IOException {
    return switch (col.getDocValuesType()) {
      case NUMERIC -> readNumericDocValues(leafCtx, col, docIds, docCount);
      case SORTED -> readSortedDocValues(leafCtx, col, docIds, docCount);
      case SORTED_NUMERIC -> readSortedNumericDocValues(leafCtx, col, docIds, docCount);
      case SORTED_SET -> readSortedSetDocValues(leafCtx, col, docIds, docCount);
      case BINARY -> readBinaryDocValues(leafCtx, col, docIds, docCount);
      case NONE -> readNullColumn(col, docCount);
    };
  }

  /** Creates an all-null Block for fields without DocValues (e.g., _id metadata field). */
  private static Block readNullColumn(ColumnMapping col, int docCount) {
    BlockBuilder builder = BlockBuilder.create(col.getBlockType(), docCount);
    for (int i = 0; i < docCount; i++) {
      builder.appendNull();
    }
    return builder.build();
  }

  /**
   * Reads NumericDocValues and converts to the appropriate Block type based on the column mapping.
   */
  private static Block readNumericDocValues(
      LeafReaderContext leafCtx, ColumnMapping col, int[] docIds, int docCount) throws IOException {
    NumericDocValues ndv = DocValues.getNumeric(leafCtx.reader(), col.getFieldName());
    BlockBuilder builder = BlockBuilder.create(col.getBlockType(), docCount);

    for (int i = 0; i < docCount; i++) {
      int docId = docIds[i];
      if (ndv.advanceExact(docId)) {
        long rawValue = ndv.longValue();
        appendNumericValue(builder, col.getBlockType(), rawValue);
      } else {
        builder.appendNull();
      }
    }
    return builder.build();
  }

  /** Appends a raw long value from NumericDocValues, interpreting based on target BlockType. */
  private static void appendNumericValue(
      BlockBuilder builder, ColumnMapping.BlockType blockType, long rawValue) {
    switch (blockType) {
      case LONG -> builder.appendLong(rawValue);
      case INT -> builder.appendInt((int) rawValue);
      case SHORT -> builder.appendShort((short) rawValue);
      case BYTE -> builder.appendByte((byte) rawValue);
      case DOUBLE -> builder.appendDouble(Double.longBitsToDouble(rawValue));
      case FLOAT -> builder.appendFloat(Float.intBitsToFloat((int) rawValue));
      case BOOLEAN -> builder.appendBoolean(rawValue != 0);
      case VARIABLE_WIDTH -> builder.appendBytes(new BytesRef(String.valueOf(rawValue)).bytes);
    }
  }

  /** Reads SortedDocValues (single-valued keyword fields) into a VariableWidthBlock. */
  private static Block readSortedDocValues(
      LeafReaderContext leafCtx, ColumnMapping col, int[] docIds, int docCount) throws IOException {
    SortedDocValues sdv = DocValues.getSorted(leafCtx.reader(), col.getFieldName());
    BlockBuilder builder = BlockBuilder.create(ColumnMapping.BlockType.VARIABLE_WIDTH, docCount);

    for (int i = 0; i < docCount; i++) {
      int docId = docIds[i];
      if (sdv.advanceExact(docId)) {
        BytesRef value = sdv.lookupOrd(sdv.ordValue());
        builder.appendBytes(value.bytes, value.offset, value.length);
      } else {
        builder.appendNull();
      }
    }
    return builder.build();
  }

  /**
   * Reads SortedNumericDocValues (multi-valued numeric fields). For Phase 1, takes only the first
   * value. Documents with no values get null.
   */
  private static Block readSortedNumericDocValues(
      LeafReaderContext leafCtx, ColumnMapping col, int[] docIds, int docCount) throws IOException {
    SortedNumericDocValues sndv = DocValues.getSortedNumeric(leafCtx.reader(), col.getFieldName());
    BlockBuilder builder = BlockBuilder.create(col.getBlockType(), docCount);

    for (int i = 0; i < docCount; i++) {
      int docId = docIds[i];
      if (sndv.advanceExact(docId) && sndv.docValueCount() > 0) {
        long rawValue = sndv.nextValue();
        appendNumericValue(builder, col.getBlockType(), rawValue);
      } else {
        builder.appendNull();
      }
    }
    return builder.build();
  }

  /**
   * Reads SortedSetDocValues (multi-valued keyword fields). For Phase 1, takes only the first
   * value. Documents with no values get null. IP fields get special decoding from binary to
   * human-readable string form.
   */
  private static Block readSortedSetDocValues(
      LeafReaderContext leafCtx, ColumnMapping col, int[] docIds, int docCount) throws IOException {
    SortedSetDocValues ssdv = DocValues.getSortedSet(leafCtx.reader(), col.getFieldName());
    BlockBuilder builder = BlockBuilder.create(ColumnMapping.BlockType.VARIABLE_WIDTH, docCount);
    boolean isIp = col.isIpField();

    for (int i = 0; i < docCount; i++) {
      int docId = docIds[i];
      if (ssdv.advanceExact(docId) && ssdv.docValueCount() > 0) {
        long ord = ssdv.nextOrd();
        BytesRef value = ssdv.lookupOrd(ord);
        if (isIp) {
          // Decode binary IP address to human-readable string
          byte[] ipBytes = new byte[value.length];
          System.arraycopy(value.bytes, value.offset, ipBytes, 0, value.length);
          try {
            InetAddress addr = InetAddressPoint.decode(ipBytes);
            String formatted = formatInetAddress(addr);
            byte[] strBytes = formatted.getBytes(java.nio.charset.StandardCharsets.UTF_8);
            builder.appendBytes(strBytes, 0, strBytes.length);
          } catch (Exception e) {
            // Fallback: use raw bytes if decoding fails
            builder.appendBytes(value.bytes, value.offset, value.length);
          }
        } else {
          builder.appendBytes(value.bytes, value.offset, value.length);
        }
      } else {
        builder.appendNull();
      }
    }
    return builder.build();
  }

  /** Reads BinaryDocValues into a VariableWidthBlock. */
  private static Block readBinaryDocValues(
      LeafReaderContext leafCtx, ColumnMapping col, int[] docIds, int docCount) throws IOException {
    BinaryDocValues bdv = DocValues.getBinary(leafCtx.reader(), col.getFieldName());
    BlockBuilder builder = BlockBuilder.create(ColumnMapping.BlockType.VARIABLE_WIDTH, docCount);

    for (int i = 0; i < docCount; i++) {
      int docId = docIds[i];
      if (bdv.advanceExact(docId)) {
        BytesRef value = bdv.binaryValue();
        builder.appendBytes(value.bytes, value.offset, value.length);
      } else {
        builder.appendNull();
      }
    }
    return builder.build();
  }

  private static Page createEmptyPage(List<ColumnMapping> columns) {
    Block[] blocks = new Block[columns.size()];
    for (int i = 0; i < columns.size(); i++) {
      blocks[i] = BlockBuilder.create(columns.get(i).getBlockType(), 0).build();
    }
    return new Page(0, blocks);
  }

  /**
   * Formats an InetAddress to its canonical string representation. IPv4 addresses use dotted
   * decimal notation. IPv6 addresses use the compressed form with :: for zero runs (RFC 5952).
   */
  static String formatInetAddress(InetAddress addr) {
    if (addr instanceof Inet4Address) {
      return addr.getHostAddress();
    }
    // For IPv6, compress zero groups using :: notation
    byte[] bytes = addr.getAddress();
    int[] groups = new int[8];
    for (int i = 0; i < 8; i++) {
      groups[i] = ((bytes[i * 2] & 0xFF) << 8) | (bytes[i * 2 + 1] & 0xFF);
    }
    // Find the longest run of consecutive zero groups
    int bestStart = -1, bestLen = 0;
    int curStart = -1, curLen = 0;
    for (int i = 0; i < 8; i++) {
      if (groups[i] == 0) {
        if (curStart == -1) curStart = i;
        curLen++;
        if (curLen > bestLen) {
          bestStart = curStart;
          bestLen = curLen;
        }
      } else {
        curStart = -1;
        curLen = 0;
      }
    }
    // Build the compressed string
    StringBuilder sb = new StringBuilder();
    if (bestLen >= 1) {
      for (int i = 0; i < bestStart; i++) {
        if (i > 0) sb.append(':');
        sb.append(Integer.toHexString(groups[i]));
      }
      sb.append("::");
      for (int i = bestStart + bestLen; i < 8; i++) {
        if (i > bestStart + bestLen) sb.append(':');
        sb.append(Integer.toHexString(groups[i]));
      }
    } else {
      for (int i = 0; i < 8; i++) {
        if (i > 0) sb.append(':');
        sb.append(Integer.toHexString(groups[i]));
      }
    }
    return sb.toString();
  }
}
