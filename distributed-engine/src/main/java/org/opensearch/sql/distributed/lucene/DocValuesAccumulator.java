/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.lucene;

import java.io.IOException;
import java.util.Arrays;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.DocIdSetIterator;

/**
 * Bridges Lucene DocValues directly to accumulation logic, bypassing the Block/Accumulator
 * abstraction for maximum performance. Used by {@link LuceneAggScan} to perform aggregation
 * directly during the Lucene collection phase.
 *
 * <p>Supports COUNT, SUM, MIN, MAX aggregations on numeric and sorted doc values fields. For GROUP
 * BY, groups are identified by the ordinal of the group-by field's DocValues.
 */
public class DocValuesAccumulator {

  /** Supported aggregation functions. */
  public enum AggFunction {
    COUNT,
    SUM,
    MIN,
    MAX
  }

  private final AggFunction function;
  private final String fieldName;
  private final ColumnMapping.DocValuesType docValuesType;
  private final ColumnMapping.BlockType blockType;

  /** Per-group state. Group 0 is used for global (no GROUP BY) aggregations. */
  private double[] values;

  private long[] counts;
  private boolean[] hasValue;
  private int groupCount;

  public DocValuesAccumulator(
      AggFunction function,
      String fieldName,
      ColumnMapping.DocValuesType docValuesType,
      ColumnMapping.BlockType blockType) {
    this.function = function;
    this.fieldName = fieldName;
    this.docValuesType = docValuesType;
    this.blockType = blockType;
    int initialCapacity = 64;
    this.values = new double[initialCapacity];
    this.counts = new long[initialCapacity];
    this.hasValue = new boolean[initialCapacity];
    this.groupCount = 0;
    if (function == AggFunction.MIN) {
      Arrays.fill(values, Double.POSITIVE_INFINITY);
    } else if (function == AggFunction.MAX) {
      Arrays.fill(values, Double.NEGATIVE_INFINITY);
    }
  }

  /**
   * Accumulates values from a leaf segment for all documents matched by the given iterator. For
   * global aggregation (no GROUP BY), all values go to group 0.
   *
   * @param leafCtx the Lucene segment context
   * @param iterator doc ID iterator for matching documents
   * @return the number of documents processed
   */
  public int accumulate(LeafReaderContext leafCtx, DocIdSetIterator iterator) throws IOException {
    if (function == AggFunction.COUNT && fieldName == null) {
      return accumulateCountAll(iterator, 0);
    }
    return switch (docValuesType) {
      case NUMERIC -> accumulateNumeric(leafCtx, iterator);
      case SORTED_NUMERIC -> accumulateSortedNumeric(leafCtx, iterator);
      default ->
          throw new UnsupportedOperationException(
              "DocValuesAccumulator does not support " + docValuesType + " for aggregation");
    };
  }

  /**
   * Accumulates values with GROUP BY support. Groups are assigned by the caller based on the
   * group-by field's DocValues ordinals or hash values.
   *
   * @param leafCtx the Lucene segment context
   * @param iterator doc ID iterator for matching documents
   * @param groupDocValues the DocValues used to determine group assignment
   * @param groupAssigner maps a document to its group ID
   * @return the number of documents processed
   */
  public int accumulateGrouped(
      LeafReaderContext leafCtx, DocIdSetIterator iterator, GroupAssigner groupAssigner)
      throws IOException {
    if (function == AggFunction.COUNT && fieldName == null) {
      return accumulateCountAllGrouped(iterator, groupAssigner);
    }
    return switch (docValuesType) {
      case NUMERIC -> accumulateNumericGrouped(leafCtx, iterator, groupAssigner);
      case SORTED_NUMERIC -> accumulateSortedNumericGrouped(leafCtx, iterator, groupAssigner);
      default ->
          throw new UnsupportedOperationException(
              "DocValuesAccumulator does not support "
                  + docValuesType
                  + " for grouped aggregation");
    };
  }

  /** Returns the accumulated result for the given group. */
  public double getValue(int groupId) {
    return values[groupId];
  }

  /** Returns the count for the given group (used for COUNT). */
  public long getCount(int groupId) {
    return counts[groupId];
  }

  /** Returns whether the given group has any non-null input. */
  public boolean hasValue(int groupId) {
    return hasValue[groupId];
  }

  public int getGroupCount() {
    return groupCount;
  }

  public AggFunction getFunction() {
    return function;
  }

  public ColumnMapping.BlockType getBlockType() {
    return blockType;
  }

  private void ensureCapacity(int requiredGroups) {
    if (requiredGroups > values.length) {
      int oldLen = values.length;
      int newLen = Math.max(oldLen * 2, requiredGroups);
      values = Arrays.copyOf(values, newLen);
      counts = Arrays.copyOf(counts, newLen);
      hasValue = Arrays.copyOf(hasValue, newLen);
      if (function == AggFunction.MIN) {
        Arrays.fill(values, oldLen, newLen, Double.POSITIVE_INFINITY);
      } else if (function == AggFunction.MAX) {
        Arrays.fill(values, oldLen, newLen, Double.NEGATIVE_INFINITY);
      }
    }
    if (requiredGroups > groupCount) {
      groupCount = requiredGroups;
    }
  }

  private int accumulateCountAll(DocIdSetIterator iterator, int groupId) throws IOException {
    ensureCapacity(groupId + 1);
    int count = 0;
    while (iterator.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
      counts[groupId]++;
      hasValue[groupId] = true;
      count++;
    }
    return count;
  }

  private int accumulateCountAllGrouped(DocIdSetIterator iterator, GroupAssigner groupAssigner)
      throws IOException {
    int count = 0;
    while (iterator.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
      int docId = iterator.docID();
      int groupId = groupAssigner.assignGroup(docId);
      ensureCapacity(groupId + 1);
      counts[groupId]++;
      hasValue[groupId] = true;
      count++;
    }
    return count;
  }

  private int accumulateNumeric(LeafReaderContext leafCtx, DocIdSetIterator iterator)
      throws IOException {
    NumericDocValues ndv =
        org.apache.lucene.index.DocValues.getNumeric(leafCtx.reader(), fieldName);
    int count = 0;
    while (iterator.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
      int docId = iterator.docID();
      if (ndv.advanceExact(docId)) {
        double val = toDouble(ndv.longValue());
        accumulate(0, val);
      }
      count++;
    }
    return count;
  }

  private int accumulateNumericGrouped(
      LeafReaderContext leafCtx, DocIdSetIterator iterator, GroupAssigner groupAssigner)
      throws IOException {
    NumericDocValues ndv =
        org.apache.lucene.index.DocValues.getNumeric(leafCtx.reader(), fieldName);
    int count = 0;
    while (iterator.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
      int docId = iterator.docID();
      int groupId = groupAssigner.assignGroup(docId);
      ensureCapacity(groupId + 1);
      if (ndv.advanceExact(docId)) {
        double val = toDouble(ndv.longValue());
        accumulate(groupId, val);
      }
      count++;
    }
    return count;
  }

  private int accumulateSortedNumeric(LeafReaderContext leafCtx, DocIdSetIterator iterator)
      throws IOException {
    SortedNumericDocValues sndv =
        org.apache.lucene.index.DocValues.getSortedNumeric(leafCtx.reader(), fieldName);
    int count = 0;
    while (iterator.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
      int docId = iterator.docID();
      if (sndv.advanceExact(docId) && sndv.docValueCount() > 0) {
        double val = toDouble(sndv.nextValue());
        accumulate(0, val);
      }
      count++;
    }
    return count;
  }

  private int accumulateSortedNumericGrouped(
      LeafReaderContext leafCtx, DocIdSetIterator iterator, GroupAssigner groupAssigner)
      throws IOException {
    SortedNumericDocValues sndv =
        org.apache.lucene.index.DocValues.getSortedNumeric(leafCtx.reader(), fieldName);
    int count = 0;
    while (iterator.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
      int docId = iterator.docID();
      int groupId = groupAssigner.assignGroup(docId);
      ensureCapacity(groupId + 1);
      if (sndv.advanceExact(docId) && sndv.docValueCount() > 0) {
        double val = toDouble(sndv.nextValue());
        accumulate(groupId, val);
      }
      count++;
    }
    return count;
  }

  private void accumulate(int groupId, double val) {
    ensureCapacity(groupId + 1);
    switch (function) {
      case COUNT -> {
        counts[groupId]++;
        hasValue[groupId] = true;
      }
      case SUM -> {
        values[groupId] += val;
        hasValue[groupId] = true;
      }
      case MIN -> {
        if (val < values[groupId]) {
          values[groupId] = val;
        }
        hasValue[groupId] = true;
      }
      case MAX -> {
        if (val > values[groupId]) {
          values[groupId] = val;
        }
        hasValue[groupId] = true;
      }
    }
  }

  private double toDouble(long rawValue) {
    return switch (blockType) {
      case DOUBLE -> Double.longBitsToDouble(rawValue);
      case FLOAT -> Float.intBitsToFloat((int) rawValue);
      default -> rawValue;
    };
  }

  /** Functional interface for assigning a document to a group ID. */
  @FunctionalInterface
  public interface GroupAssigner {
    int assignGroup(int docId) throws IOException;
  }

  /**
   * Creates a GroupAssigner based on SortedDocValues (keyword fields). Each unique ordinal maps to
   * a unique group ID.
   */
  public static GroupAssigner sortedDocValuesGroupAssigner(SortedDocValues sdv) {
    return docId -> {
      if (sdv.advanceExact(docId)) {
        return sdv.ordValue();
      }
      return -1;
    };
  }

  /**
   * Creates a GroupAssigner based on NumericDocValues (long/int fields). Uses a HashMap internally
   * to map values to group IDs.
   */
  public static GroupAssigner numericDocValuesGroupAssigner(
      NumericDocValues ndv, java.util.Map<Long, Integer> valueToGroup) {
    return docId -> {
      if (ndv.advanceExact(docId)) {
        long val = ndv.longValue();
        return valueToGroup.computeIfAbsent(val, k -> valueToGroup.size());
      }
      return -1;
    };
  }
}
