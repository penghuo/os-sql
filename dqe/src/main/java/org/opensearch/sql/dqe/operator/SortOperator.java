/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.operator;

import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

/**
 * Physical operator that sorts all input rows by specified columns. Buffers all input from the
 * child operator, sorts row indices using a comparator, then emits a single sorted page.
 */
public class SortOperator implements Operator {

  private final Operator source;
  private final List<Integer> sortColumnIndices;
  private final List<Boolean> ascending;
  private final List<Boolean> nullsFirst;
  private final List<Type> columnTypes;
  private final long topN;
  private boolean finished;

  /**
   * Convenience constructor that uses SQL-standard null ordering defaults: NULLS LAST for ASC,
   * NULLS FIRST for DESC.
   */
  public SortOperator(
      Operator source,
      List<Integer> sortColumnIndices,
      List<Boolean> ascending,
      List<Type> columnTypes) {
    this(source, sortColumnIndices, ascending, defaultNullsFirst(ascending), columnTypes, -1);
  }

  /**
   * Create a SortOperator with optional Top-N optimization.
   *
   * @param source child operator providing input pages
   * @param sortColumnIndices indices of columns to sort by (in priority order)
   * @param ascending true for ascending, false for descending (per sort column)
   * @param nullsFirst true to sort nulls before non-null values, false for nulls last (per sort
   *     column)
   * @param columnTypes types of all columns in the input pages
   * @param topN if positive, only keep the top N rows using a bounded heap instead of full sort
   */
  public SortOperator(
      Operator source,
      List<Integer> sortColumnIndices,
      List<Boolean> ascending,
      List<Boolean> nullsFirst,
      List<Type> columnTypes,
      long topN) {
    if (sortColumnIndices.size() != ascending.size()) {
      throw new IllegalArgumentException("sortColumnIndices and ascending must have the same size");
    }
    if (sortColumnIndices.size() != nullsFirst.size()) {
      throw new IllegalArgumentException(
          "sortColumnIndices and nullsFirst must have the same size");
    }
    this.source = source;
    this.sortColumnIndices = sortColumnIndices;
    this.ascending = ascending;
    this.nullsFirst = nullsFirst;
    this.columnTypes = columnTypes;
    this.topN = topN;
    this.finished = false;
  }

  @Override
  public Page processNextBatch() {
    if (finished) {
      return null;
    }
    finished = true;

    // Drain all pages from source
    List<Page> pages = new ArrayList<>();
    Page page;
    while ((page = source.processNextBatch()) != null) {
      pages.add(page);
    }

    if (pages.isEmpty()) {
      return null;
    }

    int totalRows = pages.stream().mapToInt(Page::getPositionCount).sum();

    // Fast path: single input page — skip the expensive copy into new BlockBuilders.
    // This is the common case when the source is FusedGroupByAggregate (returns one Page)
    // or any single-batch operator. For 100K+ groups this avoids 100K * numCols block
    // append operations and the associated memory allocation.
    Page combined;
    if (pages.size() == 1) {
      combined = pages.get(0);
    } else {
      int numColumns = pages.get(0).getChannelCount();
      // Materialize all rows into a combined page
      BlockBuilder[] builders = new BlockBuilder[numColumns];
      for (int col = 0; col < numColumns; col++) {
        builders[col] = columnTypes.get(col).createBlockBuilder(null, totalRows);
      }

      for (Page p : pages) {
        for (int pos = 0; pos < p.getPositionCount(); pos++) {
          for (int col = 0; col < numColumns; col++) {
            columnTypes.get(col).appendTo(p.getBlock(col), pos, builders[col]);
          }
        }
      }

      Block[] combinedBlocks = new Block[numColumns];
      for (int i = 0; i < numColumns; i++) {
        combinedBlocks[i] = builders[i].build();
      }
      combined = new Page(combinedBlocks);
    }

    Comparator<Integer> comparator = buildComparator(combined);

    if (topN > 0 && topN < totalRows) {
      return topNSort(combined, comparator, totalRows);
    }

    return fullSort(combined, comparator, totalRows);
  }

  /** Full sort: sort all rows and return the complete sorted page. */
  private Page fullSort(Page combined, Comparator<Integer> comparator, int totalRows) {
    Integer[] indices = new Integer[totalRows];
    for (int i = 0; i < totalRows; i++) {
      indices[i] = i;
    }
    java.util.Arrays.sort(indices, comparator);

    int[] sortedPositions = new int[totalRows];
    for (int i = 0; i < totalRows; i++) {
      sortedPositions[i] = indices[i];
    }
    return combined.copyPositions(sortedPositions, 0, totalRows);
  }

  /**
   * Top-N sort: maintain a bounded max-heap of size topN. For each row, if the heap is full and the
   * row is better than the worst in the heap, replace it. O(n log k) where k = topN.
   */
  private Page topNSort(Page combined, Comparator<Integer> comparator, int totalRows) {
    int k = (int) Math.min(topN, totalRows);
    // Max-heap: the worst element (by sort order) is at the top so we can evict it
    PriorityQueue<Integer> heap = new PriorityQueue<>(k + 1, comparator.reversed());

    for (int i = 0; i < totalRows; i++) {
      heap.offer(i);
      if (heap.size() > k) {
        heap.poll(); // Remove the worst element
      }
    }

    // Extract elements from heap and sort them
    int heapSize = heap.size();
    Integer[] topIndices = new Integer[heapSize];
    for (int i = heapSize - 1; i >= 0; i--) {
      topIndices[i] = heap.poll();
    }
    java.util.Arrays.sort(topIndices, comparator);

    int[] sortedPositions = new int[heapSize];
    for (int i = 0; i < heapSize; i++) {
      sortedPositions[i] = topIndices[i];
    }
    return combined.copyPositions(sortedPositions, 0, heapSize);
  }

  /**
   * Build a flat comparator that checks all sort keys in a single lambda, avoiding the overhead of
   * chained thenComparing wrappers. Pre-fetches blocks and types into arrays for fast access.
   */
  @SuppressWarnings("unchecked")
  private Comparator<Integer> buildComparator(Page page) {
    int numKeys = sortColumnIndices.size();
    Block[] sortBlocks = new Block[numKeys];
    Type[] sortTypes = new Type[numKeys];
    boolean[] ascArr = new boolean[numKeys];
    boolean[] nfArr = new boolean[numKeys];
    for (int i = 0; i < numKeys; i++) {
      int colIdx = sortColumnIndices.get(i);
      sortBlocks[i] = page.getBlock(colIdx);
      sortTypes[i] = columnTypes.get(colIdx);
      ascArr[i] = ascending.get(i);
      nfArr[i] = nullsFirst.get(i);
    }

    return (a, b) -> {
      for (int i = 0; i < numKeys; i++) {
        Block blk = sortBlocks[i];
        boolean aNull = blk.isNull(a);
        boolean bNull = blk.isNull(b);
        if (aNull && bNull) continue;
        if (aNull) return nfArr[i] ? -1 : 1;
        if (bNull) return nfArr[i] ? 1 : -1;

        int cmp = compareValues(blk, a, b, sortTypes[i]);
        if (cmp != 0) {
          return ascArr[i] ? cmp : -cmp;
        }
      }
      return 0;
    };
  }

  private static int compareValues(Block block, int posA, int posB, Type type) {
    if (type instanceof BigintType) {
      return Long.compare(
          BigintType.BIGINT.getLong(block, posA), BigintType.BIGINT.getLong(block, posB));
    } else if (type instanceof IntegerType) {
      return Long.compare(
          IntegerType.INTEGER.getLong(block, posA), IntegerType.INTEGER.getLong(block, posB));
    } else if (type instanceof DoubleType) {
      return Double.compare(
          DoubleType.DOUBLE.getDouble(block, posA), DoubleType.DOUBLE.getDouble(block, posB));
    } else if (type instanceof VarcharType) {
      return VarcharType.VARCHAR
          .getSlice(block, posA)
          .compareTo(VarcharType.VARCHAR.getSlice(block, posB));
    } else if (type instanceof TimestampType) {
      // TimestampType stores values as long (microseconds since epoch)
      return Long.compare(type.getLong(block, posA), type.getLong(block, posB));
    } else if (type instanceof BooleanType) {
      return Boolean.compare(
          BooleanType.BOOLEAN.getBoolean(block, posA), BooleanType.BOOLEAN.getBoolean(block, posB));
    }
    // Fallback: try comparing as longs for any other numeric type
    try {
      return Long.compare(type.getLong(block, posA), type.getLong(block, posB));
    } catch (Exception e) {
      throw new UnsupportedOperationException("Unsupported type for sorting: " + type);
    }
  }

  @Override
  public void close() {
    source.close();
  }

  /** Derive Trino-compatible null ordering: NULLS LAST for both ASC and DESC. */
  private static List<Boolean> defaultNullsFirst(List<Boolean> ascending) {
    List<Boolean> defaults = new ArrayList<>(ascending.size());
    for (int i = 0; i < ascending.size(); i++) {
      defaults.add(false); // Trino defaults to NULLS LAST
    }
    return defaults;
  }
}
