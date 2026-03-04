/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.operator;

import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Physical operator that sorts all input rows by specified columns. Buffers all input from the
 * child operator, sorts row indices using a comparator, then emits a single sorted page.
 */
public class SortOperator implements Operator {

  private final Operator source;
  private final List<Integer> sortColumnIndices;
  private final List<Boolean> ascending;
  private final List<Type> columnTypes;
  private boolean finished;

  /**
   * Create a SortOperator.
   *
   * @param source child operator providing input pages
   * @param sortColumnIndices indices of columns to sort by (in priority order)
   * @param ascending true for ascending, false for descending (per sort column)
   * @param columnTypes types of all columns in the input pages
   */
  public SortOperator(
      Operator source,
      List<Integer> sortColumnIndices,
      List<Boolean> ascending,
      List<Type> columnTypes) {
    if (sortColumnIndices.size() != ascending.size()) {
      throw new IllegalArgumentException("sortColumnIndices and ascending must have the same size");
    }
    this.source = source;
    this.sortColumnIndices = sortColumnIndices;
    this.ascending = ascending;
    this.columnTypes = columnTypes;
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

    // Count total rows
    int totalRows = pages.stream().mapToInt(Page::getPositionCount).sum();
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
    Page combined = new Page(combinedBlocks);

    // Create row index array and sort it
    Integer[] indices = new Integer[totalRows];
    for (int i = 0; i < totalRows; i++) {
      indices[i] = i;
    }

    Comparator<Integer> comparator = buildComparator(combined);
    java.util.Arrays.sort(indices, comparator);

    // Build sorted page using copyPositions
    int[] sortedPositions = new int[totalRows];
    for (int i = 0; i < totalRows; i++) {
      sortedPositions[i] = indices[i];
    }

    return combined.copyPositions(sortedPositions, 0, totalRows);
  }

  @SuppressWarnings("unchecked")
  private Comparator<Integer> buildComparator(Page page) {
    Comparator<Integer> comparator = (a, b) -> 0;
    for (int i = 0; i < sortColumnIndices.size(); i++) {
      int colIdx = sortColumnIndices.get(i);
      boolean asc = ascending.get(i);
      Type type = columnTypes.get(colIdx);
      Block block = page.getBlock(colIdx);

      Comparator<Integer> colComparator =
          (a, b) -> {
            boolean aNull = block.isNull(a);
            boolean bNull = block.isNull(b);
            if (aNull && bNull) return 0;
            if (aNull) return 1; // nulls last
            if (bNull) return -1;

            int cmp = compareValues(block, a, b, type);
            return asc ? cmp : -cmp;
          };
      comparator = comparator.thenComparing(colComparator);
    }
    return comparator;
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
    }
    throw new UnsupportedOperationException("Unsupported type for sorting: " + type);
  }

  @Override
  public void close() {
    source.close();
  }
}
