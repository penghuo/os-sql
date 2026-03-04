/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.operator;

import io.airlift.slice.Slices;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.VarcharType;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/** Test-only Operator implementation that returns pre-built Pages from a list. */
public class TestPageSource implements Operator {

  private final Iterator<Page> pages;

  public TestPageSource(List<Page> pages) {
    this.pages = pages.iterator();
  }

  @Override
  public Page processNextBatch() {
    return pages.hasNext() ? pages.next() : null;
  }

  @Override
  public void close() {}

  /**
   * Build a Page with {@code n} rows containing a single BIGINT column with values 0..n-1.
   *
   * @param n number of rows
   * @return page with one BIGINT column
   */
  public static Page buildBigintPage(int n) {
    BlockBuilder builder = BigintType.BIGINT.createBlockBuilder(null, n);
    for (int i = 0; i < n; i++) {
      BigintType.BIGINT.writeLong(builder, i);
    }
    return new Page(builder.build());
  }

  /**
   * Build a Page with a single BIGINT column containing the specified values.
   *
   * @param values the values to write
   * @return page with one BIGINT column
   */
  public static Page buildBigintPageWithValues(long... values) {
    BlockBuilder builder = BigintType.BIGINT.createBlockBuilder(null, values.length);
    for (long v : values) {
      BigintType.BIGINT.writeLong(builder, v);
    }
    return new Page(builder.build());
  }

  /**
   * Build a Page with {@code numColumns} BIGINT columns and {@code numRows} rows. Each column
   * contains values from 0 to numRows-1, offset by column index (col_i, row_j = j * 10 + i).
   *
   * @param numColumns number of columns
   * @param numRows number of rows
   * @return page with multiple BIGINT columns
   */
  public static Page buildMultiColumnPage(int numColumns, int numRows) {
    Block[] blocks = new Block[numColumns];
    for (int col = 0; col < numColumns; col++) {
      BlockBuilder builder = BigintType.BIGINT.createBlockBuilder(null, numRows);
      for (int row = 0; row < numRows; row++) {
        BigintType.BIGINT.writeLong(builder, (long) row * 10 + col);
      }
      blocks[col] = builder.build();
    }
    return new Page(blocks);
  }

  /**
   * Build a Page with a VARCHAR column and a BIGINT column from paired data.
   *
   * @param entries pairs of (String category, long value)
   * @return page with [VARCHAR, BIGINT] columns
   */
  public static Page buildCategoryValuePage(Object... entries) {
    if (entries.length % 2 != 0) {
      throw new IllegalArgumentException("Entries must be pairs of (String, Number)");
    }
    int numRows = entries.length / 2;
    BlockBuilder categoryBuilder = VarcharType.VARCHAR.createBlockBuilder(null, numRows);
    BlockBuilder valueBuilder = BigintType.BIGINT.createBlockBuilder(null, numRows);

    for (int i = 0; i < entries.length; i += 2) {
      VarcharType.VARCHAR.writeSlice(categoryBuilder, Slices.utf8Slice((String) entries[i]));
      BigintType.BIGINT.writeLong(valueBuilder, ((Number) entries[i + 1]).longValue());
    }

    return new Page(categoryBuilder.build(), valueBuilder.build());
  }

  /**
   * Drain all pages from an operator into a list.
   *
   * @param op the operator to drain
   * @return list of all pages produced by the operator
   */
  public static List<Page> drainOperator(Operator op) {
    List<Page> result = new ArrayList<>();
    Page page;
    while ((page = op.processNextBatch()) != null) {
      result.add(page);
    }
    return result;
  }
}
