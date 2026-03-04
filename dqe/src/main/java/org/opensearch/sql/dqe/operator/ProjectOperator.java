/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.operator;

import io.trino.spi.Page;
import java.util.List;

/**
 * Physical operator that projects (selects) specific columns from input pages. Creates new pages
 * containing only the blocks at the specified column indices.
 */
public class ProjectOperator implements Operator {

  private final Operator source;
  private final int[] columnIndices;

  public ProjectOperator(Operator source, List<Integer> columnIndices) {
    this.source = source;
    this.columnIndices = columnIndices.stream().mapToInt(Integer::intValue).toArray();
  }

  @Override
  public Page processNextBatch() {
    Page page = source.processNextBatch();
    if (page == null) {
      return null;
    }

    return page.getColumns(columnIndices);
  }

  @Override
  public void close() {
    source.close();
  }
}
