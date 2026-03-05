/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.function.expression;

import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.type.Type;
import lombok.Getter;

/** Returns the Block at a given column index from the input Page (zero-copy). */
@Getter
public class ColumnReference implements BlockExpression {

  private final int columnIndex;
  private final Type type;

  public ColumnReference(int columnIndex, Type type) {
    this.columnIndex = columnIndex;
    this.type = type;
  }

  @Override
  public Block evaluate(Page page) {
    return page.getBlock(columnIndex);
  }
}
