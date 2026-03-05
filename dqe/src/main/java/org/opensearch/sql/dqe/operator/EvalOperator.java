/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.operator;

import io.trino.spi.Page;
import io.trino.spi.block.Block;
import java.util.List;
import org.opensearch.sql.dqe.function.expression.BlockExpression;

/**
 * Physical operator that evaluates computed expressions (SELECT-list computed columns). Each output
 * column is produced by evaluating a {@link BlockExpression} against the input Page.
 */
public class EvalOperator implements Operator {

  private final Operator source;
  private final List<BlockExpression> outputExpressions;

  public EvalOperator(Operator source, List<BlockExpression> outputExpressions) {
    this.source = source;
    this.outputExpressions = outputExpressions;
  }

  @Override
  public Page processNextBatch() {
    Page input = source.processNextBatch();
    if (input == null) {
      return null;
    }
    Block[] blocks = new Block[outputExpressions.size()];
    for (int i = 0; i < outputExpressions.size(); i++) {
      blocks[i] = outputExpressions.get(i).evaluate(input);
    }
    return new Page(blocks);
  }

  @Override
  public void close() {
    source.close();
  }
}
