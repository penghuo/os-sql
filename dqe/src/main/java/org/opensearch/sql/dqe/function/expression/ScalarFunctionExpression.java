/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.function.expression;

import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.type.Type;
import java.util.List;
import org.opensearch.sql.dqe.function.scalar.ScalarFunctionImplementation;

/**
 * Evaluates a scalar function by first evaluating all child expressions to produce argument Blocks,
 * then delegating to the {@link ScalarFunctionImplementation}.
 */
public class ScalarFunctionExpression implements BlockExpression {

  private final ScalarFunctionImplementation implementation;
  private final List<BlockExpression> arguments;
  private final Type returnType;

  public ScalarFunctionExpression(
      ScalarFunctionImplementation implementation,
      List<BlockExpression> arguments,
      Type returnType) {
    this.implementation = implementation;
    this.arguments = arguments;
    this.returnType = returnType;
  }

  @Override
  public Block evaluate(Page page) {
    Block[] argBlocks = new Block[arguments.size()];
    for (int i = 0; i < arguments.size(); i++) {
      argBlocks[i] = arguments.get(i).evaluate(page);
    }
    return implementation.evaluate(argBlocks, page.getPositionCount());
  }

  @Override
  public Type getType() {
    return returnType;
  }
}
