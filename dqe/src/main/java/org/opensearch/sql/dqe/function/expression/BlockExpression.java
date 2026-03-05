/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.function.expression;

import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.type.Type;

/**
 * A vectorized expression that evaluates an entire Page at once, producing an output Block. This is
 * the core abstraction for the expression evaluation framework — all expressions (column
 * references, constants, functions, operators) implement this interface.
 */
public interface BlockExpression {

  /** Evaluate this expression against the given page and return an output Block. */
  Block evaluate(Page page);

  /** Return the Trino type of the output Block produced by this expression. */
  Type getType();
}
