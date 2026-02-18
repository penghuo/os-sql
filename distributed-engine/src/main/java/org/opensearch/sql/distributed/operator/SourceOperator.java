/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.operator;

import org.opensearch.sql.distributed.data.Page;

/**
 * An operator that produces Pages without receiving input. Sits at the start of a pipeline. Ported
 * from Trino's io.trino.operator.SourceOperator.
 */
public interface SourceOperator extends Operator {

  @Override
  default boolean needsInput() {
    return false;
  }

  @Override
  default void addInput(Page page) {
    throw new UnsupportedOperationException(getClass().getSimpleName() + " does not accept input");
  }
}
