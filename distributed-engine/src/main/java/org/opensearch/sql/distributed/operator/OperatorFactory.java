/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.operator;

import org.opensearch.sql.distributed.context.OperatorContext;

/**
 * Factory for creating Operator instances. Each Pipeline holds a list of OperatorFactories and uses
 * them to create one Operator per Driver. Ported from Trino's io.trino.operator.OperatorFactory.
 */
public interface OperatorFactory {

  /** Creates a new Operator instance for the given context. */
  Operator createOperator(OperatorContext operatorContext);

  /** Signals that no more operators will be created from this factory. */
  void noMoreOperators();
}
