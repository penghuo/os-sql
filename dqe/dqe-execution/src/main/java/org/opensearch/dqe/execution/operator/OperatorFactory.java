/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.execution.operator;

/** Factory for creating operator instances. Kept for Phase 2+ parallel pipeline instantiation. */
public interface OperatorFactory {

  /** Creates an operator instance for a specific pipeline invocation. */
  Operator createOperator(OperatorContext operatorContext);

  /** Duplicates this factory for parallel pipeline instantiation. */
  OperatorFactory duplicate();
}
