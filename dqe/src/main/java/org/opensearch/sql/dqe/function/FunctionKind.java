/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.function;

/** Classifies a registered function by its evaluation mode. */
public enum FunctionKind {
  SCALAR,
  AGGREGATE,
  WINDOW
}
