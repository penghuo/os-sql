/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.planner;

/**
 * Thrown when a query pattern is not supported by the distributed engine. This triggers fallback to
 * the DSL execution path.
 *
 * <p>Examples of unsupported patterns in Phase 1: joins, window functions, correlated subqueries.
 */
public class UnsupportedPatternException extends UnsupportedOperationException {

  public UnsupportedPatternException(String message) {
    super(message);
  }

  public UnsupportedPatternException(String message, Throwable cause) {
    super(message, cause);
  }
}
