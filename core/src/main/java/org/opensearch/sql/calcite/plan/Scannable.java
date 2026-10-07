/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.plan;

import org.apache.calcite.linq4j.Enumerable;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * The customized table scan is implemented in OpenSearch module, to invoke this scan() method in
 * core module, we add this interface. Now the only implementation is CalciteEnumerableIndexScan.
 * When a RelNode after optimization is a Scannable, we can directly invoke scan() method to get the
 * result of the scan instead of codegen and compile via Linq4j expression.
 */
public interface Scannable {

  /** Source id meaning "this scan is not being observed for progress". */
  long NO_PROGRESS_SOURCE = -1L;

  public Enumerable<@Nullable Object> scan();

  /**
   * Scans on behalf of one physical plan position.
   *
   * <p>Generated code calls this overload with a constant assigned while the position was being
   * code-generated, which is the only way two positions can be told apart: Calcite canonicalizes
   * equal plan nodes, so an equijoin of an index to itself leaves both of its scan positions
   * pointing at the same node object. Reading an id off the node at runtime would merge them;
   * baking a different constant into each generated call site keeps them distinct no matter how
   * often either is re-enumerated.
   *
   * @param progressSourceId progress source this position reports into, or {@link
   *     #NO_PROGRESS_SOURCE}
   */
  default Enumerable<@Nullable Object> scan(long progressSourceId) {
    return scan();
  }
}
