/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.executor;

import org.apache.calcite.rel.RelNode;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.common.response.ResponseListener;

/**
 * Interface for executing queries through the distributed engine. Implementations convert a Calcite
 * RelNode into a distributed execution plan and execute it across cluster nodes.
 *
 * <p>If the query pattern is unsupported (e.g., joins, window functions), implementations should
 * throw {@link UnsupportedOperationException} to trigger fallback to the DSL path.
 */
public interface DistributedQueryExecutor {

  /**
   * Execute the given RelNode plan through the distributed engine.
   *
   * @param plan the optimized Calcite RelNode
   * @param context the Calcite plan context
   * @param listener callback for query response
   * @throws UnsupportedOperationException if the query pattern is not supported by the distributed
   *     engine
   */
  void execute(
      RelNode plan,
      CalcitePlanContext context,
      ResponseListener<ExecutionEngine.QueryResponse> listener);

  /**
   * Explain the distributed execution plan for the given RelNode.
   *
   * @param plan the optimized Calcite RelNode
   * @param context the Calcite plan context
   * @return distributed plan info (stages, operators) to be merged into the explain response
   * @throws UnsupportedOperationException if the query pattern is not supported
   */
  ExecutionEngine.DistributedExplainInfo explain(RelNode plan, CalcitePlanContext context);
}
