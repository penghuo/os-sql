/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.plugin.explain;

import io.trino.sql.tree.Statement;

/**
 * Prints query plans in a human-readable text format.
 *
 * <p>Phase 1 implementation prints the parsed AST from {@link Statement#toString()}. Full
 * implementation (post PL-10, when AnalyzedQuery is available) will show resolved tables/columns,
 * pushed-down predicates, remaining filters, projection, sort, and limit.
 */
public class PlanPrinter {

  public PlanPrinter() {}

  /**
   * Print the parsed AST as a text plan.
   *
   * @param statement the parsed Trino SQL statement
   * @return formatted plan string
   */
  public String print(Statement statement) {
    StringBuilder sb = new StringBuilder();
    sb.append("DQE Query Plan (Phase 1 - parsed AST)\n");
    sb.append("=====================================\n");
    sb.append(statement.toString()).append("\n");
    return sb.toString();
  }
}
