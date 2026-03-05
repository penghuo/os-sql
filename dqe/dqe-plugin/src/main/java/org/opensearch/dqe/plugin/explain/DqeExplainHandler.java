/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.plugin.explain;

import io.trino.sql.tree.Statement;
import org.opensearch.dqe.parser.DqeErrorCode;
import org.opensearch.dqe.parser.DqeException;
import org.opensearch.dqe.parser.DqeSqlParser;
import org.opensearch.dqe.plugin.request.DqeQueryRequest;

/**
 * Handles DQE explain requests: parse the SQL query and return the query plan without executing.
 *
 * <p>Phase 1 implementation uses only the {@link DqeSqlParser} to parse the query and print the
 * AST. Full implementation (when DqeAnalyzer and DqeMetadata are available) will show resolved
 * tables/columns, pushed-down predicates (Query DSL), remaining filter predicates, projection
 * columns, sort specification, and limit.
 *
 * <p>Constructor signature matches frozen interface: {@code DqeExplainHandler(DqeSqlParser,
 * DqeAnalyzer, DqeMetadata, PlanPrinter)}. Since DqeAnalyzer and DqeMetadata don't exist yet, this
 * implementation accepts them as null.
 */
public class DqeExplainHandler {

  private final DqeSqlParser parser;
  private final PlanPrinter planPrinter;

  // DqeAnalyzer and DqeMetadata will be wired when available (PL-10)

  /**
   * Creates a new explain handler.
   *
   * @param parser the SQL parser
   * @param planPrinter the plan printer
   */
  public DqeExplainHandler(DqeSqlParser parser, PlanPrinter planPrinter) {
    this.parser = parser;
    this.planPrinter = planPrinter;
  }

  /**
   * Explain a DQE query request, returning the plan as a formatted string.
   *
   * @param request the parsed query request
   * @return the explain output
   * @throws DqeException on parsing or analysis errors
   */
  public String explain(DqeQueryRequest request) throws DqeException {
    if (request.getQuery() == null || request.getQuery().isBlank()) {
      throw new DqeException("Cannot explain empty query", DqeErrorCode.INVALID_REQUEST);
    }

    Statement statement = parser.parse(request.getQuery());
    return planPrinter.print(statement);
  }
}
