/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.trino.parser;

import io.trino.sql.parser.SqlParser;
import io.trino.sql.tree.DefaultTraversalVisitor;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.Query;
import io.trino.sql.tree.QuerySpecification;
import io.trino.sql.tree.Statement;
import io.trino.sql.tree.Table;
import java.util.HashSet;
import java.util.Set;

public class DqeSqlParser {

  private final SqlParser parser = new SqlParser();

  public Statement parse(String sql) {
    return parser.createStatement(sql);
  }

  /**
   * Parse a predicate string into a Trino Expression AST. Wraps the predicate in a synthetic SELECT
   * statement to leverage Trino's full SQL parser.
   *
   * @param predicateString the predicate expression (e.g. "status > 200 AND category = 'error'")
   * @return the parsed Expression
   */
  public Expression parseExpression(String predicateString) {
    Statement stmt = parser.createStatement("SELECT 1 FROM _t WHERE " + predicateString);
    QuerySpecification spec = (QuerySpecification) ((Query) stmt).getQueryBody();
    return spec.getWhere()
        .orElseThrow(
            () -> new IllegalArgumentException("Failed to parse predicate: " + predicateString));
  }

  public Set<String> extractTableNames(String sql) {
    Statement stmt = parse(sql);
    Set<String> tables = new HashSet<>();
    new DefaultTraversalVisitor<Void>() {
      @Override
      protected Void visitTable(Table node, Void context) {
        tables.add(node.getName().toString());
        return null;
      }
    }.process(stmt, null);
    return tables;
  }
}
