/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.trino.parser;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.LogicalExpression;
import io.trino.sql.tree.Query;
import io.trino.sql.tree.Statement;
import java.util.Set;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("DqeSqlParser")
class DqeSqlParserTest {

  private final DqeSqlParser parser = new DqeSqlParser();

  @Test
  @DisplayName("Parse SELECT returns Query node")
  void parseSelect() {
    Statement stmt = parser.parse("SELECT a FROM t");
    assertInstanceOf(Query.class, stmt);
  }

  @Test
  @DisplayName("Extract table names from query")
  void extractTableNames() {
    Set<String> tables = parser.extractTableNames("SELECT a FROM logs WHERE x = 1");
    assertEquals(Set.of("logs"), tables);
  }

  @Test
  @DisplayName("Extract table names from join")
  void extractTableNamesFromJoin() {
    Set<String> tables = parser.extractTableNames("SELECT * FROM a JOIN b ON a.id = b.id");
    assertEquals(Set.of("a", "b"), tables);
  }

  @Test
  @DisplayName("Invalid SQL throws exception")
  void invalidSqlThrows() {
    assertThrows(Exception.class, () -> parser.parse("NOT VALID SQL !!!"));
  }

  @Test
  @DisplayName("parseExpression returns ComparisonExpression for simple comparison")
  void parseExpressionComparison() {
    Expression expr = parser.parseExpression("status > 200");
    assertInstanceOf(ComparisonExpression.class, expr);
    ComparisonExpression cmp = (ComparisonExpression) expr;
    assertEquals(ComparisonExpression.Operator.GREATER_THAN, cmp.getOperator());
  }

  @Test
  @DisplayName("parseExpression returns LogicalExpression for AND predicate")
  void parseExpressionLogicalAnd() {
    Expression expr = parser.parseExpression("status > 200 AND category = 'error'");
    assertInstanceOf(LogicalExpression.class, expr);
    LogicalExpression logical = (LogicalExpression) expr;
    assertEquals(LogicalExpression.Operator.AND, logical.getOperator());
    assertEquals(2, logical.getTerms().size());
  }

  @Test
  @DisplayName("parseExpression round-trips through toString()")
  void parseExpressionRoundTrip() {
    Expression original = parser.parseExpression("status > 200");
    Expression roundTripped = parser.parseExpression(original.toString());
    assertInstanceOf(ComparisonExpression.class, roundTripped);
    ComparisonExpression cmp = (ComparisonExpression) roundTripped;
    assertEquals(ComparisonExpression.Operator.GREATER_THAN, cmp.getOperator());
  }
}
