/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.analyzer.validation;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.*;

import io.trino.sql.tree.Statement;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opensearch.dqe.parser.DqeSqlParser;
import org.opensearch.dqe.parser.DqeUnsupportedOperationException;

@DisplayName("UnsupportedConstructValidator")
class UnsupportedConstructValidatorTest {

  private UnsupportedConstructValidator validator;
  private DqeSqlParser parser;

  @BeforeEach
  void setUp() {
    validator = new UnsupportedConstructValidator();
    parser = new DqeSqlParser();
  }

  @Test
  @DisplayName("Simple SELECT * FROM table passes validation")
  void simpleSelectPasses() {
    Statement stmt = parser.parse("SELECT * FROM my_table");
    assertDoesNotThrow(() -> validator.validate(stmt));
  }

  @Test
  @DisplayName("SELECT with WHERE passes validation")
  void selectWithWherePasses() {
    Statement stmt = parser.parse("SELECT name, age FROM t WHERE age > 18");
    assertDoesNotThrow(() -> validator.validate(stmt));
  }

  @Test
  @DisplayName("SELECT with ORDER BY passes validation")
  void selectWithOrderByPasses() {
    Statement stmt = parser.parse("SELECT * FROM t ORDER BY name ASC");
    assertDoesNotThrow(() -> validator.validate(stmt));
  }

  @Test
  @DisplayName("SELECT with LIMIT passes validation")
  void selectWithLimitPasses() {
    Statement stmt = parser.parse("SELECT * FROM t LIMIT 10");
    assertDoesNotThrow(() -> validator.validate(stmt));
  }

  @Test
  @DisplayName("SELECT with table alias passes")
  void selectWithAliasPasses() {
    Statement stmt = parser.parse("SELECT e.name FROM employees e WHERE e.age > 18");
    assertDoesNotThrow(() -> validator.validate(stmt));
  }

  @Test
  @DisplayName("GROUP BY is rejected")
  void groupByRejected() {
    Statement stmt = parser.parse("SELECT department, COUNT(*) FROM t GROUP BY department");
    DqeUnsupportedOperationException ex =
        assertThrows(DqeUnsupportedOperationException.class, () -> validator.validate(stmt));
    assertThat(ex.getMessage(), containsString("GROUP BY"));
  }

  @Test
  @DisplayName("HAVING is rejected")
  void havingRejected() {
    Statement stmt =
        parser.parse(
            "SELECT department, COUNT(*) FROM t GROUP BY department HAVING COUNT(*) > 5");
    DqeUnsupportedOperationException ex =
        assertThrows(DqeUnsupportedOperationException.class, () -> validator.validate(stmt));
    // Could be rejected for GROUP BY first
    assertNotNull(ex.getMessage());
  }

  @Test
  @DisplayName("DISTINCT is rejected")
  void distinctRejected() {
    Statement stmt = parser.parse("SELECT DISTINCT name FROM t");
    DqeUnsupportedOperationException ex =
        assertThrows(DqeUnsupportedOperationException.class, () -> validator.validate(stmt));
    assertThat(ex.getMessage(), containsString("DISTINCT"));
  }

  @Test
  @DisplayName("JOIN is rejected")
  void joinRejected() {
    Statement stmt =
        parser.parse("SELECT a.name FROM employees a JOIN departments b ON a.dept_id = b.id");
    DqeUnsupportedOperationException ex =
        assertThrows(DqeUnsupportedOperationException.class, () -> validator.validate(stmt));
    assertThat(ex.getMessage(), containsString("JOIN"));
  }

  @Test
  @DisplayName("Subquery is rejected")
  void subqueryRejected() {
    Statement stmt = parser.parse("SELECT * FROM t WHERE id IN (SELECT id FROM other)");
    DqeUnsupportedOperationException ex =
        assertThrows(DqeUnsupportedOperationException.class, () -> validator.validate(stmt));
    assertThat(ex.getMessage(), containsString("subquery"));
  }

  @Test
  @DisplayName("UNION is rejected")
  void unionRejected() {
    Statement stmt = parser.parse("SELECT name FROM t1 UNION ALL SELECT name FROM t2");
    DqeUnsupportedOperationException ex =
        assertThrows(DqeUnsupportedOperationException.class, () -> validator.validate(stmt));
    assertThat(ex.getMessage(), containsString("UNION"));
  }

  @Test
  @DisplayName("INTERSECT is rejected")
  void intersectRejected() {
    Statement stmt = parser.parse("SELECT name FROM t1 INTERSECT SELECT name FROM t2");
    DqeUnsupportedOperationException ex =
        assertThrows(DqeUnsupportedOperationException.class, () -> validator.validate(stmt));
    assertThat(ex.getMessage(), containsString("INTERSECT"));
  }

  @Test
  @DisplayName("EXCEPT is rejected")
  void exceptRejected() {
    Statement stmt = parser.parse("SELECT name FROM t1 EXCEPT SELECT name FROM t2");
    DqeUnsupportedOperationException ex =
        assertThrows(DqeUnsupportedOperationException.class, () -> validator.validate(stmt));
    assertThat(ex.getMessage(), containsString("EXCEPT"));
  }

  @Test
  @DisplayName("CTE (WITH) is rejected")
  void cteRejected() {
    Statement stmt = parser.parse("WITH cte AS (SELECT * FROM t) SELECT * FROM cte");
    DqeUnsupportedOperationException ex =
        assertThrows(DqeUnsupportedOperationException.class, () -> validator.validate(stmt));
    assertThat(ex.getMessage(), containsString("WITH"));
  }

  @Test
  @DisplayName("Aggregate function COUNT is rejected")
  void aggregateCountRejected() {
    Statement stmt = parser.parse("SELECT COUNT(*) FROM t");
    DqeUnsupportedOperationException ex =
        assertThrows(DqeUnsupportedOperationException.class, () -> validator.validate(stmt));
    assertThat(ex.getMessage(), containsString("count"));
  }

  @Test
  @DisplayName("Aggregate function SUM is rejected")
  void aggregateSumRejected() {
    Statement stmt = parser.parse("SELECT SUM(salary) FROM t");
    DqeUnsupportedOperationException ex =
        assertThrows(DqeUnsupportedOperationException.class, () -> validator.validate(stmt));
    assertThat(ex.getMessage(), containsString("sum"));
  }

  @Test
  @DisplayName("Window function is rejected")
  void windowFunctionRejected() {
    Statement stmt =
        parser.parse("SELECT ROW_NUMBER() OVER (ORDER BY salary DESC) FROM employees");
    DqeUnsupportedOperationException ex =
        assertThrows(DqeUnsupportedOperationException.class, () -> validator.validate(stmt));
    assertThat(ex.getMessage(), containsString("window function"));
  }
}
