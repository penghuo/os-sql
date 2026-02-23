/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.parser;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.trino.sql.tree.Query;
import io.trino.sql.tree.Statement;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class DqeSqlParserTest {

  private DqeSqlParser parser;

  @BeforeEach
  void setUp() {
    parser = new DqeSqlParser();
  }

  // ---------------------------------------------------------------------------
  // Valid SELECT queries
  // ---------------------------------------------------------------------------
  @Nested
  @DisplayName("Valid SELECT queries")
  class ValidSelectQueries {

    @Test
    @DisplayName("Simple SELECT *")
    void selectStar() {
      Statement stmt = parser.parse("SELECT * FROM my_table");
      assertNotNull(stmt);
      assertThat(stmt, instanceOf(Query.class));
    }

    @Test
    @DisplayName("SELECT with specific columns")
    void selectColumns() {
      Statement stmt = parser.parse("SELECT name, age, salary FROM employees");
      assertNotNull(stmt);
      assertThat(stmt, instanceOf(Query.class));
    }

    @Test
    @DisplayName("SELECT with column aliases")
    void selectWithAliases() {
      Statement stmt = parser.parse("SELECT name AS full_name, age AS years FROM employees");
      assertNotNull(stmt);
    }

    @Test
    @DisplayName("SELECT with table alias")
    void selectWithTableAlias() {
      Statement stmt = parser.parse("SELECT e.name, e.age FROM employees e");
      assertNotNull(stmt);
    }

    @Test
    @DisplayName("SELECT with schema-qualified table")
    void selectSchemaQualifiedTable() {
      Statement stmt = parser.parse("SELECT * FROM my_schema.my_table");
      assertNotNull(stmt);
    }

    @Test
    @DisplayName("SELECT DISTINCT")
    void selectDistinct() {
      Statement stmt = parser.parse("SELECT DISTINCT department FROM employees");
      assertNotNull(stmt);
    }
  }

  // ---------------------------------------------------------------------------
  // WHERE clause
  // ---------------------------------------------------------------------------
  @Nested
  @DisplayName("WHERE clause")
  class WhereClause {

    @Test
    @DisplayName("WHERE with equality")
    void whereEquality() {
      Statement stmt = parser.parse("SELECT * FROM t WHERE id = 1");
      assertNotNull(stmt);
    }

    @Test
    @DisplayName("WHERE with string comparison")
    void whereStringComparison() {
      Statement stmt = parser.parse("SELECT * FROM t WHERE name = 'Alice'");
      assertNotNull(stmt);
    }

    @Test
    @DisplayName("WHERE with AND/OR")
    void whereAndOr() {
      Statement stmt = parser.parse("SELECT * FROM t WHERE a = 1 AND (b = 2 OR c = 3)");
      assertNotNull(stmt);
    }

    @Test
    @DisplayName("WHERE with NOT")
    void whereNot() {
      Statement stmt = parser.parse("SELECT * FROM t WHERE NOT active");
      assertNotNull(stmt);
    }

    @Test
    @DisplayName("WHERE with comparison operators")
    void whereComparisonOperators() {
      Statement stmt =
          parser.parse(
              "SELECT * FROM t WHERE age > 18 AND salary <= 100000 AND id <> 0 AND score >= 50");
      assertNotNull(stmt);
    }
  }

  // ---------------------------------------------------------------------------
  // ORDER BY / LIMIT
  // ---------------------------------------------------------------------------
  @Nested
  @DisplayName("ORDER BY and LIMIT")
  class OrderByAndLimit {

    @Test
    @DisplayName("ORDER BY single column")
    void orderBySingle() {
      Statement stmt = parser.parse("SELECT * FROM t ORDER BY name");
      assertNotNull(stmt);
    }

    @Test
    @DisplayName("ORDER BY multiple columns with direction")
    void orderByMultiple() {
      Statement stmt = parser.parse("SELECT * FROM t ORDER BY name ASC, age DESC");
      assertNotNull(stmt);
    }

    @Test
    @DisplayName("ORDER BY with NULLS FIRST/LAST")
    void orderByNulls() {
      Statement stmt =
          parser.parse("SELECT * FROM t ORDER BY name ASC NULLS FIRST, age DESC NULLS LAST");
      assertNotNull(stmt);
    }

    @Test
    @DisplayName("LIMIT")
    void limit() {
      Statement stmt = parser.parse("SELECT * FROM t LIMIT 10");
      assertNotNull(stmt);
    }

    @Test
    @DisplayName("OFFSET with FETCH NEXT (SQL standard)")
    void offsetWithFetch() {
      Statement stmt =
          parser.parse("SELECT * FROM t OFFSET 20 ROWS FETCH NEXT 10 ROWS ONLY");
      assertNotNull(stmt);
    }

    @Test
    @DisplayName("ORDER BY with LIMIT")
    void orderByWithLimit() {
      Statement stmt = parser.parse("SELECT * FROM t ORDER BY name LIMIT 100");
      assertNotNull(stmt);
    }
  }

  // ---------------------------------------------------------------------------
  // Expressions
  // ---------------------------------------------------------------------------
  @Nested
  @DisplayName("Expressions")
  class Expressions {

    @Test
    @DisplayName("Arithmetic expressions")
    void arithmetic() {
      Statement stmt = parser.parse("SELECT a + b, a - b, a * b, a / b, a % b FROM t");
      assertNotNull(stmt);
    }

    @Test
    @DisplayName("CAST expression")
    void cast() {
      Statement stmt =
          parser.parse("SELECT CAST(age AS VARCHAR), CAST(salary AS DOUBLE) FROM employees");
      assertNotNull(stmt);
    }

    @Test
    @DisplayName("IS NULL / IS NOT NULL")
    void isNull() {
      Statement stmt =
          parser.parse("SELECT * FROM t WHERE name IS NULL OR age IS NOT NULL");
      assertNotNull(stmt);
    }

    @Test
    @DisplayName("BETWEEN")
    void between() {
      Statement stmt = parser.parse("SELECT * FROM t WHERE age BETWEEN 18 AND 65");
      assertNotNull(stmt);
    }

    @Test
    @DisplayName("IN list")
    void inList() {
      Statement stmt = parser.parse("SELECT * FROM t WHERE status IN ('active', 'pending')");
      assertNotNull(stmt);
    }

    @Test
    @DisplayName("LIKE")
    void like() {
      Statement stmt = parser.parse("SELECT * FROM t WHERE name LIKE 'A%'");
      assertNotNull(stmt);
    }

    @Test
    @DisplayName("CASE expression")
    void caseExpression() {
      Statement stmt =
          parser.parse(
              "SELECT CASE WHEN age < 18 THEN 'minor' WHEN age < 65 THEN 'adult' ELSE 'senior' END FROM t");
      assertNotNull(stmt);
    }

    @Test
    @DisplayName("COALESCE")
    void coalesce() {
      Statement stmt = parser.parse("SELECT COALESCE(name, 'unknown') FROM t");
      assertNotNull(stmt);
    }

    @Test
    @DisplayName("NULLIF")
    void nullif() {
      Statement stmt = parser.parse("SELECT NULLIF(status, 'unknown') FROM t");
      assertNotNull(stmt);
    }

    @Test
    @DisplayName("String concatenation with ||")
    void stringConcat() {
      Statement stmt = parser.parse("SELECT first_name || ' ' || last_name FROM t");
      assertNotNull(stmt);
    }

    @Test
    @DisplayName("Decimal literal")
    void decimalLiteral() {
      Statement stmt = parser.parse("SELECT * FROM t WHERE price > 19.99");
      assertNotNull(stmt);
    }

    @Test
    @DisplayName("Negative number")
    void negativeNumber() {
      Statement stmt = parser.parse("SELECT * FROM t WHERE temperature < -10");
      assertNotNull(stmt);
    }
  }

  // ---------------------------------------------------------------------------
  // Unsupported constructs that still parse successfully
  // (validated at analysis time, not parse time)
  // ---------------------------------------------------------------------------
  @Nested
  @DisplayName("Unsupported constructs (parse ok, rejected at analysis)")
  class UnsupportedConstructsParsing {

    @Test
    @DisplayName("GROUP BY parses successfully")
    void groupBy() {
      Statement stmt = parser.parse("SELECT department, COUNT(*) FROM employees GROUP BY department");
      assertNotNull(stmt);
    }

    @Test
    @DisplayName("JOIN parses successfully")
    void join() {
      Statement stmt =
          parser.parse(
              "SELECT a.name, b.salary FROM employees a JOIN salaries b ON a.id = b.emp_id");
      assertNotNull(stmt);
    }

    @Test
    @DisplayName("Subquery parses successfully")
    void subquery() {
      Statement stmt =
          parser.parse("SELECT * FROM t WHERE id IN (SELECT id FROM other_table)");
      assertNotNull(stmt);
    }

    @Test
    @DisplayName("HAVING parses successfully")
    void having() {
      Statement stmt =
          parser.parse(
              "SELECT department, COUNT(*) FROM employees GROUP BY department HAVING COUNT(*) > 5");
      assertNotNull(stmt);
    }

    @Test
    @DisplayName("UNION parses successfully")
    void union() {
      Statement stmt =
          parser.parse("SELECT name FROM t1 UNION ALL SELECT name FROM t2");
      assertNotNull(stmt);
    }

    @Test
    @DisplayName("Window function parses successfully")
    void windowFunction() {
      Statement stmt =
          parser.parse("SELECT name, ROW_NUMBER() OVER (ORDER BY salary DESC) FROM employees");
      assertNotNull(stmt);
    }
  }

  // ---------------------------------------------------------------------------
  // Invalid syntax (error handling)
  // ---------------------------------------------------------------------------
  @Nested
  @DisplayName("Invalid SQL syntax")
  class InvalidSyntax {

    @Test
    @DisplayName("Missing FROM clause")
    void missingFrom() {
      DqeParsingException ex = assertThrows(DqeParsingException.class, () -> parser.parse("SELECT"));
      assertThat(ex.getMessage(), notNullValue());
      assertEquals(DqeErrorCode.PARSING_ERROR, ex.getErrorCode());
      assertThat(ex.getLineNumber(), greaterThan(0));
      assertThat(ex.getColumnNumber(), greaterThan(0));
    }

    @Test
    @DisplayName("Incomplete WHERE clause")
    void incompleteWhere() {
      DqeParsingException ex =
          assertThrows(DqeParsingException.class, () -> parser.parse("SELECT * FROM t WHERE"));
      assertEquals(DqeErrorCode.PARSING_ERROR, ex.getErrorCode());
    }

    @Test
    @DisplayName("Invalid token")
    void invalidToken() {
      DqeParsingException ex =
          assertThrows(
              DqeParsingException.class, () -> parser.parse("SELECT * FROM t WHERE @@@"));
      assertEquals(DqeErrorCode.PARSING_ERROR, ex.getErrorCode());
    }

    @Test
    @DisplayName("Unclosed string literal")
    void unclosedString() {
      DqeParsingException ex =
          assertThrows(
              DqeParsingException.class,
              () -> parser.parse("SELECT * FROM t WHERE name = 'unclosed"));
      assertEquals(DqeErrorCode.PARSING_ERROR, ex.getErrorCode());
    }

    @Test
    @DisplayName("Empty SQL string")
    void emptyString() {
      DqeParsingException ex = assertThrows(DqeParsingException.class, () -> parser.parse(""));
      assertEquals(DqeErrorCode.PARSING_ERROR, ex.getErrorCode());
    }

    @Test
    @DisplayName("Parsing exception preserves line number")
    void lineNumber() {
      DqeParsingException ex =
          assertThrows(
              DqeParsingException.class,
              () -> parser.parse("SELECT *\nFROM t\nWHERE !!!"));
      assertEquals(DqeErrorCode.PARSING_ERROR, ex.getErrorCode());
      assertThat(ex.getLineNumber(), greaterThan(0));
    }

    @Test
    @DisplayName("Parsing exception wraps cause")
    void causeIsPreserved() {
      DqeParsingException ex =
          assertThrows(DqeParsingException.class, () -> parser.parse("INVALID SQL GARBAGE"));
      assertNotNull(ex.getCause());
      assertThat(ex.getCause(), instanceOf(io.trino.sql.parser.ParsingException.class));
    }
  }

  // ---------------------------------------------------------------------------
  // Exception hierarchy unit tests
  // ---------------------------------------------------------------------------
  @Nested
  @DisplayName("Exception hierarchy")
  class ExceptionHierarchy {

    @Test
    @DisplayName("DqeException carries error code")
    void dqeExceptionErrorCode() {
      DqeException ex = new DqeException("test error", DqeErrorCode.INTERNAL_ERROR);
      assertEquals("test error", ex.getMessage());
      assertEquals(DqeErrorCode.INTERNAL_ERROR, ex.getErrorCode());
    }

    @Test
    @DisplayName("DqeException with cause")
    void dqeExceptionWithCause() {
      RuntimeException cause = new RuntimeException("root cause");
      DqeException ex = new DqeException("wrapped", DqeErrorCode.EXECUTION_ERROR, cause);
      assertEquals("wrapped", ex.getMessage());
      assertEquals(DqeErrorCode.EXECUTION_ERROR, ex.getErrorCode());
      assertEquals(cause, ex.getCause());
    }

    @Test
    @DisplayName("DqeUnsupportedOperationException message format")
    void unsupportedOperation() {
      DqeUnsupportedOperationException ex = new DqeUnsupportedOperationException("GROUP BY");
      assertThat(ex.getMessage(), containsString("GROUP BY"));
      assertThat(ex.getMessage(), containsString("does not support"));
      assertEquals("GROUP BY", ex.getConstruct());
      assertEquals(DqeErrorCode.UNSUPPORTED_OPERATION, ex.getErrorCode());
    }

    @Test
    @DisplayName("DqeUnsupportedOperationException with detail")
    void unsupportedOperationWithDetail() {
      DqeUnsupportedOperationException ex =
          new DqeUnsupportedOperationException("JOIN", "only single-table queries in Phase 1");
      assertThat(ex.getMessage(), containsString("JOIN"));
      assertThat(ex.getMessage(), containsString("only single-table queries in Phase 1"));
      assertEquals("JOIN", ex.getConstruct());
    }

    @Test
    @DisplayName("DqeTypeMismatchException fields")
    void typeMismatch() {
      DqeTypeMismatchException ex =
          new DqeTypeMismatchException("age", "INTEGER", "VARCHAR");
      assertThat(ex.getMessage(), containsString("age"));
      assertThat(ex.getMessage(), containsString("INTEGER"));
      assertThat(ex.getMessage(), containsString("VARCHAR"));
      assertEquals("age", ex.getExpression());
      assertEquals("INTEGER", ex.getExpectedType());
      assertEquals("VARCHAR", ex.getActualType());
      assertEquals(DqeErrorCode.TYPE_MISMATCH, ex.getErrorCode());
    }

    @Test
    @DisplayName("DqeAccessDeniedException fields")
    void accessDenied() {
      DqeAccessDeniedException ex = new DqeAccessDeniedException("secret_index");
      assertThat(ex.getMessage(), containsString("secret_index"));
      assertEquals("secret_index", ex.getResource());
      assertEquals(DqeErrorCode.ACCESS_DENIED, ex.getErrorCode());
    }

    @Test
    @DisplayName("DqeAccessDeniedException with detail")
    void accessDeniedWithDetail() {
      DqeAccessDeniedException ex =
          new DqeAccessDeniedException("secret_index", "requires read permission");
      assertThat(ex.getMessage(), containsString("requires read permission"));
      assertEquals("secret_index", ex.getResource());
    }

    @Test
    @DisplayName("DqeErrorCode has 19 values")
    void errorCodeCount() {
      assertEquals(19, DqeErrorCode.values().length);
    }

    @Test
    @DisplayName("All exception subclasses extend DqeException")
    void exceptionInheritance() {
      assertThat(new DqeParsingException("msg", 1, 1), instanceOf(DqeException.class));
      assertThat(new DqeUnsupportedOperationException("X"), instanceOf(DqeException.class));
      assertThat(new DqeTypeMismatchException("x", "A", "B"), instanceOf(DqeException.class));
      assertThat(new DqeAccessDeniedException("r"), instanceOf(DqeException.class));
    }
  }
}
