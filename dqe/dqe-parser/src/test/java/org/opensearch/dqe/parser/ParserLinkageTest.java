/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.parser;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.trino.sql.tree.Query;
import io.trino.sql.tree.Statement;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * P-5: Linkage smoke test for the parser.
 *
 * <p>Validates that Trino parser classes load correctly (no {@link ClassNotFoundException} or
 * {@link NoSuchMethodError}) and that the shading relocations preserve all necessary class
 * dependencies. This test exercises the full parse path from SQL string to Statement AST.
 */
class ParserLinkageTest {

  @Test
  @DisplayName("DqeSqlParser loads and instantiates without ClassNotFoundException")
  void parserInstantiates() {
    assertDoesNotThrow(DqeSqlParser::new, "DqeSqlParser constructor should not throw");
  }

  @Test
  @DisplayName("Trino Statement class loads correctly")
  void statementClassLoads() {
    assertDoesNotThrow(
        () -> Class.forName("io.trino.sql.tree.Statement"),
        "io.trino.sql.tree.Statement should be loadable");
  }

  @Test
  @DisplayName("Trino SqlParser class loads correctly")
  void sqlParserClassLoads() {
    assertDoesNotThrow(
        () -> Class.forName("io.trino.sql.parser.SqlParser"),
        "io.trino.sql.parser.SqlParser should be loadable");
  }

  @Test
  @DisplayName("Full parse round-trip: SQL -> Statement AST -> verify type")
  void fullParseRoundTrip() {
    DqeSqlParser parser = new DqeSqlParser();

    Statement stmt =
        parser.parse(
            "SELECT id, name, CAST(salary AS DOUBLE) FROM employees WHERE age > 25 ORDER BY name"
                + " LIMIT 100");
    assertNotNull(stmt, "Parsed statement should not be null");
    assertThat("Statement should be a Query", stmt, instanceOf(Query.class));
  }

  @Test
  @DisplayName("Parser handles complex expressions without linkage errors")
  void complexExpressionsLinkage() {
    DqeSqlParser parser = new DqeSqlParser();

    Statement stmt =
        parser.parse(
            "SELECT "
                + "CASE WHEN status = 'active' THEN 1 ELSE 0 END, "
                + "COALESCE(name, 'N/A'), "
                + "NULLIF(score, 0), "
                + "CAST(created_at AS TIMESTAMP), "
                + "price * quantity AS total "
                + "FROM orders "
                + "WHERE amount BETWEEN 10 AND 1000 "
                + "AND category IN ('A', 'B', 'C') "
                + "AND description LIKE '%urgent%' "
                + "AND deleted_at IS NULL "
                + "ORDER BY total DESC NULLS LAST "
                + "OFFSET 10 ROWS FETCH NEXT 50 ROWS ONLY");
    assertThat(stmt, notNullValue());
  }

  @Test
  @DisplayName("DqeParsingException linkage: error path works correctly")
  void errorPathLinkage() {
    DqeSqlParser parser = new DqeSqlParser();

    DqeParsingException e =
        assertThrows(
            DqeParsingException.class,
            () -> parser.parse("NOT VALID SQL @@@ !!!"),
            "Invalid SQL should throw DqeParsingException");
    assertNotNull(e.getMessage(), "Error message should not be null");
    assertNotNull(e.getErrorCode(), "Error code should not be null");
    assertNotNull(e.getCause(), "Cause should be the original Trino exception");
  }

  @Test
  @DisplayName("Trino ANTLR grammar loads correctly (parse with keywords)")
  void antlrGrammarLoads() {
    DqeSqlParser parser = new DqeSqlParser();

    Statement stmt =
        parser.parse(
            "SELECT DISTINCT t.* FROM my_catalog.my_schema.my_table t "
                + "WHERE t.value IS NOT NULL ORDER BY t.value ASC NULLS FIRST LIMIT 1");
    assertThat(stmt, notNullValue());
  }
}
