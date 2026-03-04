/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.trino.parser;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

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
}
