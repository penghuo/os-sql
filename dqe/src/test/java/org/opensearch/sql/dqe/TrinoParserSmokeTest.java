/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import io.trino.sql.parser.SqlParser;
import io.trino.sql.tree.Statement;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("Trino parser smoke test")
class TrinoParserSmokeTest {

  @Test
  @DisplayName("Parse a simple SELECT statement")
  void parseSimpleSelect() {
    SqlParser parser = new SqlParser();
    Statement stmt = parser.createStatement("SELECT a, b FROM t WHERE x = 1");
    assertNotNull(stmt);
  }

  @Test
  @DisplayName("Parse SELECT with GROUP BY and ORDER BY")
  void parseGroupBy() {
    SqlParser parser = new SqlParser();
    Statement stmt =
        parser.createStatement(
            "SELECT category, COUNT(*) FROM logs WHERE status = 200 "
                + "GROUP BY category ORDER BY COUNT(*) DESC LIMIT 10");
    assertNotNull(stmt);
  }
}
