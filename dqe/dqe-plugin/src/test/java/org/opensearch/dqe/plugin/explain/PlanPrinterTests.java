/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.plugin.explain;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.trino.sql.tree.Statement;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opensearch.dqe.parser.DqeSqlParser;

@DisplayName("PlanPrinter")
class PlanPrinterTests {

  private PlanPrinter planPrinter;
  private DqeSqlParser sqlParser;

  @BeforeEach
  void setUp() {
    planPrinter = new PlanPrinter();
    sqlParser = new DqeSqlParser();
  }

  @Test
  @DisplayName("prints header line")
  void printsHeader() {
    Statement stmt = sqlParser.parse("SELECT 1");
    String output = planPrinter.print(stmt);
    assertTrue(output.contains("DQE Query Plan (Phase 1 - parsed AST)"));
  }

  @Test
  @DisplayName("prints separator line")
  void printsSeparator() {
    Statement stmt = sqlParser.parse("SELECT 1");
    String output = planPrinter.print(stmt);
    assertTrue(output.contains("====="));
  }

  @Test
  @DisplayName("prints AST content")
  void printsAstContent() {
    Statement stmt = sqlParser.parse("SELECT 1");
    String output = planPrinter.print(stmt);
    assertNotNull(output);
    // The AST toString should contain something representing the query
    assertTrue(output.length() > 50);
  }

  @Test
  @DisplayName("prints different output for different queries")
  void differentQueriesProduceDifferentOutput() {
    Statement stmt1 = sqlParser.parse("SELECT 1");
    Statement stmt2 = sqlParser.parse("SELECT * FROM foo WHERE x = 1");
    String out1 = planPrinter.print(stmt1);
    String out2 = planPrinter.print(stmt2);
    // Both should have the header, but different AST content
    assertTrue(out1.contains("DQE Query Plan"));
    assertTrue(out2.contains("DQE Query Plan"));
    // The outputs should differ in the AST portion
    assertTrue(!out1.equals(out2));
  }
}
