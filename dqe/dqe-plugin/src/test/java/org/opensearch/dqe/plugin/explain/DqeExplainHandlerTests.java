/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.plugin.explain;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.opensearch.dqe.parser.DqeErrorCode;
import org.opensearch.dqe.parser.DqeException;
import org.opensearch.dqe.parser.DqeSqlParser;
import org.opensearch.dqe.plugin.request.DqeQueryRequest;

@DisplayName("DqeExplainHandler")
class DqeExplainHandlerTests {

  private DqeExplainHandler handler;

  @BeforeEach
  void setUp() {
    DqeSqlParser parser = new DqeSqlParser();
    PlanPrinter planPrinter = new PlanPrinter();
    handler = new DqeExplainHandler(parser, planPrinter);
  }

  @Nested
  @DisplayName("Valid queries")
  class ValidQueries {

    @Test
    @DisplayName("explains simple SELECT")
    void explainsSimpleSelect() {
      DqeQueryRequest request = new DqeQueryRequest("SELECT 1", "dqe");
      String output = handler.explain(request);
      assertNotNull(output);
      assertTrue(output.contains("DQE Query Plan"));
    }

    @Test
    @DisplayName("explains SELECT with FROM")
    void explainsSelectWithFrom() {
      DqeQueryRequest request = new DqeQueryRequest("SELECT * FROM my_table", "dqe");
      String output = handler.explain(request);
      assertNotNull(output);
      assertTrue(output.length() > 0);
    }

    @Test
    @DisplayName("explains SELECT with WHERE")
    void explainsSelectWithWhere() {
      DqeQueryRequest request = new DqeQueryRequest("SELECT a FROM t WHERE a > 10", "dqe");
      String output = handler.explain(request);
      assertNotNull(output);
      assertTrue(output.contains("DQE Query Plan"));
    }
  }

  @Nested
  @DisplayName("Error cases")
  class ErrorCases {

    @Test
    @DisplayName("rejects null query")
    void rejectsNullQuery() {
      DqeQueryRequest request = new DqeQueryRequest(null, "dqe");
      DqeException ex = assertThrows(DqeException.class, () -> handler.explain(request));
      assertEquals(DqeErrorCode.INVALID_REQUEST, ex.getErrorCode());
    }

    @Test
    @DisplayName("rejects empty query")
    void rejectsEmptyQuery() {
      DqeQueryRequest request = new DqeQueryRequest("", "dqe");
      DqeException ex = assertThrows(DqeException.class, () -> handler.explain(request));
      assertEquals(DqeErrorCode.INVALID_REQUEST, ex.getErrorCode());
    }

    @Test
    @DisplayName("rejects blank query")
    void rejectsBlankQuery() {
      DqeQueryRequest request = new DqeQueryRequest("   ", "dqe");
      DqeException ex = assertThrows(DqeException.class, () -> handler.explain(request));
      assertEquals(DqeErrorCode.INVALID_REQUEST, ex.getErrorCode());
    }

    @Test
    @DisplayName("throws on invalid SQL")
    void throwsOnInvalidSql() {
      DqeQueryRequest request = new DqeQueryRequest("NOT VALID SQL !!!", "dqe");
      assertThrows(Exception.class, () -> handler.explain(request));
    }
  }
}
