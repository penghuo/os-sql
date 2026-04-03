/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class QuerySummaryExtractorTest {

  @Test
  void extracts_command_structure_from_ppl() {
    assertEquals(
        "source | where | stats",
        QuerySummaryExtractor.extractPPLSummary(
            "source=logs | where status=500 | stats count() by host"));
  }

  @Test
  void extracts_source_only() {
    assertEquals("source", QuerySummaryExtractor.extractPPLSummary("source=logs"));
  }

  @Test
  void extracts_complex_pipeline() {
    assertEquals(
        "source | where | sort | head | fields",
        QuerySummaryExtractor.extractPPLSummary(
            "source=logs | where age > 30 | sort age | head 10 | fields name, age"));
  }

  @Test
  void returns_unknown_for_empty_query() {
    assertEquals("unknown", QuerySummaryExtractor.extractPPLSummary(""));
  }

  @Test
  void case_insensitive() {
    assertEquals(
        "source | where | stats",
        QuerySummaryExtractor.extractPPLSummary("SOURCE=logs | WHERE status=200 | STATS count()"));
  }
}
