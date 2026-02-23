/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.plugin.response;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.opensearch.dqe.parser.DqeErrorCode;
import org.opensearch.dqe.parser.DqeException;

@DisplayName("DqeResponseFormatter")
class DqeResponseFormatterTests {

  private DqeResponseFormatter formatter;

  @BeforeEach
  void setUp() {
    formatter = new DqeResponseFormatter();
  }

  @Nested
  @DisplayName("Success formatting")
  class SuccessFormatting {

    @Test
    @DisplayName("formats empty result set")
    void formatsEmptyResult() {
      DqeQueryResponse response = DqeQueryResponse.builder().build();
      String json = formatter.formatSuccess(response);
      assertTrue(json.contains("\"engine\":\"dqe\""));
      assertTrue(json.contains("\"schema\":[]"));
      assertTrue(json.contains("\"data\":[]"));
    }

    @Test
    @DisplayName("formats result with schema and data")
    void formatsResultWithData() {
      DqeQueryResponse response =
          DqeQueryResponse.builder()
              .schema(
                  List.of(
                      new DqeQueryResponse.ColumnSchema("id", "integer"),
                      new DqeQueryResponse.ColumnSchema("name", "keyword")))
              .data(List.of(List.of(1, "alice"), List.of(2, "bob")))
              .build();

      String json = formatter.formatSuccess(response);
      assertTrue(json.contains("\"name\":\"id\""));
      assertTrue(json.contains("\"type\":\"integer\""));
      assertTrue(json.contains("\"name\":\"name\""));
      assertTrue(json.contains("\"type\":\"keyword\""));
      assertTrue(json.contains("[1,\"alice\"]"));
      assertTrue(json.contains("[2,\"bob\"]"));
    }

    @Test
    @DisplayName("handles null values in data")
    void handlesNullValues() {
      DqeQueryResponse response =
          DqeQueryResponse.builder()
              .schema(List.of(new DqeQueryResponse.ColumnSchema("col", "text")))
              .data(List.of(Arrays.asList((Object) null)))
              .build();

      String json = formatter.formatSuccess(response);
      assertTrue(json.contains("[null]"));
    }

    @Test
    @DisplayName("includes stats when present")
    void includesStats() {
      DqeQueryResponse.QueryStats stats =
          DqeQueryResponse.QueryStats.builder()
              .state("COMPLETED")
              .queryId("q-123")
              .elapsedMs(42)
              .rowsProcessed(100)
              .bytesProcessed(4096)
              .stages(2)
              .shardsQueried(5)
              .build();

      DqeQueryResponse response = DqeQueryResponse.builder().stats(stats).build();
      String json = formatter.formatSuccess(response);
      assertTrue(json.contains("\"stats\":"));
      assertTrue(json.contains("\"state\":\"COMPLETED\""));
      assertTrue(json.contains("\"query_id\":\"q-123\""));
      assertTrue(json.contains("\"elapsed_ms\":42"));
      assertTrue(json.contains("\"rows_processed\":100"));
      assertTrue(json.contains("\"bytes_processed\":4096"));
      assertTrue(json.contains("\"stages\":2"));
      assertTrue(json.contains("\"shards_queried\":5"));
    }

    @Test
    @DisplayName("omits stats when null")
    void omitsStatsWhenNull() {
      DqeQueryResponse response = DqeQueryResponse.builder().build();
      String json = formatter.formatSuccess(response);
      assertFalse(json.contains("\"stats\":"));
    }

    @Test
    @DisplayName("escapes special characters in strings")
    void escapesSpecialChars() {
      DqeQueryResponse response =
          DqeQueryResponse.builder()
              .schema(List.of(new DqeQueryResponse.ColumnSchema("col\"umn", "te\nxt")))
              .build();

      String json = formatter.formatSuccess(response);
      assertTrue(json.contains("col\\\"umn"));
      assertTrue(json.contains("te\\nxt"));
    }

    @Test
    @DisplayName("handles boolean and numeric data")
    void handlesBooleanAndNumeric() {
      DqeQueryResponse response =
          DqeQueryResponse.builder()
              .schema(
                  List.of(
                      new DqeQueryResponse.ColumnSchema("b", "boolean"),
                      new DqeQueryResponse.ColumnSchema("d", "double")))
              .data(List.of(List.of(true, 3.14)))
              .build();

      String json = formatter.formatSuccess(response);
      assertTrue(json.contains("true"));
      assertTrue(json.contains("3.14"));
    }
  }

  @Nested
  @DisplayName("Error formatting")
  class ErrorFormatting {

    @Test
    @DisplayName("formats error with query ID")
    void formatsErrorWithQueryId() {
      DqeException ex = new DqeException("something failed", DqeErrorCode.EXECUTION_ERROR);
      String json = formatter.formatError(ex, "q-456");
      assertTrue(json.contains("\"engine\":\"dqe\""));
      assertTrue(json.contains("\"type\":\"DqeException\""));
      assertTrue(json.contains("\"reason\":\"something failed\""));
      assertTrue(json.contains("\"error_code\":\"EXECUTION_ERROR\""));
      assertTrue(json.contains("\"status\":400"));
      assertTrue(json.contains("\"query_id\":\"q-456\""));
    }

    @Test
    @DisplayName("formats error without query ID")
    void formatsErrorWithoutQueryId() {
      DqeException ex = new DqeException("parse error", DqeErrorCode.INVALID_REQUEST);
      String json = formatter.formatError(ex, null);
      assertTrue(json.contains("\"engine\":\"dqe\""));
      assertTrue(json.contains("\"error_code\":\"INVALID_REQUEST\""));
      assertFalse(json.contains("\"query_id\""));
    }
  }
}
