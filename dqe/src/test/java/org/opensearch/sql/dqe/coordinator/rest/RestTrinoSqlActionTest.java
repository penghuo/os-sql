/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.coordinator.rest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opensearch.rest.RestRequest;

@DisplayName("RestTrinoSqlAction routes and configuration")
class RestTrinoSqlActionTest {

  private final RestTrinoSqlAction action = new RestTrinoSqlAction();

  @Test
  @DisplayName("routes() returns query and explain POST endpoints")
  void routesReturnCorrectEndpoints() {
    List<RestTrinoSqlAction.Route> routes = action.routes();
    assertEquals(2, routes.size());

    Set<String> paths =
        routes.stream().map(RestTrinoSqlAction.Route::getPath).collect(Collectors.toSet());
    assertTrue(paths.contains("/_plugins/_trino_sql"), "Should contain query endpoint");
    assertTrue(paths.contains("/_plugins/_trino_sql/_explain"), "Should contain explain endpoint");

    // Both routes should be POST
    for (RestTrinoSqlAction.Route route : routes) {
      assertEquals(RestRequest.Method.POST, route.getMethod());
    }
  }

  @Test
  @DisplayName("getName() returns expected action name")
  void getNameReturnsExpected() {
    assertEquals("trino_sql_query_action", action.getName());
  }

  @Test
  @DisplayName("extractQuery parses query from JSON body")
  void extractQueryParsesJson() {
    String json = "{\"query\": \"SELECT * FROM logs\"}";
    assertEquals("SELECT * FROM logs", RestTrinoSqlAction.extractQuery(json));
  }

  @Test
  @DisplayName("extractQuery handles query with escaped quotes")
  void extractQueryWithEscapedQuotes() {
    String json = "{\"query\": \"SELECT * FROM logs WHERE msg = \\\"hello\\\"\"}";
    assertEquals("SELECT * FROM logs WHERE msg = \"hello\"", RestTrinoSqlAction.extractQuery(json));
  }

  @Test
  @DisplayName("extractQuery handles query with other fields in JSON")
  void extractQueryWithExtraFields() {
    String json = "{\"format\": \"json\", \"query\": \"SELECT 1\", \"other\": true}";
    assertEquals("SELECT 1", RestTrinoSqlAction.extractQuery(json));
  }

  @Test
  @DisplayName("extractQuery throws on missing query field")
  void extractQueryThrowsOnMissingField() {
    assertThrows(
        IllegalArgumentException.class,
        () -> RestTrinoSqlAction.extractQuery("{\"sql\": \"SELECT 1\"}"));
  }

  @Test
  @DisplayName("extractQuery throws on malformed JSON")
  void extractQueryThrowsOnMalformedJson() {
    assertThrows(
        IllegalArgumentException.class, () -> RestTrinoSqlAction.extractQuery("{\"query\""));
  }

  @Test
  @DisplayName("escapeJson handles null input")
  void escapeJsonHandlesNull() {
    assertEquals("null", RestTrinoSqlAction.escapeJson(null));
  }

  @Test
  @DisplayName("escapeJson escapes special characters")
  void escapeJsonEscapesSpecialChars() {
    assertEquals("hello\\nworld", RestTrinoSqlAction.escapeJson("hello\nworld"));
    assertEquals("a\\\\b", RestTrinoSqlAction.escapeJson("a\\b"));
    assertEquals("a\\\"b", RestTrinoSqlAction.escapeJson("a\"b"));
  }
}
