/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.plugin.request;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@DisplayName("DqeQueryRequest")
class DqeQueryRequestTests {

  @Nested
  @DisplayName("Simple constructor")
  class SimpleConstructor {

    @Test
    @DisplayName("creates request with query and engine")
    void createsWithQueryAndEngine() {
      DqeQueryRequest req = new DqeQueryRequest("SELECT 1", "dqe");
      assertEquals("SELECT 1", req.getQuery());
      assertEquals("dqe", req.getEngine());
      assertEquals(0, req.getFetchSize());
      assertTrue(req.getSessionProperties().isEmpty());
      assertNotNull(req.getQueryId());
      assertTrue(req.getQueryMaxMemoryBytes().isEmpty());
      assertTrue(req.getQueryTimeout().isEmpty());
    }
  }

  @Nested
  @DisplayName("Builder")
  class BuilderTests {

    @Test
    @DisplayName("builds full request via builder")
    void buildsFullRequest() {
      DqeQueryRequest req =
          DqeQueryRequest.builder()
              .query("SELECT * FROM t")
              .engine("calcite")
              .fetchSize(50)
              .sessionProperties(Map.of("k", "v"))
              .queryId("test-id")
              .queryMaxMemoryBytes(1024L)
              .queryTimeout(Duration.ofSeconds(30))
              .build();

      assertEquals("SELECT * FROM t", req.getQuery());
      assertEquals("calcite", req.getEngine());
      assertEquals(50, req.getFetchSize());
      assertEquals(Map.of("k", "v"), req.getSessionProperties());
      assertEquals("test-id", req.getQueryId());
      assertEquals(1024L, req.getQueryMaxMemoryBytes().get());
      assertEquals(Duration.ofSeconds(30), req.getQueryTimeout().get());
    }

    @Test
    @DisplayName("generates query ID when not set")
    void generatesQueryId() {
      DqeQueryRequest req = DqeQueryRequest.builder().query("SELECT 1").build();
      assertNotNull(req.getQueryId());
    }

    @Test
    @DisplayName("session properties are unmodifiable")
    void sessionPropertiesUnmodifiable() {
      DqeQueryRequest req =
          DqeQueryRequest.builder()
              .query("SELECT 1")
              .sessionProperties(Map.of("k", "v"))
              .build();
      org.junit.jupiter.api.Assertions.assertThrows(
          UnsupportedOperationException.class, () -> req.getSessionProperties().put("x", "y"));
    }

    @Test
    @DisplayName("null session properties becomes empty map")
    void nullSessionPropertiesBecomesEmpty() {
      DqeQueryRequest req =
          DqeQueryRequest.builder().query("SELECT 1").sessionProperties(null).build();
      assertTrue(req.getSessionProperties().isEmpty());
    }
  }
}
