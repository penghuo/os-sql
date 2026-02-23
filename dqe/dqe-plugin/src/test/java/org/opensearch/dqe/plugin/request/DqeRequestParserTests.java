/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.plugin.request;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.time.Duration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.opensearch.dqe.parser.DqeErrorCode;
import org.opensearch.dqe.parser.DqeException;
import org.opensearch.dqe.plugin.settings.DqeSettings;

@DisplayName("DqeRequestParser")
class DqeRequestParserTests {

  private DqeRequestParser parser;

  @BeforeEach
  void setUp() {
    DqeSettings settings = mock(DqeSettings.class);
    parser = new DqeRequestParser(settings);
  }

  @Nested
  @DisplayName("Basic parsing")
  class BasicParsing {

    @Test
    @DisplayName("parses minimal request with just query")
    void parsesMinimalRequest() {
      DqeQueryRequest req = parser.parse("{\"query\": \"SELECT 1\"}");
      assertEquals("SELECT 1", req.getQuery());
      assertEquals("dqe", req.getEngine());
      assertEquals(0, req.getFetchSize());
      assertNotNull(req.getQueryId());
      assertTrue(req.getSessionProperties().isEmpty());
    }

    @Test
    @DisplayName("parses request with all fields")
    void parsesFullRequest() {
      String body =
          "{\"query\": \"SELECT * FROM t\", \"engine\": \"calcite\", \"fetch_size\": 100}";
      DqeQueryRequest req = parser.parse(body);
      assertEquals("SELECT * FROM t", req.getQuery());
      assertEquals("calcite", req.getEngine());
      assertEquals(100, req.getFetchSize());
    }

    @Test
    @DisplayName("trims whitespace from query")
    void trimsQuery() {
      DqeQueryRequest req = parser.parse("{\"query\": \"  SELECT 1  \"}");
      assertEquals("SELECT 1", req.getQuery());
    }

    @Test
    @DisplayName("ignores parameters and cursor fields")
    void ignoresParametersAndCursor() {
      String body =
          "{\"query\": \"SELECT 1\", \"parameters\": [], \"cursor\": \"abc\"}";
      DqeQueryRequest req = parser.parse(body);
      assertEquals("SELECT 1", req.getQuery());
    }
  }

  @Nested
  @DisplayName("Validation errors")
  class ValidationErrors {

    @Test
    @DisplayName("rejects null body")
    void rejectsNullBody() {
      DqeException ex = assertThrows(DqeException.class, () -> parser.parse(null));
      assertEquals(DqeErrorCode.INVALID_REQUEST, ex.getErrorCode());
    }

    @Test
    @DisplayName("rejects empty body")
    void rejectsEmptyBody() {
      DqeException ex = assertThrows(DqeException.class, () -> parser.parse(""));
      assertEquals(DqeErrorCode.INVALID_REQUEST, ex.getErrorCode());
    }

    @Test
    @DisplayName("rejects blank body")
    void rejectsBlankBody() {
      DqeException ex = assertThrows(DqeException.class, () -> parser.parse("   "));
      assertEquals(DqeErrorCode.INVALID_REQUEST, ex.getErrorCode());
    }

    @Test
    @DisplayName("rejects invalid JSON")
    void rejectsInvalidJson() {
      DqeException ex =
          assertThrows(DqeException.class, () -> parser.parse("{not valid json}"));
      assertEquals(DqeErrorCode.INVALID_REQUEST, ex.getErrorCode());
      assertTrue(ex.getMessage().contains("Invalid JSON"));
    }

    @Test
    @DisplayName("rejects missing query field")
    void rejectsMissingQuery() {
      DqeException ex =
          assertThrows(DqeException.class, () -> parser.parse("{\"engine\": \"dqe\"}"));
      assertEquals(DqeErrorCode.INVALID_REQUEST, ex.getErrorCode());
    }

    @Test
    @DisplayName("rejects empty query")
    void rejectsEmptyQuery() {
      DqeException ex =
          assertThrows(DqeException.class, () -> parser.parse("{\"query\": \"\"}"));
      assertEquals(DqeErrorCode.INVALID_REQUEST, ex.getErrorCode());
    }

    @Test
    @DisplayName("rejects query exceeding max length")
    void rejectsOverlongQuery() {
      String longQuery = "S".repeat(DqeRequestParser.MAX_QUERY_LENGTH + 1);
      DqeException ex =
          assertThrows(
              DqeException.class,
              () -> parser.parse("{\"query\": \"" + longQuery + "\"}"));
      assertEquals(DqeErrorCode.INVALID_REQUEST, ex.getErrorCode());
      assertTrue(ex.getMessage().contains("exceeds maximum"));
    }

    @Test
    @DisplayName("rejects unsupported field")
    void rejectsUnsupportedField() {
      DqeException ex =
          assertThrows(
              DqeException.class,
              () -> parser.parse("{\"query\": \"SELECT 1\", \"unknown_field\": 1}"));
      assertEquals(DqeErrorCode.INVALID_REQUEST, ex.getErrorCode());
      assertTrue(ex.getMessage().contains("Unsupported field"));
    }

    @Test
    @DisplayName("rejects negative fetch_size")
    void rejectsNegativeFetchSize() {
      DqeException ex =
          assertThrows(
              DqeException.class,
              () -> parser.parse("{\"query\": \"SELECT 1\", \"fetch_size\": -1}"));
      assertEquals(DqeErrorCode.INVALID_REQUEST, ex.getErrorCode());
    }
  }

  @Nested
  @DisplayName("Session properties")
  class SessionProperties {

    @Test
    @DisplayName("parses valid session properties")
    void parsesSessionProperties() {
      String body =
          "{\"query\": \"SELECT 1\", \"session_properties\": "
              + "{\"query_max_memory\": \"256mb\", \"query_timeout\": \"30s\"}}";
      DqeQueryRequest req = parser.parse(body);
      assertEquals("256mb", req.getSessionProperties().get("query_max_memory"));
      assertEquals("30s", req.getSessionProperties().get("query_timeout"));
    }

    @Test
    @DisplayName("rejects unknown session property")
    void rejectsUnknownSessionProperty() {
      String body =
          "{\"query\": \"SELECT 1\", \"session_properties\": "
              + "{\"invalid_prop\": \"value\"}}";
      DqeException ex = assertThrows(DqeException.class, () -> parser.parse(body));
      assertEquals(DqeErrorCode.INVALID_REQUEST, ex.getErrorCode());
      assertTrue(ex.getMessage().contains("Unknown session property"));
    }

    @Test
    @DisplayName("parses query_max_memory with mb suffix")
    void parsesMemoryMb() {
      String body =
          "{\"query\": \"SELECT 1\", \"session_properties\": "
              + "{\"query_max_memory\": \"256mb\"}}";
      DqeQueryRequest req = parser.parse(body);
      assertTrue(req.getQueryMaxMemoryBytes().isPresent());
      assertEquals(256L * 1024 * 1024, req.getQueryMaxMemoryBytes().get());
    }

    @Test
    @DisplayName("parses query_max_memory with gb suffix")
    void parsesMemoryGb() {
      String body =
          "{\"query\": \"SELECT 1\", \"session_properties\": "
              + "{\"query_max_memory\": \"1gb\"}}";
      DqeQueryRequest req = parser.parse(body);
      assertEquals(1024L * 1024 * 1024, req.getQueryMaxMemoryBytes().get());
    }

    @Test
    @DisplayName("parses query_max_memory with kb suffix")
    void parsesMemoryKb() {
      String body =
          "{\"query\": \"SELECT 1\", \"session_properties\": "
              + "{\"query_max_memory\": \"512kb\"}}";
      DqeQueryRequest req = parser.parse(body);
      assertEquals(512L * 1024, req.getQueryMaxMemoryBytes().get());
    }

    @Test
    @DisplayName("parses query_max_memory as plain bytes")
    void parsesMemoryBytes() {
      String body =
          "{\"query\": \"SELECT 1\", \"session_properties\": "
              + "{\"query_max_memory\": \"1048576\"}}";
      DqeQueryRequest req = parser.parse(body);
      assertEquals(1048576L, req.getQueryMaxMemoryBytes().get());
    }

    @Test
    @DisplayName("parses query_timeout with seconds suffix")
    void parsesTimeoutSeconds() {
      String body =
          "{\"query\": \"SELECT 1\", \"session_properties\": "
              + "{\"query_timeout\": \"30s\"}}";
      DqeQueryRequest req = parser.parse(body);
      assertTrue(req.getQueryTimeout().isPresent());
      assertEquals(Duration.ofSeconds(30), req.getQueryTimeout().get());
    }

    @Test
    @DisplayName("parses query_timeout with minutes suffix")
    void parsesTimeoutMinutes() {
      String body =
          "{\"query\": \"SELECT 1\", \"session_properties\": "
              + "{\"query_timeout\": \"5m\"}}";
      DqeQueryRequest req = parser.parse(body);
      assertEquals(Duration.ofMinutes(5), req.getQueryTimeout().get());
    }

    @Test
    @DisplayName("parses query_timeout with ms suffix")
    void parsesTimeoutMs() {
      String body =
          "{\"query\": \"SELECT 1\", \"session_properties\": "
              + "{\"query_timeout\": \"500ms\"}}";
      DqeQueryRequest req = parser.parse(body);
      assertEquals(Duration.ofMillis(500), req.getQueryTimeout().get());
    }

    @Test
    @DisplayName("rejects invalid memory value")
    void rejectsInvalidMemory() {
      String body =
          "{\"query\": \"SELECT 1\", \"session_properties\": "
              + "{\"query_max_memory\": \"abc\"}}";
      DqeException ex = assertThrows(DqeException.class, () -> parser.parse(body));
      assertEquals(DqeErrorCode.INVALID_REQUEST, ex.getErrorCode());
    }

    @Test
    @DisplayName("rejects invalid timeout value")
    void rejectsInvalidTimeout() {
      String body =
          "{\"query\": \"SELECT 1\", \"session_properties\": "
              + "{\"query_timeout\": \"abc\"}}";
      DqeException ex = assertThrows(DqeException.class, () -> parser.parse(body));
      assertEquals(DqeErrorCode.INVALID_REQUEST, ex.getErrorCode());
    }
  }
}
