/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.coordinator.transport;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.InputStreamStreamInput;

@DisplayName("TrinoSqlResponse serialization round-trip")
class TrinoSqlResponseTest {

  @Test
  @DisplayName("Response preserves result and default contentType through round-trip")
  void responseRoundTripDefaultContentType() throws IOException {
    TrinoSqlResponse original = new TrinoSqlResponse("{\"status\":200}");

    BytesStreamOutput out = new BytesStreamOutput();
    original.writeTo(out);

    TrinoSqlResponse deserialized =
        new TrinoSqlResponse(
            new InputStreamStreamInput(new ByteArrayInputStream(out.bytes().toBytesRef().bytes)));

    assertEquals("{\"status\":200}", deserialized.getResult());
    assertEquals("application/json; charset=UTF-8", deserialized.getContentType());
  }

  @Test
  @DisplayName("Response preserves result and custom contentType through round-trip")
  void responseRoundTripCustomContentType() throws IOException {
    TrinoSqlResponse original = new TrinoSqlResponse("col1,col2\n1,2", "text/csv; charset=UTF-8");

    BytesStreamOutput out = new BytesStreamOutput();
    original.writeTo(out);

    TrinoSqlResponse deserialized =
        new TrinoSqlResponse(
            new InputStreamStreamInput(new ByteArrayInputStream(out.bytes().toBytesRef().bytes)));

    assertEquals("col1,col2\n1,2", deserialized.getResult());
    assertEquals("text/csv; charset=UTF-8", deserialized.getContentType());
  }

  @Test
  @DisplayName("Response round-trips with empty result")
  void responseRoundTripEmptyResult() throws IOException {
    TrinoSqlResponse original = new TrinoSqlResponse("");

    BytesStreamOutput out = new BytesStreamOutput();
    original.writeTo(out);

    TrinoSqlResponse deserialized =
        new TrinoSqlResponse(
            new InputStreamStreamInput(new ByteArrayInputStream(out.bytes().toBytesRef().bytes)));

    assertEquals("", deserialized.getResult());
    assertEquals("application/json; charset=UTF-8", deserialized.getContentType());
  }

  @Test
  @DisplayName("Response round-trips with large JSON result")
  void responseRoundTripLargeResult() throws IOException {
    StringBuilder sb = new StringBuilder("{\"datarows\":[");
    for (int i = 0; i < 1000; i++) {
      if (i > 0) {
        sb.append(",");
      }
      sb.append("[").append(i).append(",\"row_").append(i).append("\"]");
    }
    sb.append("]}");
    String largeJson = sb.toString();

    TrinoSqlResponse original = new TrinoSqlResponse(largeJson);

    BytesStreamOutput out = new BytesStreamOutput();
    original.writeTo(out);

    TrinoSqlResponse deserialized =
        new TrinoSqlResponse(
            new InputStreamStreamInput(new ByteArrayInputStream(out.bytes().toBytesRef().bytes)));

    assertEquals(largeJson, deserialized.getResult());
  }

  @Test
  @DisplayName("Response round-trips with special characters")
  void responseRoundTripSpecialChars() throws IOException {
    String json = "{\"msg\":\"hello\\nworld\",\"path\":\"a\\\\b\"}";
    TrinoSqlResponse original = new TrinoSqlResponse(json);

    BytesStreamOutput out = new BytesStreamOutput();
    original.writeTo(out);

    TrinoSqlResponse deserialized =
        new TrinoSqlResponse(
            new InputStreamStreamInput(new ByteArrayInputStream(out.bytes().toBytesRef().bytes)));

    assertEquals(json, deserialized.getResult());
  }
}
