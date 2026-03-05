/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.coordinator.transport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.InputStreamStreamInput;

@DisplayName("TrinoSqlRequest serialization round-trip")
class TrinoSqlRequestTest {

  @Test
  @DisplayName("Request preserves query and explain=false through round-trip")
  void requestRoundTripQuery() throws IOException {
    TrinoSqlRequest original = new TrinoSqlRequest("SELECT * FROM logs", false);

    BytesStreamOutput out = new BytesStreamOutput();
    original.writeTo(out);

    TrinoSqlRequest deserialized =
        new TrinoSqlRequest(
            new InputStreamStreamInput(new ByteArrayInputStream(out.bytes().toBytesRef().bytes)));

    assertEquals("SELECT * FROM logs", deserialized.getQuery());
    assertFalse(deserialized.isExplain());
  }

  @Test
  @DisplayName("Request preserves query and explain=true through round-trip")
  void requestRoundTripExplain() throws IOException {
    TrinoSqlRequest original =
        new TrinoSqlRequest("SELECT category, COUNT(*) FROM logs GROUP BY category", true);

    BytesStreamOutput out = new BytesStreamOutput();
    original.writeTo(out);

    TrinoSqlRequest deserialized =
        new TrinoSqlRequest(
            new InputStreamStreamInput(new ByteArrayInputStream(out.bytes().toBytesRef().bytes)));

    assertEquals("SELECT category, COUNT(*) FROM logs GROUP BY category", deserialized.getQuery());
    assertTrue(deserialized.isExplain());
  }

  @Test
  @DisplayName("Request round-trips with empty query string")
  void requestRoundTripEmptyQuery() throws IOException {
    TrinoSqlRequest original = new TrinoSqlRequest("", false);

    BytesStreamOutput out = new BytesStreamOutput();
    original.writeTo(out);

    TrinoSqlRequest deserialized =
        new TrinoSqlRequest(
            new InputStreamStreamInput(new ByteArrayInputStream(out.bytes().toBytesRef().bytes)));

    assertEquals("", deserialized.getQuery());
    assertFalse(deserialized.isExplain());
  }

  @Test
  @DisplayName("Request round-trips with special characters in query")
  void requestRoundTripSpecialChars() throws IOException {
    String query = "SELECT * FROM logs WHERE msg = 'hello\\nworld'";
    TrinoSqlRequest original = new TrinoSqlRequest(query, false);

    BytesStreamOutput out = new BytesStreamOutput();
    original.writeTo(out);

    TrinoSqlRequest deserialized =
        new TrinoSqlRequest(
            new InputStreamStreamInput(new ByteArrayInputStream(out.bytes().toBytesRef().bytes)));

    assertEquals(query, deserialized.getQuery());
  }

  @Test
  @DisplayName("Validate returns null (no validation errors)")
  void validateReturnsNull() {
    TrinoSqlRequest request = new TrinoSqlRequest("SELECT 1", false);
    assertNull(request.validate());
  }

  @Test
  @DisplayName("fromActionRequest returns same instance when already TrinoSqlRequest")
  void fromActionRequestSameInstance() {
    TrinoSqlRequest original = new TrinoSqlRequest("SELECT 1", true);
    TrinoSqlRequest result = TrinoSqlRequest.fromActionRequest(original);
    assertTrue(original == result, "fromActionRequest should return same instance");
  }
}
