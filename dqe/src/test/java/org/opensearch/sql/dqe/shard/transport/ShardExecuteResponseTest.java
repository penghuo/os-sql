/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.shard.transport;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.InputStreamStreamInput;

@DisplayName("ShardExecuteResponse serialization round-trip")
class ShardExecuteResponseTest {

  @Test
  @DisplayName("Response preserves resultJson through serialization round-trip")
  void responseRoundTrip() throws IOException {
    String json = "[{\"status\":200,\"message\":\"ok\"}]";
    ShardExecuteResponse original = new ShardExecuteResponse(json);

    BytesStreamOutput out = new BytesStreamOutput();
    original.writeTo(out);

    ShardExecuteResponse deserialized =
        new ShardExecuteResponse(
            new InputStreamStreamInput(new ByteArrayInputStream(out.bytes().toBytesRef().bytes)));

    assertEquals(json, deserialized.getResultJson());
  }

  @Test
  @DisplayName("Response round-trips with empty JSON array")
  void responseRoundTripEmptyArray() throws IOException {
    String json = "[]";
    ShardExecuteResponse original = new ShardExecuteResponse(json);

    BytesStreamOutput out = new BytesStreamOutput();
    original.writeTo(out);

    ShardExecuteResponse deserialized =
        new ShardExecuteResponse(
            new InputStreamStreamInput(new ByteArrayInputStream(out.bytes().toBytesRef().bytes)));

    assertEquals(json, deserialized.getResultJson());
  }

  @Test
  @DisplayName("Response round-trips with multi-row JSON")
  void responseRoundTripMultiRow() throws IOException {
    String json = "[{\"a\":1,\"b\":2},{\"a\":3,\"b\":4},{\"a\":5,\"b\":6}]";
    ShardExecuteResponse original = new ShardExecuteResponse(json);

    BytesStreamOutput out = new BytesStreamOutput();
    original.writeTo(out);

    ShardExecuteResponse deserialized =
        new ShardExecuteResponse(
            new InputStreamStreamInput(new ByteArrayInputStream(out.bytes().toBytesRef().bytes)));

    assertEquals(json, deserialized.getResultJson());
  }

  @Test
  @DisplayName("Response round-trips with special characters in JSON")
  void responseRoundTripSpecialChars() throws IOException {
    String json = "[{\"msg\":\"hello\\nworld\",\"path\":\"a\\\\b\"}]";
    ShardExecuteResponse original = new ShardExecuteResponse(json);

    BytesStreamOutput out = new BytesStreamOutput();
    original.writeTo(out);

    ShardExecuteResponse deserialized =
        new ShardExecuteResponse(
            new InputStreamStreamInput(new ByteArrayInputStream(out.bytes().toBytesRef().bytes)));

    assertEquals(json, deserialized.getResultJson());
  }
}
