/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.shard.transport;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.InputStreamStreamInput;

@DisplayName("ShardExecuteRequest serialization round-trip")
class ShardExecuteRequestTest {

  @Test
  @DisplayName("Request preserves all fields through serialization round-trip")
  void requestRoundTrip() throws IOException {
    byte[] fragment = new byte[] {1, 2, 3, 4, 5};
    ShardExecuteRequest original = new ShardExecuteRequest(fragment, "logs", 2, 30000L);

    BytesStreamOutput out = new BytesStreamOutput();
    original.writeTo(out);

    ShardExecuteRequest deserialized =
        new ShardExecuteRequest(
            new InputStreamStreamInput(new ByteArrayInputStream(out.bytes().toBytesRef().bytes)));

    assertArrayEquals(fragment, deserialized.getSerializedFragment());
    assertEquals("logs", deserialized.getIndexName());
    assertEquals(2, deserialized.getShardId());
    assertEquals(30000L, deserialized.getTimeoutMillis());
  }

  @Test
  @DisplayName("Request round-trips with empty fragment")
  void requestRoundTripEmptyFragment() throws IOException {
    byte[] fragment = new byte[0];
    ShardExecuteRequest original = new ShardExecuteRequest(fragment, "events", 0, 5000L);

    BytesStreamOutput out = new BytesStreamOutput();
    original.writeTo(out);

    ShardExecuteRequest deserialized =
        new ShardExecuteRequest(
            new InputStreamStreamInput(new ByteArrayInputStream(out.bytes().toBytesRef().bytes)));

    assertArrayEquals(fragment, deserialized.getSerializedFragment());
    assertEquals("events", deserialized.getIndexName());
    assertEquals(0, deserialized.getShardId());
    assertEquals(5000L, deserialized.getTimeoutMillis());
  }

  @Test
  @DisplayName("Request round-trips with large fragment")
  void requestRoundTripLargeFragment() throws IOException {
    byte[] fragment = new byte[10000];
    for (int i = 0; i < fragment.length; i++) {
      fragment[i] = (byte) (i % 256);
    }
    ShardExecuteRequest original = new ShardExecuteRequest(fragment, "big-index", 7, 120000L);

    BytesStreamOutput out = new BytesStreamOutput();
    original.writeTo(out);

    ShardExecuteRequest deserialized =
        new ShardExecuteRequest(
            new InputStreamStreamInput(new ByteArrayInputStream(out.bytes().toBytesRef().bytes)));

    assertArrayEquals(fragment, deserialized.getSerializedFragment());
    assertEquals("big-index", deserialized.getIndexName());
    assertEquals(7, deserialized.getShardId());
    assertEquals(120000L, deserialized.getTimeoutMillis());
  }

  @Test
  @DisplayName("Validate returns null (no validation errors)")
  void validateReturnsNull() {
    ShardExecuteRequest request = new ShardExecuteRequest(new byte[] {1}, "index", 0, 1000L);
    assertNull(request.validate());
  }
}
