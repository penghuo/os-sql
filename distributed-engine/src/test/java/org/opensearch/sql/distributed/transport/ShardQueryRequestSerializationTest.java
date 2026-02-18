/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.transport;

import static org.junit.jupiter.api.Assertions.*;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.InputStreamStreamInput;

class ShardQueryRequestSerializationTest {

  @Test
  @DisplayName("ShardQueryRequest round-trip serialization")
  void testRoundTrip() throws IOException {
    byte[] fragment = "test-fragment-data".getBytes();
    ShardQueryRequest original =
        new ShardQueryRequest("query-123", 0, fragment, List.of(0, 1, 2), "test-index");

    // Serialize
    BytesStreamOutput out = new BytesStreamOutput();
    original.writeTo(out);

    // Deserialize
    InputStreamStreamInput in =
        new InputStreamStreamInput(new ByteArrayInputStream(out.bytes().toBytesRef().bytes));
    ShardQueryRequest deserialized = new ShardQueryRequest(in);

    assertEquals("query-123", deserialized.getQueryId());
    assertEquals(0, deserialized.getStageId());
    assertArrayEquals(fragment, deserialized.getSerializedFragment());
    assertEquals(List.of(0, 1, 2), deserialized.getShardIds());
    assertEquals("test-index", deserialized.getIndexName());
  }

  @Test
  @DisplayName("Validation rejects empty queryId")
  void testValidationEmptyQueryId() {
    ShardQueryRequest request = new ShardQueryRequest("", 0, new byte[] {1}, List.of(0), "idx");
    assertNotNull(request.validate());
  }

  @Test
  @DisplayName("Validation rejects empty serializedFragment")
  void testValidationEmptyFragment() {
    ShardQueryRequest request = new ShardQueryRequest("q1", 0, new byte[] {}, List.of(0), "idx");
    assertNotNull(request.validate());
  }

  @Test
  @DisplayName("Validation rejects empty shardIds")
  void testValidationEmptyShards() {
    ShardQueryRequest request = new ShardQueryRequest("q1", 0, new byte[] {1}, List.of(), "idx");
    assertNotNull(request.validate());
  }

  @Test
  @DisplayName("Valid request passes validation")
  void testValidRequest() {
    ShardQueryRequest request =
        new ShardQueryRequest("q1", 0, new byte[] {1}, List.of(0, 1), "idx");
    assertNull(request.validate());
  }
}
