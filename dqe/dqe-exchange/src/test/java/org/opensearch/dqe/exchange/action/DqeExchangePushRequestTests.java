/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.exchange.action;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;

class DqeExchangePushRequestTests {

  @Test
  @DisplayName("serialization round-trip preserves all fields")
  void serializationRoundTrip() throws IOException {
    byte[] pages = new byte[] {1, 2, 3, 4, 5};
    DqeExchangePushRequest original =
        new DqeExchangePushRequest("query-123", 1, 0, 42L, pages, false, 1024L, 3);

    BytesStreamOutput out = new BytesStreamOutput();
    original.writeTo(out);

    StreamInput in = out.bytes().streamInput();
    DqeExchangePushRequest deserialized = new DqeExchangePushRequest(in);

    assertEquals("query-123", deserialized.getQueryId());
    assertEquals(1, deserialized.getStageId());
    assertEquals(0, deserialized.getPartitionId());
    assertEquals(42L, deserialized.getSequenceNumber());
    assertArrayEquals(pages, deserialized.getSerializedPages());
    assertFalse(deserialized.isLast());
    assertEquals(1024L, deserialized.getUncompressedBytes());
    assertEquals(3, deserialized.getPageCount());
  }

  @Test
  @DisplayName("serialization round-trip with isLast=true")
  void serializationRoundTripWithIsLast() throws IOException {
    DqeExchangePushRequest original =
        new DqeExchangePushRequest("query-456", 2, 1, 100L, new byte[0], true, 0L, 0);

    BytesStreamOutput out = new BytesStreamOutput();
    original.writeTo(out);

    StreamInput in = out.bytes().streamInput();
    DqeExchangePushRequest deserialized = new DqeExchangePushRequest(in);

    assertTrue(deserialized.isLast());
    assertEquals(0, deserialized.getSerializedPages().length);
  }

  @Test
  @DisplayName("serialization with large payload")
  void serializationWithLargePayload() throws IOException {
    byte[] largePages = new byte[1024 * 1024]; // 1MB
    for (int i = 0; i < largePages.length; i++) {
      largePages[i] = (byte) (i % 256);
    }

    DqeExchangePushRequest original =
        new DqeExchangePushRequest(
            "query-large", 0, 0, 1L, largePages, false, 2L * 1024 * 1024, 10);

    BytesStreamOutput out = new BytesStreamOutput();
    original.writeTo(out);

    StreamInput in = out.bytes().streamInput();
    DqeExchangePushRequest deserialized = new DqeExchangePushRequest(in);

    assertArrayEquals(largePages, deserialized.getSerializedPages());
  }

  @Test
  @DisplayName("constructor rejects null queryId")
  void constructorRejectsNullQueryId() {
    assertThrows(
        NullPointerException.class,
        () -> new DqeExchangePushRequest(null, 0, 0, 0L, new byte[0], false, 0L, 0));
  }

  @Test
  @DisplayName("constructor rejects null serializedPages")
  void constructorRejectsNullSerializedPages() {
    assertThrows(
        NullPointerException.class,
        () -> new DqeExchangePushRequest("q", 0, 0, 0L, null, false, 0L, 0));
  }
}
