/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.exchange.action;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;

class DqeExchangePushResponseTests {

  @Test
  @DisplayName("serialization round-trip with accepted=true")
  void serializationRoundTripAccepted() throws IOException {
    DqeExchangePushResponse original = new DqeExchangePushResponse(true, 16 * 1024 * 1024L);

    BytesStreamOutput out = new BytesStreamOutput();
    original.writeTo(out);

    StreamInput in = out.bytes().streamInput();
    DqeExchangePushResponse deserialized = new DqeExchangePushResponse(in);

    assertTrue(deserialized.isAccepted());
    assertEquals(16 * 1024 * 1024L, deserialized.getBufferRemainingBytes());
  }

  @Test
  @DisplayName("serialization round-trip with accepted=false")
  void serializationRoundTripRejected() throws IOException {
    DqeExchangePushResponse original = new DqeExchangePushResponse(false, 0L);

    BytesStreamOutput out = new BytesStreamOutput();
    original.writeTo(out);

    StreamInput in = out.bytes().streamInput();
    DqeExchangePushResponse deserialized = new DqeExchangePushResponse(in);

    assertFalse(deserialized.isAccepted());
    assertEquals(0L, deserialized.getBufferRemainingBytes());
  }
}
