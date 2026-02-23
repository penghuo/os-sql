/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.exchange.action;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;

class DqeExchangeAbortResponseTests {

  @Test
  @DisplayName("serialization round-trip with acknowledged=true")
  void serializationRoundTripAcknowledged() throws IOException {
    DqeExchangeAbortResponse original = new DqeExchangeAbortResponse(true);

    BytesStreamOutput out = new BytesStreamOutput();
    original.writeTo(out);

    StreamInput in = out.bytes().streamInput();
    DqeExchangeAbortResponse deserialized = new DqeExchangeAbortResponse(in);

    assertTrue(deserialized.isAcknowledged());
  }

  @Test
  @DisplayName("serialization round-trip with acknowledged=false")
  void serializationRoundTripNotAcknowledged() throws IOException {
    DqeExchangeAbortResponse original = new DqeExchangeAbortResponse(false);

    BytesStreamOutput out = new BytesStreamOutput();
    original.writeTo(out);

    StreamInput in = out.bytes().streamInput();
    DqeExchangeAbortResponse deserialized = new DqeExchangeAbortResponse(in);

    assertFalse(deserialized.isAcknowledged());
  }
}
