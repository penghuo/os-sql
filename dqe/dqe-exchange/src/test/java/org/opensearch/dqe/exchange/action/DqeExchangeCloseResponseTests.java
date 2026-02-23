/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.exchange.action;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;

class DqeExchangeCloseResponseTests {

  @Test
  @DisplayName("serialization round-trip preserves acknowledged field")
  void serializationRoundTrip() throws IOException {
    DqeExchangeCloseResponse original = new DqeExchangeCloseResponse(true);

    BytesStreamOutput out = new BytesStreamOutput();
    original.writeTo(out);

    StreamInput in = out.bytes().streamInput();
    DqeExchangeCloseResponse deserialized = new DqeExchangeCloseResponse(in);

    assertTrue(deserialized.isAcknowledged());
  }
}
