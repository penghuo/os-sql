/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.exchange.action;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;

class DqeExchangeAbortRequestTests {

  @Test
  @DisplayName("serialization round-trip preserves all fields")
  void serializationRoundTrip() throws IOException {
    DqeExchangeAbortRequest original =
        new DqeExchangeAbortRequest("query-abort", 2, 1, "node failure");

    BytesStreamOutput out = new BytesStreamOutput();
    original.writeTo(out);

    StreamInput in = out.bytes().streamInput();
    DqeExchangeAbortRequest deserialized = new DqeExchangeAbortRequest(in);

    assertEquals("query-abort", deserialized.getQueryId());
    assertEquals(2, deserialized.getStageId());
    assertEquals(1, deserialized.getPartitionId());
    assertEquals("node failure", deserialized.getReason());
  }

  @Test
  @DisplayName("constructor rejects null queryId")
  void constructorRejectsNullQueryId() {
    assertThrows(
        NullPointerException.class, () -> new DqeExchangeAbortRequest(null, 0, 0, "reason"));
  }

  @Test
  @DisplayName("constructor rejects null reason")
  void constructorRejectsNullReason() {
    assertThrows(NullPointerException.class, () -> new DqeExchangeAbortRequest("q", 0, 0, null));
  }
}
