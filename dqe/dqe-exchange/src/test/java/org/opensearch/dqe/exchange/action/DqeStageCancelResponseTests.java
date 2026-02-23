/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.exchange.action;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;

class DqeStageCancelResponseTests {

  @Test
  @DisplayName("serialization round-trip with acknowledged=true")
  void serializationRoundTripAcknowledged() throws IOException {
    DqeStageCancelResponse original = new DqeStageCancelResponse("q-1", 0, true);

    BytesStreamOutput out = new BytesStreamOutput();
    original.writeTo(out);

    StreamInput in = out.bytes().streamInput();
    DqeStageCancelResponse deserialized = new DqeStageCancelResponse(in);

    assertEquals("q-1", deserialized.getQueryId());
    assertEquals(0, deserialized.getStageId());
    assertTrue(deserialized.isAcknowledged());
  }

  @Test
  @DisplayName("serialization round-trip with acknowledged=false")
  void serializationRoundTripNotAcknowledged() throws IOException {
    DqeStageCancelResponse original = new DqeStageCancelResponse("q-2", 5, false);

    BytesStreamOutput out = new BytesStreamOutput();
    original.writeTo(out);

    StreamInput in = out.bytes().streamInput();
    DqeStageCancelResponse deserialized = new DqeStageCancelResponse(in);

    assertFalse(deserialized.isAcknowledged());
  }

  @Test
  @DisplayName("constructor rejects null queryId")
  void constructorRejectsNullQueryId() {
    assertThrows(NullPointerException.class, () -> new DqeStageCancelResponse(null, 0, true));
  }
}
