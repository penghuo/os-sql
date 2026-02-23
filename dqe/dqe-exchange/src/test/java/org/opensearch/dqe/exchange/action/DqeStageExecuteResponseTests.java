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

class DqeStageExecuteResponseTests {

  @Test
  @DisplayName("serialization round-trip with accepted=true")
  void serializationRoundTripAccepted() throws IOException {
    DqeStageExecuteResponse original = new DqeStageExecuteResponse("query-1", 0, true);

    BytesStreamOutput out = new BytesStreamOutput();
    original.writeTo(out);

    StreamInput in = out.bytes().streamInput();
    DqeStageExecuteResponse deserialized = new DqeStageExecuteResponse(in);

    assertEquals("query-1", deserialized.getQueryId());
    assertEquals(0, deserialized.getStageId());
    assertTrue(deserialized.isAccepted());
  }

  @Test
  @DisplayName("serialization round-trip with accepted=false")
  void serializationRoundTripRejected() throws IOException {
    DqeStageExecuteResponse original = new DqeStageExecuteResponse("query-2", 3, false);

    BytesStreamOutput out = new BytesStreamOutput();
    original.writeTo(out);

    StreamInput in = out.bytes().streamInput();
    DqeStageExecuteResponse deserialized = new DqeStageExecuteResponse(in);

    assertEquals("query-2", deserialized.getQueryId());
    assertEquals(3, deserialized.getStageId());
    assertFalse(deserialized.isAccepted());
  }

  @Test
  @DisplayName("constructor rejects null queryId")
  void constructorRejectsNullQueryId() {
    assertThrows(NullPointerException.class, () -> new DqeStageExecuteResponse(null, 0, true));
  }
}
