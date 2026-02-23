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

class DqeStageCancelRequestTests {

  @Test
  @DisplayName("serialization round-trip preserves all fields")
  void serializationRoundTrip() throws IOException {
    DqeStageCancelRequest original = new DqeStageCancelRequest("query-cancel-test", 2);

    BytesStreamOutput out = new BytesStreamOutput();
    original.writeTo(out);

    StreamInput in = out.bytes().streamInput();
    DqeStageCancelRequest deserialized = new DqeStageCancelRequest(in);

    assertEquals("query-cancel-test", deserialized.getQueryId());
    assertEquals(2, deserialized.getStageId());
  }

  @Test
  @DisplayName("constructor rejects null queryId")
  void constructorRejectsNullQueryId() {
    assertThrows(NullPointerException.class, () -> new DqeStageCancelRequest(null, 0));
  }
}
