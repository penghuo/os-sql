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

class DqeExchangeCloseRequestTests {

  @Test
  @DisplayName("serialization round-trip preserves all fields")
  void serializationRoundTrip() throws IOException {
    DqeExchangeCloseRequest original = new DqeExchangeCloseRequest("query-close", 1, 3);

    BytesStreamOutput out = new BytesStreamOutput();
    original.writeTo(out);

    StreamInput in = out.bytes().streamInput();
    DqeExchangeCloseRequest deserialized = new DqeExchangeCloseRequest(in);

    assertEquals("query-close", deserialized.getQueryId());
    assertEquals(1, deserialized.getStageId());
    assertEquals(3, deserialized.getPartitionId());
  }

  @Test
  @DisplayName("constructor rejects null queryId")
  void constructorRejectsNullQueryId() {
    assertThrows(NullPointerException.class, () -> new DqeExchangeCloseRequest(null, 0, 0));
  }
}
