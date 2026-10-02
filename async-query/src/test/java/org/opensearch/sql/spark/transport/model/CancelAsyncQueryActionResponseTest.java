/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.transport.model;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import org.junit.jupiter.api.Test;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.BytesStreamInput;

public class CancelAsyncQueryActionResponseTest {

  @Test
  public void streamRoundTrip_preservesStatusAcknowledgement() throws IOException {
    String acknowledgement = "{\"status\":\"CANCELLED\"}";
    BytesStreamOutput out = new BytesStreamOutput();
    new CancelAsyncQueryActionResponse(acknowledgement).writeTo(out);
    out.flush();

    try (BytesStreamInput in = new BytesStreamInput(out.bytes().toBytesRef().bytes)) {
      assertEquals(acknowledgement, new CancelAsyncQueryActionResponse(in).getResult());
    }
  }
}
