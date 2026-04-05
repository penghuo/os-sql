/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.trino.transport;

import java.io.IOException;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

/** Empty response for ack — just confirms receipt. */
public class TrinoTaskResultsAckResponse extends ActionResponse {

  public TrinoTaskResultsAckResponse() {}

  public TrinoTaskResultsAckResponse(StreamInput in) throws IOException {
    super(in);
  }

  @Override
  public void writeTo(StreamOutput out) throws IOException {
    // No payload
  }
}
