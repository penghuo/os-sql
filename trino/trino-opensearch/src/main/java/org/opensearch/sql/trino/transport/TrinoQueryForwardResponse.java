/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.trino.transport;

import java.io.IOException;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

/** Response containing the JSON result of a forwarded query. */
public class TrinoQueryForwardResponse extends ActionResponse {

  private final String resultJson;

  public TrinoQueryForwardResponse(String resultJson) {
    this.resultJson = resultJson;
  }

  public TrinoQueryForwardResponse(StreamInput in) throws IOException {
    super(in);
    this.resultJson = in.readString();
  }

  @Override
  public void writeTo(StreamOutput out) throws IOException {
    out.writeString(resultJson);
  }

  public String getResultJson() {
    return resultJson;
  }
}
