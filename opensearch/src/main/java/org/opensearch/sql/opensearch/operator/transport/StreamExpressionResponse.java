/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.operator.transport;

import java.io.IOException;
import org.opensearch.action.ActionResponse;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;

public class StreamExpressionResponse extends ActionResponse{
  private String result;

  public StreamExpressionResponse() {
    super();
  }

  public StreamExpressionResponse(String result) {
    super();
    this.result = result;
  }

  public StreamExpressionResponse(StreamInput in) throws IOException {
    super(in);
    this.result = in.readString();
  }

  @Override
  public void writeTo(StreamOutput out) throws IOException {
    out.writeString(this.result);
  }

  public String getResult() {
    return result;
  }
}
