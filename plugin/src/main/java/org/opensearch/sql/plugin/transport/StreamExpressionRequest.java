/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport;

import java.io.IOException;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;

public class StreamExpressionRequest extends ActionRequest {
  private String streamExpression;

  public StreamExpressionRequest() {
    super();
  }

  public StreamExpressionRequest(String streamExpression) {
    super();
    this.streamExpression = streamExpression;
  }

  public StreamExpressionRequest(StreamInput in) throws IOException {
    super(in);
    streamExpression = in.readString();
  }

  public String getStreamExpression() {
    return streamExpression;
  }

  @Override
  public void writeTo(StreamOutput out) throws IOException {
    super.writeTo(out);
    out.writeString(streamExpression);
  }

  @Override
  public ActionRequestValidationException validate() {
    return null;
  }
}
