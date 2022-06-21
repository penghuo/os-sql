/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.s3.transport;

import java.io.IOException;
import org.opensearch.action.ActionResponse;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;

public class CreateViewResponse extends ActionResponse {

  private boolean status;

  public CreateViewResponse() {
    super();
  }

  public CreateViewResponse(boolean status) {
    super();
    this.status = status;
  }

  public CreateViewResponse(StreamInput in) throws IOException {
    super(in);
    status = in.readBoolean();
  }

  @Override
  public void writeTo(StreamOutput out) throws IOException {
    out.writeBoolean(status);
  }

  public boolean isStatus() {
    return status;
  }
}
