/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.exchange.action;

import java.io.IOException;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

/** Transport response acknowledging an exchange close request. */
public class DqeExchangeCloseResponse extends ActionResponse {

  private final boolean acknowledged;

  public DqeExchangeCloseResponse(boolean acknowledged) {
    this.acknowledged = acknowledged;
  }

  public DqeExchangeCloseResponse(StreamInput in) throws IOException {
    super(in);
    this.acknowledged = in.readBoolean();
  }

  @Override
  public void writeTo(StreamOutput out) throws IOException {
    out.writeBoolean(acknowledged);
  }

  public boolean isAcknowledged() {
    return acknowledged;
  }
}
