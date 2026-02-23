/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.exchange.action;

import java.io.IOException;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

/**
 * Transport response for a push exchange request.
 *
 * <p>Indicates whether the exchange buffer accepted the chunk and reports remaining buffer capacity
 * to support backpressure decisions.
 */
public class DqeExchangePushResponse extends ActionResponse {

  private final boolean accepted;
  private final long bufferRemainingBytes;

  /**
   * Construct a push response.
   *
   * @param accepted true if the chunk was accepted into the buffer
   * @param bufferRemainingBytes remaining capacity in the exchange buffer (bytes)
   */
  public DqeExchangePushResponse(boolean accepted, long bufferRemainingBytes) {
    this.accepted = accepted;
    this.bufferRemainingBytes = bufferRemainingBytes;
  }

  /** Deserialization constructor. */
  public DqeExchangePushResponse(StreamInput in) throws IOException {
    super(in);
    this.accepted = in.readBoolean();
    this.bufferRemainingBytes = in.readVLong();
  }

  @Override
  public void writeTo(StreamOutput out) throws IOException {
    out.writeBoolean(accepted);
    out.writeVLong(bufferRemainingBytes);
  }

  public boolean isAccepted() {
    return accepted;
  }

  public long getBufferRemainingBytes() {
    return bufferRemainingBytes;
  }
}
