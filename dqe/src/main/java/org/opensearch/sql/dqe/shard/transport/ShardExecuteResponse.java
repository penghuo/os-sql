/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.shard.transport;

import java.io.IOException;
import lombok.Getter;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

/**
 * Transport response carrying the result of a shard-level DQE plan fragment execution. The result
 * is encoded as a JSON string (an array of row objects) for simplicity in the MVP. The coordinator
 * deserializes this JSON to reconstruct the result rows.
 */
@Getter
public class ShardExecuteResponse extends ActionResponse {

  /** JSON-encoded result rows. Each element is a JSON object mapping column names to values. */
  private final String resultJson;

  public ShardExecuteResponse(String resultJson) {
    this.resultJson = resultJson;
  }

  /** Deserialize from a stream. */
  public ShardExecuteResponse(StreamInput in) throws IOException {
    super(in);
    this.resultJson = in.readString();
  }

  @Override
  public void writeTo(StreamOutput out) throws IOException {
    out.writeString(resultJson);
  }
}
