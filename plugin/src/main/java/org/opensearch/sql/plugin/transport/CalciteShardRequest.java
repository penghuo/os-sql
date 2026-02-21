/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport;

import java.io.IOException;
import lombok.Getter;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

/** Transport request carrying a serialized Calcite shard plan for execution on a target shard. */
@Getter
public class CalciteShardRequest extends ActionRequest {

  private final String planJson;
  private final String indexName;
  private final int shardId;

  public CalciteShardRequest(String planJson, String indexName, int shardId) {
    this.planJson = planJson;
    this.indexName = indexName;
    this.shardId = shardId;
  }

  public CalciteShardRequest(StreamInput in) throws IOException {
    super(in);
    this.planJson = in.readString();
    this.indexName = in.readString();
    this.shardId = in.readVInt();
  }

  @Override
  public void writeTo(StreamOutput out) throws IOException {
    super.writeTo(out);
    out.writeString(planJson);
    out.writeString(indexName);
    out.writeVInt(shardId);
  }

  public ActionRequestValidationException validate() {
    return null;
  }
}
