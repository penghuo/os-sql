/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.operator.transport;

import java.io.IOException;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;

public class StreamExpressionRequest extends ActionRequest {
  private ShardRouting shardRouting;

  private String streamExpression;

  public StreamExpressionRequest() {
    super();
  }

  public StreamExpressionRequest(ShardRouting shardRouting, String streamExpression) {
    super();
    this.shardRouting = shardRouting;
    this.streamExpression = streamExpression;
  }

  public StreamExpressionRequest(StreamInput in) throws IOException {
    super(in);
    shardRouting = new ShardRouting(in);
    streamExpression = in.readString();
  }

  public String getStreamExpression() {
    return streamExpression;
  }

  public ShardRouting getShardRouting() {
    return shardRouting;
  }

  @Override
  public void writeTo(StreamOutput out) throws IOException {
    super.writeTo(out);
    shardRouting.writeTo(out);
    out.writeString(streamExpression);
  }

  @Override
  public ActionRequestValidationException validate() {
    return null;
  }
}
