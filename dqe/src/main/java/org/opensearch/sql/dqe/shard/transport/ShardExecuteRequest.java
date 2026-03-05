/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.shard.transport;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import lombok.Getter;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.core.common.io.stream.InputStreamStreamInput;
import org.opensearch.core.common.io.stream.OutputStreamStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

/**
 * Transport request that carries a serialized DQE plan fragment to be executed on a specific shard.
 * The plan fragment is produced by the coordinator via {@code DqePlanNode.writePlanNode()} and
 * deserialized on the shard via {@code DqePlanNode.readPlanNode()}.
 */
@Getter
public class ShardExecuteRequest extends ActionRequest {

  /** Serialized plan fragment bytes produced by {@code DqePlanNode.writePlanNode()}. */
  private final byte[] serializedFragment;

  /** Name of the index this fragment should scan. */
  private final String indexName;

  /** Shard ordinal within the index. */
  private final int shardId;

  /** Execution timeout in milliseconds. */
  private final long timeoutMillis;

  public ShardExecuteRequest(
      byte[] serializedFragment, String indexName, int shardId, long timeoutMillis) {
    this.serializedFragment = serializedFragment;
    this.indexName = indexName;
    this.shardId = shardId;
    this.timeoutMillis = timeoutMillis;
  }

  /** Deserialize from a stream. */
  public ShardExecuteRequest(StreamInput in) throws IOException {
    super(in);
    this.serializedFragment = in.readByteArray();
    this.indexName = in.readString();
    this.shardId = in.readInt();
    this.timeoutMillis = in.readLong();
  }

  @Override
  public void writeTo(StreamOutput out) throws IOException {
    super.writeTo(out);
    out.writeByteArray(serializedFragment);
    out.writeString(indexName);
    out.writeInt(shardId);
    out.writeLong(timeoutMillis);
  }

  @Override
  public ActionRequestValidationException validate() {
    return null;
  }

  /**
   * Re-create a {@link ShardExecuteRequest} from a generic {@link ActionRequest}. If the request is
   * already an instance of this class, it is returned directly. Otherwise, the request is
   * round-tripped through stream serialization.
   */
  public static ShardExecuteRequest fromActionRequest(ActionRequest actionRequest) {
    if (actionRequest instanceof ShardExecuteRequest) {
      return (ShardExecuteRequest) actionRequest;
    }
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
        OutputStreamStreamOutput osso = new OutputStreamStreamOutput(baos)) {
      actionRequest.writeTo(osso);
      try (InputStreamStreamInput input =
          new InputStreamStreamInput(new ByteArrayInputStream(baos.toByteArray()))) {
        return new ShardExecuteRequest(input);
      }
    } catch (IOException e) {
      throw new IllegalArgumentException(
          "failed to parse ActionRequest into ShardExecuteRequest", e);
    }
  }
}
