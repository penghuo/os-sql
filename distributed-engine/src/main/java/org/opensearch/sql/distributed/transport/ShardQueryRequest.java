/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.transport;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

/**
 * Request for executing a plan fragment on a data node. Contains the serialized StageFragment (as
 * bytes) and the list of shard IDs to execute against.
 */
public class ShardQueryRequest extends ActionRequest {

  private final String queryId;
  private final int stageId;
  private final byte[] serializedFragment;
  private final List<Integer> shardIds;
  private final String indexName;

  public ShardQueryRequest(
      String queryId,
      int stageId,
      byte[] serializedFragment,
      List<Integer> shardIds,
      String indexName) {
    this.queryId = queryId;
    this.stageId = stageId;
    this.serializedFragment = serializedFragment;
    this.shardIds = List.copyOf(shardIds);
    this.indexName = indexName;
  }

  public ShardQueryRequest(StreamInput in) throws IOException {
    super(in);
    this.queryId = in.readString();
    this.stageId = in.readVInt();
    this.serializedFragment = in.readByteArray();
    int shardCount = in.readVInt();
    List<Integer> shards = new ArrayList<>(shardCount);
    for (int i = 0; i < shardCount; i++) {
      shards.add(in.readVInt());
    }
    this.shardIds = List.copyOf(shards);
    this.indexName = in.readString();
  }

  @Override
  public void writeTo(StreamOutput out) throws IOException {
    super.writeTo(out);
    out.writeString(queryId);
    out.writeVInt(stageId);
    out.writeByteArray(serializedFragment);
    out.writeVInt(shardIds.size());
    for (int shardId : shardIds) {
      out.writeVInt(shardId);
    }
    out.writeString(indexName);
  }

  @Override
  public ActionRequestValidationException validate() {
    if (queryId == null || queryId.isEmpty()) {
      ActionRequestValidationException ex = new ActionRequestValidationException();
      ex.addValidationError("queryId is required");
      return ex;
    }
    if (serializedFragment == null || serializedFragment.length == 0) {
      ActionRequestValidationException ex = new ActionRequestValidationException();
      ex.addValidationError("serializedFragment is required");
      return ex;
    }
    if (shardIds == null || shardIds.isEmpty()) {
      ActionRequestValidationException ex = new ActionRequestValidationException();
      ex.addValidationError("shardIds cannot be empty");
      return ex;
    }
    return null;
  }

  public String getQueryId() {
    return queryId;
  }

  public int getStageId() {
    return stageId;
  }

  public byte[] getSerializedFragment() {
    return serializedFragment;
  }

  public List<Integer> getShardIds() {
    return shardIds;
  }

  public String getIndexName() {
    return indexName;
  }
}
