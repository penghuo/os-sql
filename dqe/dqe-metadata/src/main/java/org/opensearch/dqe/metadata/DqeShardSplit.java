/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.metadata;

import java.io.IOException;
import java.util.Objects;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;

/**
 * Immutable split representing one shard copy selected for query execution. Contains the logical
 * shard ID (deduplication key), the target node ID, the index name, and whether the selected copy
 * is the primary.
 *
 * <p>For an index with N primary shards, exactly N DqeShardSplits are produced. Each targets either
 * the primary or one replica — never both. The logical shard ID is unique across all splits for a
 * given table handle.
 */
public final class DqeShardSplit implements Writeable {

  private final int shardId;
  private final String nodeId;
  private final String indexName;
  private final boolean primary;

  public DqeShardSplit(int shardId, String nodeId, String indexName, boolean primary) {
    this.shardId = shardId;
    this.nodeId = Objects.requireNonNull(nodeId, "nodeId must not be null");
    this.indexName = Objects.requireNonNull(indexName, "indexName must not be null");
    this.primary = primary;
  }

  /** Deserializes from a stream. */
  public DqeShardSplit(StreamInput in) throws IOException {
    this.shardId = in.readInt();
    this.nodeId = in.readString();
    this.indexName = in.readString();
    this.primary = in.readBoolean();
  }

  @Override
  public void writeTo(StreamOutput out) throws IOException {
    out.writeInt(shardId);
    out.writeString(nodeId);
    out.writeString(indexName);
    out.writeBoolean(primary);
  }

  /** Returns the logical shard ID (0-based). This is the deduplication key. */
  public int getShardId() {
    return shardId;
  }

  /** Returns the node ID where this shard copy resides. */
  public String getNodeId() {
    return nodeId;
  }

  /** Returns the index name this shard belongs to. */
  public String getIndexName() {
    return indexName;
  }

  /** Returns true if the selected copy is the primary shard. */
  public boolean isPrimary() {
    return primary;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof DqeShardSplit other)) {
      return false;
    }
    return shardId == other.shardId && indexName.equals(other.indexName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(shardId, indexName);
  }

  @Override
  public String toString() {
    return "DqeShardSplit{"
        + indexName
        + "["
        + shardId
        + "]"
        + (primary ? " primary" : " replica")
        + " on "
        + nodeId
        + "}";
  }
}
