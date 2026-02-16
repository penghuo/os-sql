/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.distributed;

import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.opensearch.sql.storage.split.Split;

/**
 * A split representing a single shard of an OpenSearch index. Used by the distributed execution
 * engine to assign work to specific shards on specific nodes.
 */
@Getter
@RequiredArgsConstructor
@EqualsAndHashCode
@ToString
public class ShardSplit implements Split {

  /** The name of the index this shard belongs to. */
  private final String indexName;

  /** The shard number within the index. */
  private final int shardId;

  /** The node ID where the primary copy of this shard resides. */
  private final String preferredNodeId;

  /** Node IDs holding replica copies of this shard. */
  private final List<String> replicaNodeIds;

  /** Whether this shard is located on the local (coordinator) node. */
  private final boolean isLocal;

  /**
   * Returns a unique identifier for this shard split in the format {@code indexName:shardId}.
   *
   * @return the split identifier
   */
  @Override
  public String getSplitId() {
    return indexName + ":" + shardId;
  }
}
