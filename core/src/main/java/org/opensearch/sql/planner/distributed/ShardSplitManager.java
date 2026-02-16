/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.distributed;

import java.util.List;

/**
 * Manages the discovery and enumeration of shard splits for distributed query execution.
 *
 * <p>The implementation is responsible for querying the cluster state to determine which shards
 * exist for a given index, where their primary and replica copies reside, and how many documents
 * they contain.
 *
 * <p>Concrete implementations (e.g., in the opensearch module) use {@code ClusterService} to access
 * the cluster state, index routing tables, and shard routing information.
 */
public interface ShardSplitManager {

  /**
   * Returns the list of shard splits for the given index.
   *
   * <p>Each split corresponds to a primary shard and includes its preferred node (the node hosting
   * the primary copy), replica node IDs, and whether the shard is local to the given node.
   *
   * @param indexName the name of the index to enumerate shards for
   * @param localNodeId the ID of the local (coordinator) node, used to set {@link
   *     ShardSplit#isLocal()}
   * @return list of shard splits for the index
   */
  List<ShardSplit> getSplits(String indexName, String localNodeId);

  /**
   * Returns the estimated document count for the given index.
   *
   * <p>This is used by the {@link CardinalityEstimator} and {@link QueryRouter} to decide whether
   * distributed execution is worthwhile.
   *
   * @param indexName the name of the index
   * @return estimated number of documents in the index
   */
  long getDocCount(String indexName);
}
