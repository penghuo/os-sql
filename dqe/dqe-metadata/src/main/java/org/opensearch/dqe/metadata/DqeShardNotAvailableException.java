/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.metadata;

import org.opensearch.dqe.parser.DqeErrorCode;
import org.opensearch.dqe.parser.DqeException;

/** Thrown when a required shard has no available copy (neither primary nor replica). */
public class DqeShardNotAvailableException extends DqeException {

  private final String indexName;
  private final int shardId;

  public DqeShardNotAvailableException(String indexName, int shardId) {
    super(
        "Shard not available: " + indexName + "[" + shardId + "]",
        DqeErrorCode.SHARD_NOT_AVAILABLE);
    this.indexName = indexName;
    this.shardId = shardId;
  }

  public String getIndexName() {
    return indexName;
  }

  public int getShardId() {
    return shardId;
  }
}
