/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.executor.progress;

import java.util.Objects;

/**
 * One participating shard and the share of source work it represents.
 *
 * <p>{@code docCount} is the shard's live primary document count, taken from the index-stats
 * estimate resolved before execution. It is {@code 0} when no estimate was available, in which case
 * progress accounting falls back to equal weight across participating shards — a 10-shard index
 * then advances in tenths regardless of how skewed the shards actually are.
 *
 * @param shard shard identity
 * @param docCount live primary document count, or {@code 0} when unknown
 */
public record ShardWeight(SourceShardKey shard, long docCount) {

  public ShardWeight {
    Objects.requireNonNull(shard, "shard must not be null");
    if (docCount < 0) {
      throw new IllegalArgumentException("docCount must not be negative, got " + docCount);
    }
  }
}
