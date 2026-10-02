/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.executor.progress;

import java.util.Objects;

/**
 * Stable identity of one shard taking part in a source read.
 *
 * <p>Keyed on the index UUID rather than the index name so that two concrete indices resolved from
 * the same wildcard, or an index recreated under its old name, cannot collide. Shard callbacks
 * arrive out of order and may repeat, so progress accounting deduplicates on this key before
 * counting a shard as complete.
 *
 * @param indexUuid UUID of the concrete index the shard belongs to
 * @param shardId shard number within that index
 */
public record SourceShardKey(String indexUuid, int shardId) {

  public SourceShardKey {
    Objects.requireNonNull(indexUuid, "indexUuid must not be null");
    if (shardId < 0) {
      throw new IllegalArgumentException("shardId must not be negative, got " + shardId);
    }
  }
}
