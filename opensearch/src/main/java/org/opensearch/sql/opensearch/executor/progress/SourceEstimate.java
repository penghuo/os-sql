/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.progress;

import java.util.Map;
import java.util.Objects;
import org.opensearch.sql.executor.progress.SourceShardKey;

/**
 * Pre-execution size estimate for one source's concrete indices.
 *
 * <p>Both fields come from the same {@code _stats} response. {@code totalDocs} weights this source
 * against the query's other sources; {@code shardDocs} weights the shards inside one search, so a
 * single-request search over skewed shards advances proportionally to the documents each shard
 * holds rather than in equal steps.
 *
 * <p>This estimates index size, not filter selectivity. A heavily filtered source covers fewer
 * documents than its index holds, which makes the denominator conservative — progress then rises
 * faster than the estimate implies and is clamped. That is the intended trade: the alternative is
 * forcing exact total-hit tracking on every source, which the design rules out.
 *
 * @param totalDocs sum of live primary-shard document counts
 * @param shardDocs per-primary-shard document counts
 */
public record SourceEstimate(long totalDocs, Map<SourceShardKey, Long> shardDocs) {

  public SourceEstimate {
    shardDocs = Map.copyOf(Objects.requireNonNull(shardDocs, "shardDocs must not be null"));
    if (totalDocs < 0) {
      throw new IllegalArgumentException("totalDocs must not be negative, got " + totalDocs);
    }
  }
}
