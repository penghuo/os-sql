/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.progress;

import java.util.ArrayList;
import java.util.List;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchShard;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.sql.executor.progress.SourceShardKey;

/**
 * Drives {@link SearchProgressTracker}'s search-phase callbacks from tests.
 *
 * <p>OpenSearch keeps {@code SearchProgressListener}'s {@code notify*} entry points package-private
 * to its own package, so a test outside it cannot replay a search's phases. The overrides on the
 * tracker are {@code protected}, which makes them reachable from this package — so this class lives
 * here and exposes them to tests elsewhere, rather than widening production visibility for
 * testing's sake.
 */
public final class SearchProgressTrackerTestAccess {

  private SearchProgressTrackerTestAccess() {}

  /** Replays {@code onListShards} with weights resolved from whatever the tracker was given. */
  public static void listShards(
      SearchProgressTracker tracker, List<SourceShardKey> shards, List<SourceShardKey> skipped) {
    tracker.onListShards(
        toSearchShards(shards), toSearchShards(skipped), SearchResponse.Clusters.EMPTY, false);
  }

  /** Replays {@code onQueryResult} for the shard at {@code shardIndex} of the listed order. */
  public static void queryResult(SearchProgressTracker tracker, int shardIndex) {
    tracker.onQueryResult(shardIndex);
  }

  /** Replays {@code onQueryFailure} for the shard at {@code shardIndex} of the listed order. */
  public static void queryFailure(SearchProgressTracker tracker, int shardIndex) {
    tracker.onQueryFailure(shardIndex, null, new IllegalStateException("shard failed"));
  }

  /** Replays {@code onFinalReduce} naming {@code shards} as incorporated. */
  public static void finalReduce(SearchProgressTracker tracker, List<SourceShardKey> shards) {
    tracker.onFinalReduce(toSearchShards(shards), null, null, 1);
  }

  private static List<SearchShard> toSearchShards(List<SourceShardKey> keys) {
    List<SearchShard> shards = new ArrayList<>(keys.size());
    for (SourceShardKey key : keys) {
      shards.add(
          new SearchShard(
              null,
              new ShardId(new Index("index-" + key.indexUuid(), key.indexUuid()), key.shardId())));
    }
    return shards;
  }
}
