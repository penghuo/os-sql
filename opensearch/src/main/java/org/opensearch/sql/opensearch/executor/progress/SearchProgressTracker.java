/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.progress;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.lucene.search.TotalHits;
import org.opensearch.action.search.SearchProgressListener;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchShard;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.sql.executor.progress.ShardWeight;
import org.opensearch.sql.executor.progress.SourceShardKey;

/**
 * Translates one search's phase callbacks into shard-level progress events.
 *
 * <p>This is what makes a query that issues a single request report anything at all before its
 * response arrives. A {@code stats} over a large index, or a sort the coordinator must finish
 * before emitting a row, has no intermediate rows to publish — but its shards report completion one
 * by one, and that is a real, bounded measure of source work done.
 *
 * <h2>Which callbacks count as completion</h2>
 *
 * Query-phase results and failures mark a shard's source collection done. Fetch callbacks
 * deliberately do not: fetch retrieves the selected hits after collection and is not emitted for
 * every shard that did collection work, so using it would leave a permanent gap for shards that
 * collected but contributed no top hits. Reduce callbacks also mark shards complete, because for an
 * aggregation the shards named in a partial or final reduce have demonstrably finished their share.
 *
 * <p>Failures count as completion. A failed shard has stopped doing source work; leaving it pending
 * would stall the fraction at a value the query can never leave, which reads to a client as a hung
 * query.
 *
 * <h2>Threading</h2>
 *
 * Callbacks arrive on search transport threads, possibly concurrently, and must not be delayed.
 * Every method here does a bounded amount of in-memory work and hands straight off to the observer,
 * which does its own locking.
 */
public final class SearchProgressTracker extends SearchProgressListener {

  private final SourceChannel channel;
  private final Map<SourceShardKey, Long> shardDocs;

  /**
   * Shards in {@code onListShards} order. Query callbacks identify a shard only by its index into
   * this list, so the list has to be retained to resolve them.
   */
  private volatile List<SourceShardKey> shardsByIndex = List.of();

  /**
   * Takes its shard weights from the channel.
   *
   * <p>There is deliberately no overload that accepts weights separately: the channel already
   * carries the source's pre-execution per-shard counts, and a second way to supply them is a way
   * to supply none. An empty map here means the estimate was unavailable, not that weighting was
   * forgotten, and makes shard progress fall back to equal weight.
   *
   * @param channel reporting handle for the source occurrence issuing this search
   */
  public SearchProgressTracker(SourceChannel channel) {
    this.channel = channel;
    this.shardDocs = channel.shardDocs();
  }

  @Override
  protected void onListShards(
      List<SearchShard> shards,
      List<SearchShard> skippedShards,
      SearchResponse.Clusters clusters,
      boolean fetchPhase) {
    List<SourceShardKey> ordered = new ArrayList<>(shards.size());
    List<ShardWeight> weights = new ArrayList<>(shards.size());
    for (SearchShard shard : shards) {
      SourceShardKey key = keyOf(shard);
      ordered.add(key);
      weights.add(new ShardWeight(key, shardDocs.getOrDefault(key, 0L)));
    }
    this.shardsByIndex = List.copyOf(ordered);
    Set<SourceShardKey> skipped = new HashSet<>();
    for (SearchShard shard : skippedShards) {
      skipped.add(keyOf(shard));
    }
    channel.shardsListed(weights, skipped);
  }

  @Override
  protected void onQueryResult(int shardIndex) {
    completeByIndex(shardIndex);
  }

  @Override
  protected void onQueryFailure(int shardIndex, SearchShardTarget target, Exception exception) {
    completeByIndex(shardIndex);
  }

  @Override
  protected void onPartialReduce(
      List<SearchShard> shards, TotalHits totalHits, InternalAggregations aggs, int reducePhase) {
    completeAll(shards);
  }

  @Override
  protected void onFinalReduce(
      List<SearchShard> shards, TotalHits totalHits, InternalAggregations aggs, int reducePhase) {
    completeAll(shards);
  }

  private void completeByIndex(int shardIndex) {
    List<SourceShardKey> shards = shardsByIndex;
    if (shardIndex < 0 || shardIndex >= shards.size()) {
      // Callback arrived before onListShards, or the index does not line up. Dropping it costs
      // resolution, never correctness: the response itself closes any remaining gap.
      return;
    }
    channel.shardCompleted(shards.get(shardIndex));
  }

  private void completeAll(List<SearchShard> shards) {
    for (SearchShard shard : shards) {
      channel.shardCompleted(keyOf(shard));
    }
  }

  /**
   * Keys a shard on its index UUID rather than its name, so two concrete indices behind one
   * wildcard — or an index recreated under a reused name — cannot be conflated.
   */
  private static SourceShardKey keyOf(SearchShard shard) {
    ShardId shardId = shard.getShardId();
    return new SourceShardKey(shardId.getIndex().getUUID(), shardId.id());
  }
}
