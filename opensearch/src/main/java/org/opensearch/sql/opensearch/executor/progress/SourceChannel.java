/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.progress;

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.opensearch.sql.executor.progress.ProgressObserver;
import org.opensearch.sql.executor.progress.ProgressUnit;
import org.opensearch.sql.executor.progress.ShardWeight;
import org.opensearch.sql.executor.progress.SourceProgressEvent;
import org.opensearch.sql.executor.progress.SourceShardKey;

/**
 * Reporting handle for one OpenSearch search issued by one physical source occurrence.
 *
 * <p>Holds the {@code (sourceId, requestId)} pair so producers never carry it, and the source's
 * per-shard document weights so the search-phase listener can weight shard completion without
 * reaching back into the registry. Every method is a bounded hand-off to the observer.
 *
 * <p>Source-scoped reporting — shape and completion — lives on {@link ProgressiveQueryContext}
 * instead, because neither belongs to a single search.
 */
public final class SourceChannel {

  /** Channel for unobserved queries. Every call is a no-op. */
  public static final SourceChannel NOOP =
      new SourceChannel(ProgressObserver.NOOP, 0L, 0L, Map.of(), ProgressUnit.SINGLE_REQUEST);

  private final ProgressObserver observer;
  private final long sourceId;
  private final long requestId;
  private final Map<SourceShardKey, Long> shardDocs;
  private final ProgressUnit declaredUnit;

  /**
   * Public so tests can drive a channel without standing up a whole query context. Production code
   * obtains channels from {@link ProgressiveQueryContext#openChannel()}, which supplies the
   * registered weights and the shape the scan declared.
   */
  public SourceChannel(
      ProgressObserver observer,
      long sourceId,
      long requestId,
      Map<SourceShardKey, Long> shardDocs,
      ProgressUnit declaredUnit) {
    this.observer = observer;
    this.sourceId = sourceId;
    this.requestId = requestId;
    this.shardDocs = Map.copyOf(shardDocs);
    this.declaredUnit = declaredUnit;
  }

  /**
   * Shape this source declared before its first search. Response reporting uses this rather than
   * inferring a shape, because a response cannot distinguish a paged search's page from a single
   * request's only answer.
   */
  public ProgressUnit declaredUnit() {
    return declaredUnit;
  }

  /** True when this channel discards everything; lets callers skip building event payloads. */
  public boolean isNoop() {
    return observer == ProgressObserver.NOOP;
  }

  /**
   * Per-primary-shard document counts from the pre-execution estimate, empty when unavailable. An
   * empty map makes shard progress fall back to equal weight.
   */
  public Map<SourceShardKey, Long> shardDocs() {
    return shardDocs;
  }

  /** Reports the shards enlisted for this search. */
  public void shardsListed(List<ShardWeight> shards, Set<SourceShardKey> skipped) {
    observer.accept(new SourceProgressEvent.ShardsListed(sourceId, requestId, shards, skipped));
  }

  /** Reports that one shard finished its query-phase collection for this search. */
  public void shardCompleted(SourceShardKey shard) {
    observer.accept(new SourceProgressEvent.ShardCompleted(sourceId, requestId, shard));
  }

  /**
   * Reports the units one response contributed, under this source's declared shape.
   *
   * <p>There is no overload taking a unit: the shape is a property of the source, and letting a
   * caller pass one is how a finished single-request source gets relabelled as paged.
   */
  public void rowsObserved(long completedUnits, long pageSize, long observedTotal) {
    observer.accept(
        new SourceProgressEvent.RowsObserved(
            sourceId, requestId, declaredUnit, completedUnits, pageSize, observedTotal));
  }
}
