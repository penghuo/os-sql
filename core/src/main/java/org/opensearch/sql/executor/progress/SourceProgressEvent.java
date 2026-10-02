/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.executor.progress;

import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Typed signal from one physical source occurrence to the query's progress calculator.
 *
 * <p>Producers never compute a fraction. They report what they observed — which shards took part,
 * which finished, how many rows or buckets a page carried — and {@link ProgressiveSourceProgress}
 * turns that into the single published value. Keeping the arithmetic in one place is what makes the
 * published fraction's bounds and monotonicity provable: a producer that miscounts can skew the
 * estimate but cannot break the contract.
 *
 * <p>Every event carries its {@code sourceId} so a self-join over one index, which registers two
 * occurrences, keeps two independent progress states. Events that belong to a single OpenSearch
 * search also carry a {@code requestId}, because a paged source issues many searches and shard
 * bookkeeping is per-search, not per-source.
 */
public sealed interface SourceProgressEvent {

  /** Stable id of the physical source occurrence this event describes. */
  long sourceId();

  /**
   * How this source's responses should be counted, declared before its first search.
   *
   * <p>Shape has to be known up front. Until the calculator knows a source is paged it can only
   * read "every shard reported" as "the source is done", which would pin a multi-page scan's
   * contribution at 1.0 after its first page — and the monotonic latch would then hold that wrong
   * value for the rest of the query. The scan knows its own shape when it builds the request: a
   * Composite aggregation in the source builder, a point-in-time id, or neither.
   *
   * @param pageSize {@code size} on the physical search request, or {@code 0} for shapes without
   *     one
   */
  record SourceShapeObserved(long sourceId, ProgressUnit unit, long pageSize)
      implements SourceProgressEvent {

    public SourceShapeObserved {
      Objects.requireNonNull(unit, "unit must not be null");
    }
  }

  /**
   * Shards enlisted for one search.
   *
   * <p>Resets the shard bookkeeping for {@code requestId}: a paged source re-lists shards on every
   * page, and carrying stale completions across pages would report work twice. Skipped shards
   * (those a {@code can_match} pre-filter excluded) contribute no work and are tracked separately
   * so they neither inflate the numerator nor the denominator.
   *
   * @param shards participating shards with their weights
   * @param skipped shards excluded before query execution
   */
  record ShardsListed(
      long sourceId, long requestId, List<ShardWeight> shards, Set<SourceShardKey> skipped)
      implements SourceProgressEvent {

    public ShardsListed {
      shards = List.copyOf(Objects.requireNonNull(shards, "shards must not be null"));
      skipped = Set.copyOf(Objects.requireNonNull(skipped, "skipped must not be null"));
    }
  }

  /**
   * One shard finished its query-phase collection for {@code requestId}.
   *
   * <p>Emitted for both success and failure: a failed shard has stopped doing source work, and
   * treating it as still pending would stall progress at a value the query will never leave.
   * Repeated events for the same shard are idempotent.
   */
  record ShardCompleted(long sourceId, long requestId, SourceShardKey shard)
      implements SourceProgressEvent {

    public ShardCompleted {
      Objects.requireNonNull(shard, "shard must not be null");
    }
  }

  /**
   * Sub-page progress inside an in-flight page, as a fraction of one expected page.
   *
   * <p>Reserved for producers that can observe partial page assembly directly. P0 derives the
   * in-flight contribution from {@link ShardsListed} / {@link ShardCompleted} instead, so nothing
   * emits this yet; the calculator honours it so a later producer needs no change here.
   *
   * @param fraction completed share of one page, clamped into {@code [0.0, 1.0]} by the calculator
   * @param expectedUnits rows the page is expected to carry, or {@code 0} when unknown
   */
  record PageProgress(long sourceId, long requestId, double fraction, long expectedUnits)
      implements SourceProgressEvent {}

  /**
   * A page or single response completed, with the units it contributed.
   *
   * <p>{@code unit} selects the formula: paged hit searches are measured in pages (§8.5 of the
   * design), Composite aggregations in document coverage summed from bucket counts (§8.6). The two
   * cannot share one formula — a Composite page's bucket coverage is comparable to the index's
   * document count, while a hit page's row count is only comparable to a page size.
   *
   * @param unit what {@code completedUnits} counts
   * @param completedUnits units this response contributed; cumulative accounting is the
   *     calculator's job, so producers report per-response deltas
   * @param pageSize {@code size} on the physical OpenSearch search request — the page the producer
   *     asked for, not the client's requested row count. {@code 0} when the shape has no page size.
   * @param observedTotal total naturally present in the response (a {@code TotalHits} value), or
   *     {@code 0} when absent; never obtained by forcing exact total-hit tracking
   * @param exact whether {@code observedTotal} is exact rather than a lower bound
   * @param complete whether this response exhausted the source. P0 producers signal completion with
   *     {@link SourceCompleted} instead, because the scan — not the response — owns the decision
   *     that iteration is over; the flag is honoured for producers that learn of exhaustion from
   *     the response.
   */
  record RowsObserved(
      long sourceId,
      long requestId,
      ProgressUnit unit,
      long completedUnits,
      long pageSize,
      long observedTotal,
      boolean exact,
      boolean complete)
      implements SourceProgressEvent {

    public RowsObserved {
      Objects.requireNonNull(unit, "unit must not be null");
    }
  }

  /**
   * The source will produce no more work.
   *
   * <p>Only normal exhaustion and an intentional upstream limit emit this. Cancellation and failure
   * deliberately do not, so an aborted query keeps the last fraction it actually reached rather
   * than rounding its abandoned sources up to done.
   */
  record SourceCompleted(long sourceId, CompletionReason reason) implements SourceProgressEvent {

    public SourceCompleted {
      Objects.requireNonNull(reason, "reason must not be null");
    }
  }
}
