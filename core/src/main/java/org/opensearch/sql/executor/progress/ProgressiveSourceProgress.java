/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.executor.progress;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.Set;

/**
 * The only component that turns source observations into the published {@code fraction_done}.
 *
 * <h2>Combination</h2>
 *
 * <pre>
 *   if every source has an estimated document count and the total is positive:
 *       combined = sum(estimated_docs[i] * source_fraction[i]) / sum(estimated_docs[i])
 *   else:
 *       combined = average(source_fraction[i])
 *
 *   candidate = 0.80 * combined
 *   published = max(previous_published, min(candidate, 0.80))
 * </pre>
 *
 * Weighting by document count is what keeps a join honest: finishing a one-million-document side of
 * a ten-million-document join is 10% of the source work, not 50%. When any source's estimate is
 * missing the weights are not comparable, so every source gets equal weight rather than letting the
 * sources that happen to have estimates dominate.
 *
 * <h2>Why the published value is scaled into {@code [0, 0.8]}</h2>
 *
 * See {@link QueryProgress#PUBLIC_CEILING}. Source completion is not query completion, so draining
 * every source reports {@code 0.8} and the terminal status decides the rest.
 *
 * <h2>Why registration is sealed</h2>
 *
 * Sources are enumerated from the physical plan before execution and the set is then closed. A
 * denominator that grew as sources appeared would make the published value fall — a two-source plan
 * would report 0.4 after its first source finished, then drop when the second registered. Nothing
 * is published until {@link #seal()}, so the denominator is correct from the first non-zero value.
 *
 * <h2>Monotonicity</h2>
 *
 * Estimates are approximations and can be revised: a filtered source covers fewer documents than
 * the index holds, multi-valued group keys can count one document into several Composite buckets,
 * and a paged scan can run past a {@code TotalHits} lower bound. Per-source fractions are therefore
 * clamped to {@code 1.0} and the published value is latched at its maximum, so no estimation error
 * can make a client see progress regress or exceed the ceiling.
 *
 * <h2>Thread safety</h2>
 *
 * All state is guarded by {@code this}. Events arrive on transport threads (search-phase
 * callbacks), background IO threads (page fetches), and the Calcite worker thread; polls read from
 * a transport thread. The critical sections touch only small in-memory maps and never call out, so
 * no producer is delayed meaningfully and no lock is held across foreign code.
 */
public final class ProgressiveSourceProgress implements ProgressObserver {

  private static final double PUBLIC_SCALE = QueryProgress.PUBLIC_CEILING.fractionDone();

  private final Map<Long, SourceState> sources = new LinkedHashMap<>();

  private boolean sealed;

  /** Highest fraction handed to a caller so far. Never decreases, never exceeds the ceiling. */
  private double published;

  @Override
  public synchronized void register(long sourceId, OptionalLong estimatedDocs) {
    Objects.requireNonNull(estimatedDocs, "estimatedDocs must not be null");
    if (sealed) {
      throw new IllegalStateException(
          "cannot register source "
              + sourceId
              + " after sealing; the denominator is already public");
    }
    sources.computeIfAbsent(sourceId, id -> new SourceState(estimatedDocs));
  }

  @Override
  public synchronized void seal() {
    sealed = true;
  }

  /** Number of registered sources. Visible for tests and diagnostics. */
  public synchronized int sourceCount() {
    return sources.size();
  }

  @Override
  public synchronized void accept(SourceProgressEvent event) {
    Objects.requireNonNull(event, "event must not be null");
    SourceState state = sources.get(event.sourceId());
    if (state == null) {
      // A source the planning hook did not see — a scan added by a late rewrite, or an event
      // arriving
      // after a different query's id was reused. Dropping it keeps the denominator stable; the
      // worst
      // outcome is a fraction that advances more slowly than it could.
      return;
    }
    switch (event) {
      case SourceProgressEvent.SourceShapeObserved shape -> state.onShapeObserved(shape);
      case SourceProgressEvent.ShardsListed listed -> state.onShardsListed(listed);
      case SourceProgressEvent.ShardCompleted completed -> state.onShardCompleted(completed);
      case SourceProgressEvent.PageProgress page -> state.onPageProgress(page);
      case SourceProgressEvent.RowsObserved rows -> state.onRowsObserved(rows);
      case SourceProgressEvent.SourceCompleted ignored -> state.onCompleted();
    }
  }

  @Override
  public synchronized QueryProgress current() {
    if (!sealed) {
      return QueryProgress.ZERO;
    }
    double candidate = Math.min(PUBLIC_SCALE * combined(), PUBLIC_SCALE);
    if (Double.isFinite(candidate) && candidate > published) {
      published = candidate;
    }
    return new QueryProgress(published);
  }

  /**
   * Document-weighted mean of the source fractions, falling back to the unweighted mean when the
   * weights are not usable. Returns {@code 0.0} for a source-less plan, which is the correct
   * running answer: there is no source work to have made progress through.
   */
  private double combined() {
    if (sources.isEmpty()) {
      return 0.0;
    }
    long weightTotal = 0L;
    boolean everySourceEstimated = true;
    for (SourceState state : sources.values()) {
      if (state.estimatedDocs.isEmpty()) {
        everySourceEstimated = false;
        break;
      }
      weightTotal += state.estimatedDocs.getAsLong();
    }
    if (everySourceEstimated && weightTotal > 0L) {
      double weighted = 0.0;
      for (SourceState state : sources.values()) {
        weighted += state.estimatedDocs.getAsLong() * state.fraction();
      }
      return weighted / weightTotal;
    }
    double sum = 0.0;
    for (SourceState state : sources.values()) {
      sum += state.fraction();
    }
    return sum / sources.size();
  }

  /**
   * Per-source accounting. Not synchronized itself — every path into it holds the enclosing
   * monitor.
   */
  private static final class SourceState {

    private final OptionalLong estimatedDocs;

    private boolean complete;

    // --- Shard bookkeeping, scoped to the current search. A paged source re-lists shards per page,
    // so these are reset on every ShardsListed rather than accumulated across the source's life.
    private long currentRequestId = Long.MIN_VALUE;
    private final Map<SourceShardKey, Long> participating = new HashMap<>();
    private final Set<SourceShardKey> completedShards = new HashSet<>();
    private boolean shardWeightsKnown;

    // --- Response bookkeeping.
    private ProgressUnit unit = ProgressUnit.SINGLE_REQUEST;

    /**
     * Whether the producer declared this source's shape up front.
     *
     * <p>A declared shape is authoritative and a response can never change it. A response can tell
     * a Composite page from a hit page, but it cannot tell a PIT page from a single request that
     * happened to return rows — so letting it set the unit would reclassify a finished
     * single-request source as paged exactly when its response lands, collapsing it from "every
     * shard reported" to "one page of an estimated many".
     */
    private boolean shapeDeclared;

    private long completedPages;
    private long completedRows;
    private long coverage;
    private long lastNonEmptyPageCoverage;
    private long pageSize;
    private long observedTotal;
    private double inFlightPageFraction;

    /**
     * Whether the in-flight search has already delivered its response. Its shards are then counted
     * through {@link #completedPages} rather than as in-flight work, which is what keeps the
     * fraction continuous across a page boundary instead of double-counting the page that just
     * landed.
     */
    private boolean currentRequestReported;

    SourceState(OptionalLong estimatedDocs) {
      this.estimatedDocs = estimatedDocs;
    }

    void onShapeObserved(SourceProgressEvent.SourceShapeObserved event) {
      unit = event.unit();
      shapeDeclared = true;
      if (event.pageSize() > 0L) {
        pageSize = event.pageSize();
      }
    }

    void onShardsListed(SourceProgressEvent.ShardsListed event) {
      if (event.requestId() != currentRequestId) {
        currentRequestId = event.requestId();
        participating.clear();
        completedShards.clear();
        shardWeightsKnown = false;
        inFlightPageFraction = 0.0;
        currentRequestReported = false;
      }
      for (ShardWeight weight : event.shards()) {
        if (event.skipped().contains(weight.shard())) {
          // A can_match pre-filter excluded this shard: it does no work, so it belongs in neither
          // the
          // numerator nor the denominator. Counting it would stall progress below 1.0 for a source
          // that has in fact finished.
          continue;
        }
        participating.put(weight.shard(), weight.docCount());
        if (weight.docCount() > 0L) {
          shardWeightsKnown = true;
        }
      }
    }

    void onShardCompleted(SourceProgressEvent.ShardCompleted event) {
      if (event.requestId() != currentRequestId) {
        // Late callback from an already-superseded page. Its work is already counted in
        // completedPages; replaying it into the current page would double-count.
        return;
      }
      if (participating.containsKey(event.shard())) {
        completedShards.add(event.shard());
      }
    }

    void onPageProgress(SourceProgressEvent.PageProgress event) {
      if (event.requestId() != currentRequestId) {
        return;
      }
      double fraction = event.fraction();
      if (!Double.isFinite(fraction) || fraction <= 0.0) {
        return;
      }
      inFlightPageFraction = Math.max(inFlightPageFraction, Math.min(fraction, 1.0));
    }

    void onRowsObserved(SourceProgressEvent.RowsObserved event) {
      if (!shapeDeclared) {
        unit = event.unit();
      }
      if (event.pageSize() > 0L) {
        pageSize = event.pageSize();
      }
      if (event.observedTotal() > 0L) {
        // Grows only: with the default track_total_hits this is a lower bound, and a later page may
        // reveal a larger one.
        observedTotal = Math.max(observedTotal, event.observedTotal());
      }
      long units = Math.max(event.completedUnits(), 0L);
      if (unit == ProgressUnit.BUCKET_COVERAGE) {
        coverage += units;
        if (units > 0L) {
          lastNonEmptyPageCoverage = units;
        }
      } else {
        completedRows += units;
      }
      completedPages++;
      // The page that just landed is accounted for in completedPages now, so it must stop counting
      // as
      // in-flight work. The next ShardsListed re-opens the window for the following page.
      inFlightPageFraction = 0.0;
      currentRequestReported = true;
      if (unit == ProgressUnit.SINGLE_REQUEST) {
        // §8.3 / §8.4 — "the final response closes any callback gap". Shard callbacks are
        // best-effort: a
        // query result can be dropped because it arrived before onListShards, and fetch callbacks
        // are not
        // emitted for every shard that collected. For a one-round-trip source the response itself
        // proves
        // every shard finished, so the fraction must not depend on a later SourceCompleted
        // arriving.
        completedShards.addAll(participating.keySet());
      }
      if (event.complete()) {
        complete = true;
      }
    }

    void onCompleted() {
      complete = true;
    }

    /** This source's share of its own work, in {@code [0.0, 1.0]}. */
    double fraction() {
      if (complete) {
        return 1.0;
      }
      return clamp(
          switch (unit) {
            case BUCKET_COVERAGE -> coverageFraction();
            case PAGED_ROWS -> pagedFraction();
            // §8.3 / §8.4 — one round trip, so shard collection is the whole story.
            case SINGLE_REQUEST -> shardFraction();
          });
    }

    /**
     * §8.6 — Composite aggregation. Bucket {@code doc_count} sums are directly comparable to the
     * index's document estimate. Without an estimate, assume one more page the size of the last
     * non-empty one, which rises 1/2, 2/3, 3/4 … and never reaches 1.0 on its own.
     */
    private double coverageFraction() {
      if (estimatedDocs.isPresent() && estimatedDocs.getAsLong() > 0L) {
        return (double) coverage / estimatedDocs.getAsLong();
      }
      long nextPageCoverage = Math.max(lastNonEmptyPageCoverage, 1L);
      return (double) coverage / (coverage + nextPageCoverage);
    }

    /**
     * §8.5 — paged hit search, measured in pages. An in-flight page contributes at most one page's
     * worth, taken from its shard callbacks, so the value keeps moving between page boundaries
     * instead of stepping only when a page lands.
     *
     * <p>The two denominators are tried in the order the design specifies: the pre-execution
     * index-size estimate first, then a {@code TotalHits} value that happened to be in the
     * response. With neither, the adaptive form assumes one more page remains.
     */
    private double pagedFraction() {
      if (pageSize <= 0L) {
        // No page size to divide by — the in-flight shard view is the only signal available.
        return shardFraction();
      }
      double pages = completedPages + inFlightPages();
      OptionalLong denominatorDocs = pagedDenominatorDocs();
      if (denominatorDocs.isPresent()) {
        long estimatedPages = Math.max(ceilDiv(denominatorDocs.getAsLong(), pageSize), 1L);
        return pages / estimatedPages;
      }
      long adaptiveTotalRows = Math.max(completedRows + pageSize, pageSize);
      return (double) completedRows / adaptiveTotalRows;
    }

    /**
     * Share of one page the in-flight search has assembled, in {@code [0.0, 1.0)}.
     *
     * <p>Zero once the current search has reported its rows: that page is counted in {@code
     * completedPages} and counting it here as well would advance the source by two pages for one
     * page of work. Between pages this is what keeps the fraction moving rather than stepping only
     * when a page lands.
     */
    private double inFlightPages() {
      if (currentRequestReported) {
        return 0.0;
      }
      return Math.min(Math.max(inFlightPageFraction, shardFraction()), 1.0);
    }

    private OptionalLong pagedDenominatorDocs() {
      if (estimatedDocs.isPresent() && estimatedDocs.getAsLong() > 0L) {
        return estimatedDocs;
      }
      return observedTotal > 0L ? OptionalLong.of(observedTotal) : OptionalLong.empty();
    }

    /**
     * §8.3 / §8.4 — share of the current search's shards that finished collecting, weighted by live
     * primary document counts when the estimate provided them and by equal shard weight otherwise.
     */
    private double shardFraction() {
      if (participating.isEmpty()) {
        return 0.0;
      }
      if (shardWeightsKnown) {
        long denominator = 0L;
        long numerator = 0L;
        for (Map.Entry<SourceShardKey, Long> entry : participating.entrySet()) {
          denominator += entry.getValue();
          if (completedShards.contains(entry.getKey())) {
            numerator += entry.getValue();
          }
        }
        if (denominator > 0L) {
          return (double) numerator / denominator;
        }
      }
      return (double) completedShards.size() / participating.size();
    }

    private static double clamp(double value) {
      if (!Double.isFinite(value) || value < 0.0) {
        return 0.0;
      }
      return Math.min(value, 1.0);
    }

    private static long ceilDiv(long dividend, long divisor) {
      return (dividend + divisor - 1) / divisor;
    }
  }
}
