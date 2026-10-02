/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.progress;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import javax.annotation.Nullable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.runtime.Hook;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.sql.executor.progress.ProgressObserver;
import org.opensearch.sql.opensearch.storage.scan.AbstractCalciteIndexScan;

/**
 * Enumerates the physical plan's index scans, resolves their size estimates, and seals the progress
 * denominator before execution starts.
 *
 * <h2>Why registration hangs off a planning hook</h2>
 *
 * The plan handed to the execution engine is pre-implementation: Calcite still expands correlated
 * and uncorrelated subqueries, decorrelates, and may restructure the tree before code generation.
 * Registering from that plan misses sources entirely — the scan inside {@code where id in
 * [source=inner | fields id]} lives in a {@link RexSubQuery} and is not among any node's inputs, so
 * it would never be registered and would report nothing for the whole query. {@link
 * Hook#PLAN_BEFORE_IMPLEMENTATION} fires with the final physical {@link RelRoot}, on the very
 * object graph that is then code-generated, which is the only point where the source set is both
 * complete and identity-stable.
 *
 * <h2>Why registration must finish before the first publication</h2>
 *
 * A denominator that grew as scans appeared would make the published fraction fall: a two-source
 * plan would reach 0.4 when its first source finished, then halve when the second registered. The
 * observer therefore publishes nothing until sealed, and sealing happens once, after the whole plan
 * has been walked.
 *
 * <h2>Estimate resolution</h2>
 *
 * One {@code _stats} lookup per distinct index expression, shared by every scan reading it — a
 * self-join issues one lookup, not two. The lookup is bounded and all-or-nothing: an unauthorized
 * caller, a timeout, or a failed shard yields no estimate for that source and the calculator falls
 * back to equal weight. Failure here never fails or delays the query.
 */
public final class SourceRegistrar {

  private static final Logger LOG = LogManager.getLogger(SourceRegistrar.class);

  /**
   * Bound on the whole registration step.
   *
   * <p>Source estimation is a latency tax on every asynchronous query and buys only resolution, so
   * it gets a budget short enough to be invisible next to the query it describes. Exceeding it
   * costs equal-weight progress, which is the documented fallback.
   */
  static final long ESTIMATE_TIMEOUT_MILLIS = 2_000L;

  private SourceRegistrar() {
    throw new AssertionError(
        SourceRegistrar.class.getCanonicalName()
            + " is a utility class and must not be initialized");
  }

  /**
   * Arms registration for the code generation about to happen on this thread.
   *
   * @param context context the scans will resolve their bindings from; {@code null} when unobserved
   * @param observer sink to register against
   * @return handle that must be closed once code generation has finished
   */
  public static Registration install(
      @Nullable ProgressiveQueryContext context, ProgressObserver observer) {
    if (context == null || observer == ProgressObserver.NOOP) {
      return Registration.NOOP;
    }
    return new Registration(context, observer);
  }

  /**
   * Active registration for one query.
   *
   * <p>Not reusable: it guards a one-shot transition, because sealing twice or registering against
   * a sealed observer would mean the denominator changed after a value was published.
   */
  public static final class Registration implements AutoCloseable {

    /** Handle for unobserved queries. */
    static final Registration NOOP = new Registration();

    private final AtomicBoolean sealed = new AtomicBoolean();
    @Nullable private final ProgressiveQueryContext context;
    @Nullable private final ProgressObserver observer;
    @Nullable private final Hook.Closeable hookHandle;

    private Registration() {
      this.context = null;
      this.observer = null;
      this.hookHandle = null;
    }

    private Registration(ProgressiveQueryContext context, ProgressObserver observer) {
      this.context = context;
      this.observer = observer;
      this.hookHandle =
          Hook.PLAN_BEFORE_IMPLEMENTATION.addThread((Consumer<RelRoot>) this::onPhysicalPlan);
    }

    /**
     * Registers every scan in the physical plan, then seals.
     *
     * <p>A firing that finds no scans does not seal. The hook is global to Calcite and can fire for
     * a nested prepare — a materialization check, for instance — whose plan has none of this
     * query's sources; consuming the one-shot there would silently disable progress for the real
     * plan. A genuinely source-less plan is sealed by {@link #sealIfPending()} instead.
     */
    private void onPhysicalPlan(RelRoot root) {
      if (sealed.get() || context == null || observer == null || root == null) {
        return;
      }
      try {
        List<AbstractCalciteIndexScan> scans = collectScans(root.rel);
        if (scans.isEmpty()) {
          return;
        }
        if (!sealed.compareAndSet(false, true)) {
          return;
        }
        Map<String, Optional<SourceEstimate>> estimates = new HashMap<>();
        long nextSourceId = 0L;
        for (AbstractCalciteIndexScan scan : scans) {
          String indexKey = scan.osIndex.getIndexName().toString();
          Optional<SourceEstimate> estimate =
              estimates.computeIfAbsent(indexKey, key -> estimate(scan));
          long sourceId = nextSourceId++;
          observer.register(
              sourceId,
              estimate
                  .map(resolved -> OptionalLong.of(resolved.totalDocs()))
                  .orElseGet(OptionalLong::empty));
          context.registerPosition(
              scan, sourceId, estimate.map(SourceEstimate::shardDocs).orElseGet(Map::of));
        }
        observer.seal();
      } catch (RuntimeException e) {
        // A plan shape the walker does not understand must not fail the query. Whatever was
        // registered before
        // the failure stays, and sealing below keeps the observer publishable.
        LOG.debug("Progress source registration failed; progress may under-report", e);
        if (observer != null) {
          observer.seal();
        }
      }
    }

    /**
     * Seals if the hook never produced a source set. Call once code generation has finished:
     * nothing can register after that, and an unsealed observer would report {@code 0.0} forever
     * rather than the source-less plan's correct {@code 0.0}-while-running, {@code 1.0}-on-success
     * behaviour.
     */
    public void sealIfPending() {
      if (observer != null && sealed.compareAndSet(false, true)) {
        observer.seal();
      }
    }

    @Override
    public void close() {
      if (hookHandle != null) {
        hookHandle.close();
      }
      sealIfPending();
    }
  }

  private static Optional<SourceEstimate> estimate(AbstractCalciteIndexScan scan) {
    try {
      return scan.osIndex
          .getClient()
          .documentCountEstimate(
              scan.osIndex.getIndexName().getIndexNames(), ESTIMATE_TIMEOUT_MILLIS);
    } catch (RuntimeException e) {
      LOG.debug("Document count estimate unavailable; falling back to equal source weight", e);
      return Optional.empty();
    }
  }

  /**
   * Hard cap on how many scan positions one plan may register.
   *
   * <p>Counting positions means a shared subtree is walked once per reference, so a pathological
   * DAG could in principle expand badly. Physical enumerable plans do not look like that, but
   * progress accounting must not be the thing that makes a query fall over.
   */
  static final int MAX_POSITIONS = 1_000;

  /**
   * Collects index scan <em>positions</em> in a stable depth-first order, descending into subquery
   * plans, and records the row quota each position's consumer imposes.
   *
   * <h2>Why positions and not distinct nodes</h2>
   *
   * Calcite canonicalizes equal plan nodes, so an equijoin of an index to itself leaves both of its
   * physical scan positions pointing at one {@code CalciteEnumerableIndexScan} object. Those
   * positions still issue two independent searches at execution time, so each is its own source.
   * De-duplicating by node identity would register one source for two reads: whichever read
   * finished first would report the plan's whole source work as done, and the other would report
   * nothing.
   *
   * <h2>Why expressions are walked too</h2>
   *
   * {@code getInputs()} alone is not enough: an uncorrelated {@code in} subquery survives into the
   * physical plan as a {@link RexSubQuery} inside a filter or project expression, holding a plan
   * that no node lists as an input. Without walking expressions that scan is never registered and
   * reports nothing.
   *
   * <h2>Why the row quota is read from the plan</h2>
   *
   * A source capped by {@code head} is finished with its work the moment it has handed over that
   * many rows — not when the query ends. Knowing the cap in advance lets the scan say so at exactly
   * that moment, which matters when the plan has other sources still to run: a joined subsearch
   * capped at two rows would otherwise hold the whole query's fraction down for the entire outer
   * read. Waiting for the query to finish is too late, and a bare close cannot be trusted because
   * Linq4j closes a source from its own {@code finally} block whether the query is finishing or
   * failing.
   */
  static List<AbstractCalciteIndexScan> collectScans(RelNode root) {
    List<AbstractCalciteIndexScan> scans = new ArrayList<>();
    collectScans(root, scans, Collections.newSetFromMap(new IdentityHashMap<>()));
    return scans;
  }

  /**
   * @param onPath nodes on the current recursion path, guarding against a cyclic reference. Scoped
   *     to the path rather than the whole walk, so a node reached through two different parents —
   *     two positions — counts twice.
   */
  private static void collectScans(
      @Nullable RelNode node, List<AbstractCalciteIndexScan> out, Set<RelNode> onPath) {
    if (node == null || out.size() >= MAX_POSITIONS || !onPath.add(node)) {
      return;
    }
    try {
      if (node instanceof AbstractCalciteIndexScan scan) {
        out.add(scan);
        return;
      }
      // Visits this node's own expressions. The shuttle returns everything unchanged, so the plan
      // is not
      // rewritten; the return value is discarded and only the traversal matters.
      node.accept(
          new RexShuttle() {
            @Override
            public RexNode visitSubQuery(RexSubQuery subQuery) {
              collectScans(subQuery.rel, out, onPath);
              return subQuery;
            }
          });
      for (RelNode input : node.getInputs()) {
        collectScans(input, out, onPath);
      }
    } finally {
      onPath.remove(node);
    }
  }
}
