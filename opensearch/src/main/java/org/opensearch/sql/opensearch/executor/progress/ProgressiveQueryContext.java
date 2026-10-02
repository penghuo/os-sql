/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.progress;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.Nullable;
import org.opensearch.sql.calcite.plan.Scannable;
import org.opensearch.sql.executor.progress.CompletionReason;
import org.opensearch.sql.executor.progress.ProgressObserver;
import org.opensearch.sql.executor.progress.ProgressUnit;
import org.opensearch.sql.executor.progress.SourceProgressEvent;
import org.opensearch.sql.executor.progress.SourceShardKey;

/**
 * Request-scoped channel between Calcite execution and the OpenSearch source layer.
 *
 * <p>Progress producers sit deep inside the storage path — an enumerator, a page fetch, a
 * search-phase callback — and none of them take the query's listener as a parameter. Threading one
 * through every signature would touch unrelated code and change how synchronous queries are built.
 * Instead the observer is installed as a scoped thread binding for the duration of execution, the
 * same shape the engine already uses for {@code QueryProfiling} and {@code CalcitePlanContext}.
 *
 * <h2>Scoping discipline</h2>
 *
 * Every installation is paired with a {@link Scope} that restores the previous binding in {@code
 * close()}. Worker and transport threads are pooled, so a binding left behind would attribute a
 * later query's source work to a job that has already completed — reporting progress for the wrong
 * query and keeping its observer reachable. {@link #capture()} and {@link #restore} carry a binding
 * explicitly across thread boundaries that do not inherit thread-locals, notably background page
 * fetches.
 *
 * <h2>Source identity is the physical plan position</h2>
 *
 * Sources are registered from the physical plan immediately before code generation. A plan node
 * maps to a <em>list</em> of ids — a position pool — rather than to one id, because the node alone
 * is not a sufficient key:
 *
 * <ul>
 *   <li><b>Not the index expression.</b> Time-bounds pruning rewrites {@code logs-*} into the
 *       concrete indices that survive, so the built request's index names need not match what was
 *       registered.
 *   <li><b>Not the node either.</b> Calcite canonicalizes equal plan nodes, so a same-index
 *       equijoin leaves both of its scan positions pointing at one object. They are still two
 *       independent reads, so the node hands out one id per position; keying on the node would
 *       report the whole plan done when the first of the two finished.
 *   <li><b>Not a claim pool per index.</b> Two occurrences reading the same index are distinct
 *       sources, and a pool shared across unrelated nodes would let whichever enumerator runs first
 *       take another position's id.
 *   <li><b>Stable across enumeration.</b> A position claims its id once, at code generation, and
 *       the id is baked into that generated call — so re-enumerating a position, as the inner side
 *       of a nested-loop join does, reuses the same source rather than inventing a new one.
 * </ul>
 */
public final class ProgressiveQueryContext {

  /** Source id used when execution is inside the query but not inside any particular scan. */
  public static final long NO_SOURCE = -1L;

  private static final ThreadLocal<Binding> CURRENT = new ThreadLocal<>();

  private final ProgressObserver observer;

  /**
   * Allocates ids for individual OpenSearch searches. Shard bookkeeping is per-search, not
   * per-source: a paged scan re-lists its shards on every page, and without a request id the
   * calculator could not tell a fresh page's shard list from a replay of the previous one.
   */
  private final AtomicLong requestIds = new AtomicLong();

  /**
   * Source ids registered for each physical plan node, in plan order, waiting to be claimed.
   *
   * <p>A node maps to a <em>list</em>, not a single id, because Calcite canonicalizes equal plan
   * nodes: an equijoin of an index to itself ends up with both of its physical scan positions
   * pointing at one {@code CalciteEnumerableIndexScan} object. Those positions are still two
   * independent reads — two searches, two sets of shards — so they are two sources, and the node
   * has to hand out one id per position rather than one id per object.
   */
  private final Map<Object, Deque<Long>> positionPools = new IdentityHashMap<>();

  /**
   * Last id handed out per node, reused if a position claims more than once. Claims happen once per
   * position per execution, so this is defensive only.
   */
  private final Map<Object, Long> lastClaimed = new IdentityHashMap<>();

  /**
   * Set when execution failed or was cancelled, before the enumerators are torn down.
   *
   * <p>Closing a scan normally means its consumer stopped asking — a {@code head}, a join that
   * found its match — and that genuinely completes the source. Closing it during an abort means the
   * opposite, and completing it there would round an abandoned query's progress up to full. The
   * engine raises this flag before it closes anything, so close can tell the two apart.
   */
  private volatile boolean aborted;

  /** Sources closed early by their consumer, awaiting the end-of-draining confirmation. */
  private final Set<Long> consumerClosed = new LinkedHashSet<>();

  /** Source ids in claim order, so a limit can identify the subtree code-generated beneath it. */
  private final Set<Long> claimedOrder = new LinkedHashSet<>();

  /** Per-source shard weights from the pre-execution estimate. Written before sealing only. */
  private final Map<Long, Map<SourceShardKey, Long>> shardDocs = new HashMap<>();

  /**
   * Shape each source declared. Channels carry it so a response reporter never has to re-derive it
   * — a response cannot distinguish a point-in-time page from a single request that returned rows,
   * and guessing would reclassify a finished single-request source as paged.
   */
  private final Map<Long, ProgressUnit> declaredUnits = new HashMap<>();

  private ProgressiveQueryContext(ProgressObserver observer) {
    this.observer = observer;
  }

  /** The observer and source occurrence bound to one thread. */
  public record Binding(ProgressiveQueryContext context, long sourceId) {

    public Binding {
      Objects.requireNonNull(context, "context must not be null");
    }
  }

  /** Undoes one installation. Always use with try-with-resources. */
  public interface Scope extends AutoCloseable {
    @Override
    void close();
  }

  private static final Scope NO_SCOPE = () -> {};

  /**
   * Creates a context for {@code observer}, or {@code null} for {@link ProgressObserver#NOOP} so
   * the synchronous path allocates nothing. Pair with {@link #open}.
   */
  @Nullable
  public static ProgressiveQueryContext create(ProgressObserver observer) {
    Objects.requireNonNull(observer, "observer must not be null");
    return observer == ProgressObserver.NOOP ? null : new ProgressiveQueryContext(observer);
  }

  /** Installs {@code context} on the current thread; a {@code null} context installs nothing. */
  public static Scope open(@Nullable ProgressiveQueryContext context) {
    return context == null ? NO_SCOPE : install(new Binding(context, NO_SOURCE));
  }

  /**
   * Records one physical source position and its shard weights.
   *
   * <p>Call once per position, before {@link ProgressObserver#seal()}. A node that occupies two
   * positions is registered twice and receives two ids.
   *
   * @param occurrence the physical plan node, used as an identity key
   */
  public synchronized void registerPosition(
      Object occurrence, long sourceId, Map<SourceShardKey, Long> shardWeights) {
    Objects.requireNonNull(occurrence, "occurrence must not be null");
    positionPools.computeIfAbsent(occurrence, node -> new ArrayDeque<>()).addLast(sourceId);
    if (!shardWeights.isEmpty()) {
      shardDocs.put(sourceId, Map.copyOf(shardWeights));
    }
  }

  /**
   * Claims the next source id registered for {@code occurrence}.
   *
   * <p>Called once per physical position, while that position is being code-generated. Code
   * generation is the only stage that sees the two positions of a self-join as distinct — Calcite
   * canonicalizes equal plan nodes, so both positions are the same object by then — which is why
   * the id is claimed here and baked into the generated call rather than looked up at runtime.
   *
   * <p>If the pool is exhausted the last id is reused: claims and registrations both come from the
   * same physical plan, so that should not happen, and reusing keeps the position reporting instead
   * of going silent.
   *
   * @return the claimed id, or {@link Scannable#NO_PROGRESS_SOURCE} when this query is not observed
   */
  public static long claimPosition(Object occurrence) {
    Binding current = CURRENT.get();
    if (current == null || occurrence == null) {
      return Scannable.NO_PROGRESS_SOURCE;
    }
    ProgressiveQueryContext context = current.context();
    synchronized (context) {
      Deque<Long> pool = context.positionPools.get(occurrence);
      if (pool != null && !pool.isEmpty()) {
        long claimed = pool.removeFirst();
        context.lastClaimed.put(occurrence, claimed);
        context.claimedOrder.add(claimed);
        return claimed;
      }
      Long last = context.lastClaimed.get(occurrence);
      return last == null ? Scannable.NO_PROGRESS_SOURCE : last;
    }
  }

  /**
   * Builds the binding for an id claimed at code-generation time.
   *
   * @return the binding, or {@code null} for {@link Scannable#NO_PROGRESS_SOURCE} or an unobserved
   *     query
   */
  @Nullable
  public static Binding bindingFor(long sourceId) {
    Binding current = CURRENT.get();
    if (current == null || sourceId == Scannable.NO_PROGRESS_SOURCE) {
      return null;
    }
    return new Binding(current.context(), sourceId);
  }

  /**
   * Source ids claimed so far, in claim order.
   *
   * <p>Used by {@link ProgressAwareEnumerableLimit} to learn which sources sit beneath it: the
   * scans in its subtree claim their ids while that subtree is being code-generated, so the
   * difference across that step is exactly its own set. Cheaper and less fragile than a second
   * traversal, and it naturally excludes a sibling branch's sources.
   */
  synchronized Set<Long> claimedSourceIds() {
    return new LinkedHashSet<>(claimedOrder);
  }

  /** Completes several sources at once, for a limit reporting everything beneath it. */
  void completeSources(Collection<Long> sourceIds, CompletionReason reason) {
    for (long sourceId : sourceIds) {
      observer.accept(new SourceProgressEvent.SourceCompleted(sourceId, reason));
    }
  }

  /**
   * Marks this query's execution as aborted, so a scan closing afterwards is not mistaken for a
   * consumer that finished normally.
   *
   * <p>This covers the failures that surface before any teardown. It cannot cover every case on its
   * own: Linq4j closes a source inside its own {@code finally}, which runs before the exception
   * reaches the execution engine, so a scan also has to check the signals it can see for itself —
   * its own failures, task cancellation, and thread interruption.
   */
  public void markAborted() {
    aborted = true;
  }

  /** Whether execution was aborted; consulted by a scan deciding what its close means. */
  public static boolean isAborted(@Nullable Binding binding) {
    return binding != null && binding.context().aborted;
  }

  /**
   * Records that a source's consumer closed it before it was exhausted — a {@code head}, a
   * coordinator-side {@code take}, a join that stopped probing.
   *
   * <p>Kept here rather than emitted immediately so the explicit end-of-draining signal can confirm
   * it. A close on its own is ambiguous; a close followed by the query draining successfully is
   * not.
   */
  public static void recordConsumerClose(@Nullable Binding binding) {
    if (binding == null) {
      return;
    }
    ProgressiveQueryContext context = binding.context();
    synchronized (context) {
      context.consumerClosed.add(binding.sourceId());
    }
  }

  /**
   * Completes every source whose consumer closed it early.
   *
   * <p>Called by the execution engine once result draining has succeeded. That is the explicit,
   * unambiguous "the coordinator stopped on purpose" signal: a query that failed or was cancelled
   * never reaches it, so an abandoned source is never rounded up to done.
   */
  public void completeConsumerClosedSources() {
    List<Long> pending;
    synchronized (this) {
      if (consumerClosed.isEmpty()) {
        return;
      }
      pending = List.copyOf(consumerClosed);
      consumerClosed.clear();
    }
    for (long sourceId : pending) {
      observer.accept(
          new SourceProgressEvent.SourceCompleted(sourceId, CompletionReason.UPSTREAM_LIMIT));
    }
  }

  /**
   * Snapshots the current binding for replay on another thread; {@code null} when nothing is bound.
   */
  @Nullable
  public static Binding capture() {
    return CURRENT.get();
  }

  /** Replays a captured binding. Accepts {@code null} so callers need no branch. */
  public static Scope restore(@Nullable Binding binding) {
    return binding == null ? NO_SCOPE : install(binding);
  }

  /**
   * Declares a source's response shape. Emitted once per source occurrence, before its first
   * search, because the calculator cannot distinguish a paged source from a single-request one
   * afterwards.
   *
   * <p>Takes the binding explicitly rather than reading the thread-local: a scan holds its binding
   * for its whole life and reports from several threads, so requiring an installation just to emit
   * would mean installing and unwinding a scope around every single event.
   */
  public static void reportShape(@Nullable Binding binding, ProgressUnit unit, long pageSize) {
    if (binding == null) {
      return;
    }
    ProgressiveQueryContext context = binding.context();
    synchronized (context) {
      context.declaredUnits.put(binding.sourceId(), unit);
    }
    context.observer.accept(
        new SourceProgressEvent.SourceShapeObserved(binding.sourceId(), unit, pageSize));
  }

  /**
   * Declares a source finished.
   *
   * <p>Only normal exhaustion and an intentional upstream limit reach here. Cancellation and
   * failure must not, so an aborted query keeps the fraction it actually reached instead of
   * rounding its abandoned sources up to done.
   */
  public static void completeSource(@Nullable Binding binding, CompletionReason reason) {
    if (binding != null) {
      binding
          .context()
          .observer
          .accept(new SourceProgressEvent.SourceCompleted(binding.sourceId(), reason));
    }
  }

  /**
   * Opens a reporting channel for one OpenSearch search on the source bound to the current thread.
   *
   * <p>Returns {@link SourceChannel#NOOP} unless a source is bound, so the storage layer can report
   * unconditionally without testing whether this query is observed.
   */
  public static SourceChannel openChannel() {
    Binding binding = CURRENT.get();
    if (binding == null || binding.sourceId() == NO_SOURCE) {
      return SourceChannel.NOOP;
    }
    ProgressiveQueryContext context = binding.context();
    long sourceId = binding.sourceId();
    Map<SourceShardKey, Long> weights;
    ProgressUnit declaredUnit;
    synchronized (context) {
      weights = context.shardDocs.getOrDefault(sourceId, Map.of());
      declaredUnit = context.declaredUnits.getOrDefault(sourceId, ProgressUnit.SINGLE_REQUEST);
    }
    return new SourceChannel(
        context.observer, sourceId, context.requestIds.incrementAndGet(), weights, declaredUnit);
  }

  private static Scope install(Binding binding) {
    Binding previous = CURRENT.get();
    CURRENT.set(binding);
    return () -> {
      if (previous == null) {
        CURRENT.remove();
      } else {
        CURRENT.set(previous);
      }
    };
  }
}
