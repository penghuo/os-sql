/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.progress;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.calcite.adapter.enumerable.EnumerableLimit;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;

/**
 * An {@link EnumerableLimit} that reports source completion when it reaches its own output quota.
 *
 * <h2>Why a plan node</h2>
 *
 * §8.5 of the design makes an intentional upstream limit one of the three things that completes a
 * source. Only the limit knows when that has happened: a scan separated from it by a filter cannot
 * infer a row budget (two raw rows may yield fewer than two qualifying rows), and a close cannot
 * distinguish a deliberate stop from a failure's cleanup. So the signal has to come from the limit
 * itself.
 *
 * <h2>Behaviour is identical to the node it replaces</h2>
 *
 * <ul>
 *   <li><b>Rows and control flow:</b> {@link #implement} delegates to {@link
 *       EnumerableLimit#implement}, so Calcite's own skip/take code produces the rows. This only
 *       wraps the resulting enumerable in a counter.
 *   <li><b>Cost and traits:</b> nothing is overridden. The node is constructed with the same
 *       cluster, trait set, input, offset, and fetch, and inherits the parent's cost model, so plan
 *       choice cannot shift.
 *   <li><b>Cleanup:</b> the counter delegates {@code close()} and reports nothing from it, so an
 *       exception or cancellation unwinds exactly as before.
 * </ul>
 *
 * <h2>Asynchronous queries only</h2>
 *
 * Substitution happens after optimization and only when the query is observed, so synchronous
 * execution never sees this node and its plans, costs, and explain output are untouched.
 */
final class ProgressAwareEnumerableLimit extends EnumerableLimit {

  private final ProgressiveQueryContext progressContext;

  private ProgressAwareEnumerableLimit(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode input,
      @Nullable RexNode offset,
      @Nullable RexNode fetch,
      ProgressiveQueryContext progressContext) {
    super(cluster, traitSet, input, offset, fetch);
    this.progressContext = Objects.requireNonNull(progressContext, "progressContext");
  }

  /**
   * Returns a progress-reporting equivalent of {@code limit} over {@code input}.
   *
   * <p>Built from the original node's own cluster, traits, offset, and fetch, so the replacement is
   * indistinguishable to the planner.
   */
  static ProgressAwareEnumerableLimit of(
      EnumerableLimit limit, RelNode input, ProgressiveQueryContext progressContext) {
    return new ProgressAwareEnumerableLimit(
        limit.getCluster(), limit.getTraitSet(), input, limit.offset, limit.fetch, progressContext);
  }

  @Override
  public ProgressAwareEnumerableLimit copy(RelTraitSet traitSet, List<RelNode> newInputs) {
    return new ProgressAwareEnumerableLimit(
        getCluster(),
        traitSet,
        newInputs.isEmpty() ? getInput() : newInputs.get(0),
        offset,
        fetch,
        progressContext);
  }

  @Override
  public Result implement(EnumerableRelImplementor implementor, EnumerableRel.Prefer pref) {
    // Snapshot before delegating: the child subtree is code-generated inside super.implement(), and
    // that is when
    // the scans beneath this limit claim their source ids. Diffing the claim set is exact and needs
    // no second
    // traversal — it also naturally excludes sources that belong to a sibling branch.
    Set<Long> claimedBefore = progressContext.claimedSourceIds();
    Result base = super.implement(implementor, pref);
    Set<Long> beneath = new LinkedHashSet<>(progressContext.claimedSourceIds());
    beneath.removeAll(claimedBefore);

    OptionalLong quota = literalValue(fetch);
    if (beneath.isEmpty() || quota.isEmpty()) {
      // Nothing beneath to report, or a limit whose fetch is not a constant so its quota is
      // unknowable. Hand back
      // Calcite's code untouched. A fetch of zero is NOT excluded here: a zero-row limit that is
      // actually visited is
      // already at its intentional quota, and skipping it would leave the work beneath it pending.
      return base;
    }

    Expression signal =
        implementor.stash(
            new ProgressLimitSignal(progressContext, new ArrayList<>(beneath), quota.getAsLong()),
            ProgressLimitSignal.class);

    // Append the child's whole block rather than reducing it to a single expression.
    // EnumerableLimit's own code is
    // a bare return today, but a child implementation is free to leave variable declarations in the
    // block, and
    // Blocks.simple would throw an AssertionError on one — which no catch here should be relied on
    // to absorb.
    // Appending keeps every declaration and yields the block's value as an expression.
    BlockBuilder builder = new BlockBuilder();
    Expression limited = builder.append("limited", base.block);
    builder.add(Expressions.return_(null, Expressions.call(signal, "observe", limited)));
    return implementor.result(base.physType, builder.toBlock());
  }

  /** Non-negative literal value of {@code node}, or empty when it is absent or not a constant. */
  private static OptionalLong literalValue(@Nullable RexNode node) {
    if (node instanceof RexLiteral literal && literal.getValue() != null) {
      Long value = literal.getValueAs(Long.class);
      if (value != null && value >= 0L) {
        return OptionalLong.of(value);
      }
    }
    return OptionalLong.empty();
  }
}
