/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.progress;

import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.calcite.adapter.enumerable.EnumerableLimit;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Swaps every {@link EnumerableLimit} in an optimized plan for a progress-reporting equivalent.
 *
 * <h2>Why a substitution rather than a planner rule</h2>
 *
 * A rule producing an equal-cost alternative leaves the choice to the planner, so the instrumented
 * node might simply not be picked; forcing it would mean lying about cost and risking plan quality
 * everywhere else. Rewriting the already-chosen plan is deterministic and cannot change which plan
 * was chosen, because the plan was already chosen. The replacement carries the original's cluster,
 * traits, input, offset, and fetch and inherits its cost model, so the re-registration Calcite
 * performs while preparing the statement has nothing to prefer — and no rule produces a plain
 * {@code EnumerableLimit} from this node, since it is already physical and is not a logical sort.
 *
 * <h2>Asynchronous queries only</h2>
 *
 * Applied only when the query is observed. Synchronous execution never sees an instrumented node,
 * so its plans, costs, and explain output are untouched by construction rather than by careful
 * matching.
 */
public final class ProgressLimitInstrumentation {

  private static final Logger LOG = LogManager.getLogger(ProgressLimitInstrumentation.class);

  private ProgressLimitInstrumentation() {
    throw new AssertionError(
        ProgressLimitInstrumentation.class.getCanonicalName()
            + " is a utility class and must not be initialized");
  }

  /**
   * Returns {@code plan} with its limits instrumented, or {@code plan} itself when the query is not
   * observed.
   *
   * <p>Never throws: a plan shape the rewriter does not understand costs progress resolution, which
   * must never cost the query.
   */
  public static RelNode instrument(RelNode plan, @Nullable ProgressiveQueryContext context) {
    if (context == null || plan == null) {
      return plan;
    }
    try {
      return rewrite(plan, context);
    } catch (RuntimeException e) {
      LOG.debug("Could not instrument plan limits; limit-driven progress will be unavailable", e);
      return plan;
    }
  }

  private static RelNode rewrite(RelNode node, ProgressiveQueryContext context) {
    List<RelNode> inputs = node.getInputs();
    List<RelNode> rewrittenInputs = new ArrayList<>(inputs.size());
    boolean inputsChanged = false;
    for (RelNode input : inputs) {
      RelNode rewritten = rewrite(input, context);
      inputsChanged |= rewritten != input;
      rewrittenInputs.add(rewritten);
    }

    // Limits held inside expressions — an uncorrelated subquery's own `head` — are reached through
    // the expression
    // tree, not through getInputs().
    RelNode withSubQueries = rewriteSubQueries(node, context);
    boolean selfChanged = withSubQueries != node;
    RelNode current = withSubQueries;
    if (inputsChanged) {
      current = current.copy(current.getTraitSet(), rewrittenInputs);
      selfChanged = true;
    }

    if (current instanceof ProgressAwareEnumerableLimit) {
      return current;
    }
    if (current instanceof EnumerableLimit limit) {
      return ProgressAwareEnumerableLimit.of(limit, limit.getInput(), context);
    }
    return selfChanged ? current : node;
  }

  private static RelNode rewriteSubQueries(RelNode node, ProgressiveQueryContext context) {
    return node.accept(
        new RexShuttle() {
          @Override
          public RexNode visitSubQuery(RexSubQuery subQuery) {
            RelNode rewritten = rewrite(subQuery.rel, context);
            return rewritten == subQuery.rel ? subQuery : subQuery.clone(rewritten);
          }
        });
  }
}
