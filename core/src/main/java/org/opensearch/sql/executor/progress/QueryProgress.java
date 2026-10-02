/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.executor.progress;

/**
 * Immutable, client-visible progress snapshot for one query.
 *
 * <p>{@code fractionDone} is the only reported quantity. It is deliberately coarse: it is derived
 * from shard callbacks, page counts, and a pre-execution index-size estimate, so it answers
 * "roughly how far along is this query" and never claims to be a row count or a share of the final
 * result.
 *
 * <p>Invariants enforced here and relied on by the REST layer:
 *
 * <ul>
 *   <li>{@code fractionDone} is finite — never {@code NaN} and never infinite;
 *   <li>{@code fractionDone} lies in {@code [0.0, 1.0]}.
 * </ul>
 *
 * <p>Monotonicity, and the rule that a running query never exceeds {@link #PUBLIC_CEILING}, are
 * properties of {@link ProgressiveSourceProgress} — a snapshot on its own cannot see the previous
 * one.
 *
 * @param fractionDone completed fraction of the query, in {@code [0.0, 1.0]}
 */
public record QueryProgress(double fractionDone) {

  /** Nothing observed yet. Reported while a job is admitted but has registered no source work. */
  public static final QueryProgress ZERO = new QueryProgress(0.0);

  /** Terminal success. Reported only once a job reaches {@code SUCCEEDED}. */
  public static final QueryProgress COMPLETE = new QueryProgress(1.0);

  /**
   * Highest fraction a running query may publish: source work is scaled into {@code [0.0, 0.8]} and
   * the top 20% is reserved.
   *
   * <p>The reserve is not an estimate of coordinator progress. It exists so that draining every
   * source cannot report query completion while blocking coordinator work — a sort, a join build, a
   * non-pushed aggregation — may still remain. A running query is allowed to sit at {@code 0.8}
   * indefinitely; only the terminal status says the result is ready.
   */
  public static final QueryProgress PUBLIC_CEILING = new QueryProgress(0.8);

  public QueryProgress {
    if (!Double.isFinite(fractionDone)) {
      throw new IllegalArgumentException("fractionDone must be finite, got " + fractionDone);
    }
    if (fractionDone < 0.0 || fractionDone > 1.0) {
      throw new IllegalArgumentException(
          "fractionDone must be within [0.0, 1.0], got " + fractionDone);
    }
  }
}
