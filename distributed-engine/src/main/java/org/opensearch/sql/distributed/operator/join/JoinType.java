/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.operator.join;

/**
 * Supported join types for the distributed join operator. Ported from Trino's join type semantics.
 */
public enum JoinType {
  /** Returns only rows where both sides match on the join key. */
  INNER,

  /** Returns all probe (left) rows; unmatched rows get nulls for build (right) columns. */
  LEFT,

  /** Returns all build (right) rows; unmatched rows get nulls for probe (left) columns. */
  RIGHT,

  /** Returns all rows from both sides; unmatched rows get nulls for the other side. */
  FULL,

  /** Returns probe rows that have at least one match. No build columns in output. */
  SEMI,

  /** Returns probe rows that have NO match. No build columns in output. */
  ANTI;

  /** Returns true if unmatched build-side rows should be emitted at finish. */
  public boolean outputUnmatchedBuild() {
    return this == RIGHT || this == FULL;
  }

  /** Returns true if unmatched probe-side rows should be emitted (with null build columns). */
  public boolean outputUnmatchedProbe() {
    return this == LEFT || this == FULL;
  }

  /** Returns true if build columns should appear in the output. */
  public boolean outputBuildColumns() {
    return this != SEMI && this != ANTI;
  }
}
