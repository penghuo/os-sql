/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.distributed;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import lombok.Data;

/**
 * A fragment represents a unit of work in a distributed query execution plan. Each fragment runs
 * independently on one or more nodes and communicates with other fragments through exchanges.
 *
 * <p>This class holds structural metadata only. The actual {@code PhysicalPlan} reference is added
 * in Phase 3 when physical planning is integrated with distributed execution.
 */
@Data
public class Fragment {

  /** Unique identifier for this fragment within the query plan. */
  private final int fragmentId;

  /** The execution locality of this fragment. */
  private final FragmentType type;

  /** Child fragments that feed data into this fragment via exchanges. */
  private final List<Fragment> children;

  /** The exchange specification describing how data arrives from child fragments. */
  private final ExchangeSpec exchangeSpec;

  /** The shard splits assigned to this fragment (only applicable for SOURCE fragments). */
  private final List<ShardSplit> splits;

  /** Estimated number of rows this fragment will produce. */
  private final long estimatedRows;

  /**
   * Creates a SOURCE fragment with assigned shard splits.
   *
   * @param fragmentId unique fragment identifier
   * @param splits shard splits to scan
   * @param estimatedRows estimated output row count
   * @return a source fragment
   */
  public static Fragment source(int fragmentId, List<ShardSplit> splits, long estimatedRows) {
    return new Fragment(
        fragmentId,
        FragmentType.SOURCE,
        Collections.emptyList(),
        null,
        splits,
        estimatedRows);
  }

  /**
   * Creates a SINGLE fragment (coordinator) that gathers data from child fragments.
   *
   * @param fragmentId unique fragment identifier
   * @param children child fragments feeding into this one
   * @param exchangeSpec how data is exchanged from children
   * @param estimatedRows estimated output row count
   * @return a single (coordinator) fragment
   */
  public static Fragment single(
      int fragmentId,
      List<Fragment> children,
      ExchangeSpec exchangeSpec,
      long estimatedRows) {
    return new Fragment(
        fragmentId,
        FragmentType.SINGLE,
        children,
        exchangeSpec,
        Collections.emptyList(),
        estimatedRows);
  }

  /**
   * Creates a HASH fragment for repartitioned processing.
   *
   * @param fragmentId unique fragment identifier
   * @param children child fragments feeding into this one
   * @param exchangeSpec how data is exchanged (must be HASH type)
   * @param estimatedRows estimated output row count
   * @return a hash-partitioned fragment
   */
  public static Fragment hash(
      int fragmentId,
      List<Fragment> children,
      ExchangeSpec exchangeSpec,
      long estimatedRows) {
    return new Fragment(
        fragmentId,
        FragmentType.HASH,
        children,
        exchangeSpec,
        Collections.emptyList(),
        estimatedRows);
  }

  /**
   * Returns a mutable copy of the children list for building plans incrementally.
   *
   * @return mutable list of child fragments
   */
  public List<Fragment> mutableChildren() {
    return new ArrayList<>(children);
  }
}
