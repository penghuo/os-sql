/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.distributed;

/**
 * Defines the execution locality of a query fragment in the distributed execution plan.
 *
 * <p>Each fragment in a distributed query plan has a type that determines how it is scheduled across
 * the cluster:
 *
 * <ul>
 *   <li>{@link #SOURCE} – Runs on every shard that owns data for the queried index. Typically the
 *       leaf fragment that performs scans, filters, and partial aggregations.
 *   <li>{@link #HASH} – Runs on a set of nodes determined by hash-partitioning on one or more
 *       keys. Used for repartitioned joins and hash-aggregations.
 *   <li>{@link #SINGLE} – Runs on exactly one node (the coordinator). Used for final aggregation,
 *       global sort/limit, and result assembly.
 * </ul>
 */
public enum FragmentType {

  /** Fragment executes on every relevant shard. */
  SOURCE,

  /** Fragment executes on nodes determined by hash-partitioning. */
  HASH,

  /** Fragment executes on a single coordinator node. */
  SINGLE
}
