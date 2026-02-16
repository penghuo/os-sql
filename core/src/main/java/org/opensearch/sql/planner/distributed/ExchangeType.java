/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.distributed;

/**
 * Defines the data distribution strategy used by an exchange between two fragments.
 *
 * <ul>
 *   <li>{@link #GATHER} – All partitions send their data to a single destination (the
 *       coordinator). Used after partial aggregation or for simple result collection.
 *   <li>{@link #HASH} – Rows are hash-partitioned on one or more keys and sent to the
 *       corresponding destination partition. Used before hash joins or hash aggregations.
 *   <li>{@link #BROADCAST} – Every row is replicated to all destination partitions. Used for small
 *       dimension tables in broadcast joins.
 *   <li>{@link #ROUND_ROBIN} – Rows are distributed evenly across destination partitions in a
 *       round-robin fashion. Used for load-balanced parallel processing.
 * </ul>
 */
public enum ExchangeType {

  /** All data flows to a single destination. */
  GATHER,

  /** Data is hash-partitioned by key(s). */
  HASH,

  /** Every row is replicated to all destinations. */
  BROADCAST,

  /** Rows are distributed evenly in round-robin order. */
  ROUND_ROBIN
}
