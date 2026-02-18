/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.planner.plan;

import java.util.List;

/**
 * Describes how data is partitioned across nodes in a stage. In Phase 1, only SINGLE and
 * COORDINATOR_ONLY partitioning are used (scatter-gather).
 */
public class PartitioningScheme {

  /** How data is distributed across nodes. */
  public enum Partitioning {
    /** Data distributed across shard-holding nodes. */
    SOURCE_DISTRIBUTED,
    /** All data gathered to coordinator. */
    COORDINATOR_ONLY,
    /** Single node execution (no distribution). */
    SINGLE
  }

  private final Partitioning partitioning;
  private final List<Integer> partitionColumns;

  public PartitioningScheme(Partitioning partitioning) {
    this(partitioning, List.of());
  }

  public PartitioningScheme(Partitioning partitioning, List<Integer> partitionColumns) {
    this.partitioning = partitioning;
    this.partitionColumns = List.copyOf(partitionColumns);
  }

  public Partitioning getPartitioning() {
    return partitioning;
  }

  public List<Integer> getPartitionColumns() {
    return partitionColumns;
  }

  /** Creates a GATHER partitioning scheme (all data to coordinator). */
  public static PartitioningScheme gatherPartitioning() {
    return new PartitioningScheme(Partitioning.COORDINATOR_ONLY);
  }

  /** Creates a SOURCE_DISTRIBUTED partitioning scheme. */
  public static PartitioningScheme sourceDistributed() {
    return new PartitioningScheme(Partitioning.SOURCE_DISTRIBUTED);
  }

  /** Creates a SINGLE partitioning scheme. */
  public static PartitioningScheme singlePartitioning() {
    return new PartitioningScheme(Partitioning.SINGLE);
  }

  @Override
  public String toString() {
    return "PartitioningScheme{" + partitioning + "}";
  }
}
