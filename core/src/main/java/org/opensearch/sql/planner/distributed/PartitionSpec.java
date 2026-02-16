/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.distributed;

import java.util.List;
import lombok.Data;

/**
 * Describes how data is partitioned across nodes. Used by HASH exchanges and hash-based
 * aggregation or join fragments.
 */
@Data
public class PartitionSpec {

  /** The keys used for partitioning. */
  private final List<String> partitionKeys;

  /** The number of partitions. */
  private final int numPartitions;
}
