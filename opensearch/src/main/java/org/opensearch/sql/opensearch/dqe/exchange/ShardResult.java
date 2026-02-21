/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.dqe.exchange;

import java.util.List;
import lombok.Value;
import org.apache.calcite.rel.type.RelDataType;

/** Represents the output from a single shard's execution of a plan fragment. */
@Value
public class ShardResult {
  /** The shard ID that produced this result. */
  int shardId;

  /** The rows returned by this shard. Each row is an Object array matching {@link #rowType}. */
  List<Object[]> rows;

  /** The row type describing the schema of each row. */
  RelDataType rowType;
}
