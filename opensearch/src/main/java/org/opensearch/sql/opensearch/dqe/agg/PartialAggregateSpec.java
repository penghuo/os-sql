/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.dqe.agg;

import java.util.List;
import lombok.Value;
import org.apache.calcite.rel.core.Aggregate;
import org.opensearch.sql.opensearch.dqe.exchange.MergeFunction;

/**
 * Describes a decomposed aggregate: a shard-side partial aggregate and the merge functions needed
 * to combine partial results on the coordinator.
 */
@Value
public class PartialAggregateSpec {
  /** The partial aggregate to run on each shard. */
  Aggregate shardAggregate;

  /** One merge function per original aggregate call, describing how to combine shard results. */
  List<MergeFunction> mergeFunctions;

  /** The number of GROUP BY keys in the aggregate. */
  int groupCount;
}
