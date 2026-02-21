/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.dqe;

import java.util.List;
import lombok.Value;
import org.apache.calcite.rel.RelNode;
import org.opensearch.sql.opensearch.dqe.exchange.Exchange;

/**
 * Represents a distributed query plan produced by {@link PlanSplitter}.
 *
 * <p>Contains the coordinator-side plan (which references Exchange nodes) and the list of all
 * Exchange nodes inserted during splitting. The executor uses the exchanges to dispatch shard
 * fragments and collect results.
 */
@Value
public class DistributedPlan {
  /** The coordinator-side plan. Contains Exchange nodes as leaves where shard execution begins. */
  RelNode coordinatorPlan;

  /** All Exchange nodes in the plan, in the order they were created during splitting. */
  List<Exchange> exchanges;
}
