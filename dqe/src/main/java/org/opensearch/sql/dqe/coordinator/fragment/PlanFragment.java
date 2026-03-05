/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.coordinator.fragment;

import org.opensearch.sql.dqe.planner.plan.DqePlanNode;

/** Represents a plan fragment assigned to a specific shard on a specific node. */
public record PlanFragment(DqePlanNode shardPlan, String indexName, int shardId, String nodeId) {}
