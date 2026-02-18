/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.planner;

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.Window;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.sql.opensearch.planner.merge.ConcatExchange;
import org.opensearch.sql.opensearch.planner.merge.Exchange;
import org.opensearch.sql.opensearch.planner.merge.HashExchange;
import org.opensearch.sql.opensearch.planner.merge.MergeAggregateExchange;
import org.opensearch.sql.opensearch.planner.merge.MergeSortExchange;
import org.opensearch.sql.opensearch.planner.merge.PartialAggregate;
import org.opensearch.sql.opensearch.storage.scan.CalciteEnumerableIndexScan;

/**
 * Splits an optimized Calcite plan into shard-local and coordinator fragments by inserting Exchange
 * nodes at the boundary.
 *
 * <p>This replaces all 15 pushdown rules in {@link
 * org.opensearch.sql.opensearch.planner.rules.OpenSearchIndexRules}. Instead of translating RelNode
 * → DSL, the PlanSplitter keeps the Calcite plan intact and just marks where the shard/coordinator
 * boundary is.
 *
 * <p>Operator placement rules:
 *
 * <ul>
 *   <li>Scan → shard (CalciteLocalShardScan)
 *   <li>Filter → shard (Calcite Filter or Lucene via PredicateAnalyzer)
 *   <li>Project/Eval → shard (row-level, no cross-shard dependency)
 *   <li>Aggregate → split into PartialAggregate (shard) + MergeAggregateExchange (coordinator)
 *   <li>Sort + Limit → shard (local TopK) + MergeSortExchange (coordinator)
 *   <li>Sort (no limit) → ConcatExchange then Sort on coordinator
 *   <li>Window (with PARTITION BY) → HashExchange by partition keys, local window on each partition
 *   <li>Window (no PARTITION BY) → ConcatExchange then Window on coordinator
 *   <li>Join (equi-join) → HashExchange both sides by join keys, local join per partition
 *   <li>Join (non-equi) → ConcatExchange both inputs, join on coordinator
 *   <li>Dedup → coordinator (needs global view)
 * </ul>
 */
public class PlanSplitter {
    private static final Logger LOG = LogManager.getLogger(PlanSplitter.class);

    /**
     * Splits the optimized plan by inserting Exchange nodes at the shard/coordinator boundary.
     *
     * @param plan the optimized RelNode plan
     * @return the plan with Exchange nodes inserted
     */
    public RelNode split(RelNode plan) {
        return plan.accept(new SplitShuttle());
    }

    /**
     * A RelShuttle that walks the plan bottom-up and inserts Exchange nodes. The strategy is:
     *
     * <ol>
     *   <li>Walk children first (bottom-up)
     *   <li>When we encounter a node whose child is a scan or shard-local operator, check if this
     *       node should be on the coordinator
     *   <li>If so, insert an Exchange between the coordinator node and its shard-local children
     * </ol>
     */
    private static class SplitShuttle extends RelShuttleImpl {

        @Override
        public RelNode visit(RelNode other) {
            // Walk children first (bottom-up)
            RelNode visited = super.visit(other);

            // If this is a scan, it stays on shard — no exchange needed
            if (isScanNode(visited)) {
                return visited;
            }

            // Filter: stays on shard, no exchange
            if (visited instanceof Filter) {
                return visited;
            }

            // Project: stays on shard, no exchange
            if (visited instanceof Project) {
                return visited;
            }

            // Aggregate: split into PartialAggregate + MergeAggregateExchange
            if (visited instanceof Aggregate aggregate) {
                return splitAggregate(aggregate);
            }

            // Sort: depends on whether there's a limit (TopK) or not
            if (visited instanceof Sort sort) {
                return splitSort(sort);
            }

            // Window: shuffle by partition keys if present, otherwise gather to coordinator
            if (visited instanceof Window window) {
                return splitWindow(window);
            }

            // Join: equi-join uses HashExchange, non-equi falls back to ConcatExchange
            if (visited instanceof Join join) {
                return splitJoin(join);
            }

            // Default: if children are shard-local and this is a coordinator op, wrap
            return visited;
        }

        /**
         * Splits an Aggregate into PartialAggregate (shard) + MergeAggregateExchange (coordinator).
         */
        private RelNode splitAggregate(Aggregate aggregate) {
            // Try to create a partial aggregate
            PartialAggregate partial = PartialAggregate.create(aggregate);
            if (partial == null) {
                // Non-decomposable: run full agg on coordinator after gathering all data
                return ensureExchangeBelow(aggregate);
            }

            // Insert ConcatExchange below the partial agg (between scan and partial agg)
            // Then MergeAggregateExchange above to merge partial results
            return MergeAggregateExchange.create(
                    partial, aggregate.getGroupSet(), aggregate.getAggCallList());
        }

        /**
         * Splits a Sort. If it has a limit (TopK), keep the sort+limit on shards and add a
         * MergeSortExchange on coordinator. Otherwise, ConcatExchange then sort on coordinator.
         */
        private RelNode splitSort(Sort sort) {
            if (sort.fetch != null) {
                // TopK: keep sort on shard, merge-sort on coordinator
                return MergeSortExchange.create(
                        sort, sort.getCollation(), sort.offset, sort.fetch);
            } else {
                // Sort without limit: gather all data then sort on coordinator
                // Insert ConcatExchange below the sort
                return ensureExchangeBelow(sort);
            }
        }

        /**
         * Splits a Join. For equi-joins, uses HashExchange on both inputs by their respective
         * join keys so each partition can perform a local join independently. For non-equi-joins,
         * falls back to gathering both inputs to the coordinator.
         */
        private RelNode splitJoin(Join join) {
            // Try to extract equi-join keys
            List<Integer> leftKeys = new ArrayList<>();
            List<Integer> rightKeys = new ArrayList<>();
            List<Boolean> filterNulls = new ArrayList<>();
            RelOptUtil.splitJoinCondition(
                    join.getLeft(),
                    join.getRight(),
                    join.getCondition(),
                    leftKeys,
                    rightKeys,
                    filterNulls);

            if (!leftKeys.isEmpty()) {
                // Equi-join: use HashExchange by join keys
                return splitEquiJoin(join, leftKeys, rightKeys);
            }

            // Non-equi-join: fall back to gather-to-coordinator
            return splitNonEquiJoin(join);
        }

        /**
         * Splits an equi-join by inserting HashExchange on both inputs by their respective join
         * keys, then wrapping the result with a ConcatExchange to concatenate partition results
         * on the coordinator.
         */
        private RelNode splitEquiJoin(
                Join join, List<Integer> leftKeys, List<Integer> rightKeys) {
            RelNode left = join.getLeft();
            RelNode right = join.getRight();

            int numPartitions = 8; // Default partition count; will be configurable

            // Insert HashExchange on both sides by their respective join keys
            if (needsExchange(left)) {
                left = HashExchange.create(left, leftKeys, numPartitions);
            }
            if (needsExchange(right)) {
                right = HashExchange.create(right, rightKeys, numPartitions);
            }

            // Local join on each partition
            RelNode localJoin = join.copy(join.getTraitSet(), List.of(left, right));

            // Coordinator concatenates partition results
            return ConcatExchange.create(localJoin);
        }

        /**
         * Splits a non-equi-join by gathering both inputs to the coordinator via ConcatExchange,
         * then performing the join on the coordinator.
         */
        private RelNode splitNonEquiJoin(Join join) {
            RelNode left = join.getLeft();
            RelNode right = join.getRight();

            if (!(left instanceof Exchange) && needsExchange(left)) {
                left = ConcatExchange.create(left);
            }
            if (!(right instanceof Exchange) && needsExchange(right)) {
                right = ConcatExchange.create(right);
            }

            return join.copy(join.getTraitSet(), List.of(left, right));
        }

        /**
         * Splits a Window. If the window has PARTITION BY keys, shuffles data by those keys
         * using HashExchange so each partition can compute the window function independently.
         * Without PARTITION BY, gathers all data to the coordinator.
         */
        private RelNode splitWindow(Window window) {
            List<Integer> partitionKeys = extractPartitionKeys(window);

            if (partitionKeys.isEmpty()) {
                // No PARTITION BY: need global view, gather all data to coordinator
                return ensureExchangeBelow(window);
            }

            // Has PARTITION BY: shuffle by partition keys, then run window locally
            RelNode input = window.getInput();
            if (needsExchange(input)) {
                int numPartitions = 8; // Default; will be configurable
                input = HashExchange.create(input, partitionKeys, numPartitions);
                return window.copy(window.getTraitSet(), List.of(input));
            }
            return window;
        }

        /**
         * Extracts partition keys from all window groups. Each {@link Window.Group} has a keys
         * field ({@link org.apache.calcite.util.ImmutableBitSet}) representing the PARTITION BY
         * columns.
         */
        private List<Integer> extractPartitionKeys(Window window) {
            List<Integer> partitionKeys = new ArrayList<>();
            for (Window.Group group : window.groups) {
                for (int key : group.keys) {
                    if (!partitionKeys.contains(key)) {
                        partitionKeys.add(key);
                    }
                }
            }
            return partitionKeys;
        }

        /**
         * Wraps the single child of a SingleRel with a ConcatExchange if needed.
         */
        private RelNode ensureExchangeBelow(SingleRel node) {
            RelNode input = node.getInput();
            if (input instanceof Exchange) {
                return node;
            }
            if (needsExchange(input)) {
                RelNode exchanged = ConcatExchange.create(input);
                return node.copy(node.getTraitSet(), List.of(exchanged));
            }
            return node;
        }

        /** Checks if a node is a scan node (leaf that accesses data). */
        private static boolean isScanNode(RelNode node) {
            return node instanceof CalciteEnumerableIndexScan;
        }

        /**
         * Determines if a node contains shard-local operators that need an Exchange above them to
         * gather results to the coordinator.
         */
        private static boolean needsExchange(RelNode node) {
            if (node instanceof Exchange) {
                return false;
            }
            if (isScanNode(node)) {
                return true;
            }
            if (node instanceof Filter || node instanceof Project) {
                return true;
            }
            if (node instanceof PartialAggregate) {
                return true;
            }
            if (node instanceof Sort) {
                return true;
            }
            // Check children
            for (RelNode child : node.getInputs()) {
                if (needsExchange(child)) {
                    return true;
                }
            }
            return false;
        }
    }
}
