/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.dqe;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.adapter.enumerable.PhysTypeImpl;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.tree.Blocks;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rex.RexLiteral;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.opensearch.sql.calcite.plan.Scannable;
import org.opensearch.sql.calcite.plan.rel.LogicalSystemLimit;
import org.opensearch.sql.opensearch.dqe.agg.PartialAggregate;
import org.opensearch.sql.opensearch.dqe.agg.PartialAggregateSpec;
import org.opensearch.sql.opensearch.dqe.exchange.ConcatExchange;
import org.opensearch.sql.opensearch.dqe.exchange.Exchange;
import org.opensearch.sql.opensearch.dqe.exchange.MergeAggregateExchange;
import org.opensearch.sql.opensearch.dqe.exchange.MergeSortExchange;
import org.opensearch.sql.opensearch.planner.physical.CalciteEnumerableNestedAggregate;
import org.opensearch.sql.opensearch.storage.scan.AbstractCalciteIndexScan;

/**
 * Splits an optimized Calcite {@link RelNode} plan into coordinator and shard fragments by
 * inserting {@link Exchange} nodes at the shard/coordinator boundary.
 *
 * <p>The algorithm is a single post-order traversal. It classifies each operator as either
 * SHARD-local or COORDINATOR-side. When a COORDINATOR operator sits above a SHARD-local subtree,
 * an Exchange is inserted between them. The Exchange type depends on the coordinator operator:
 *
 * <ul>
 *   <li>Aggregate (decomposable) -> {@link MergeAggregateExchange}
 *   <li>Sort + Limit -> {@link MergeSortExchange}
 *   <li>Everything else -> {@link ConcatExchange}
 * </ul>
 *
 * <p>If the entire plan is shard-local (e.g., a simple filter query), the whole plan is wrapped in
 * a {@link ConcatExchange}.
 *
 * <p>If no OpenSearch scan (DSLScan or AbstractCalciteIndexScan) is found in the plan, the method
 * returns {@code null}, indicating no distribution is needed (e.g., system index queries).
 */
public final class PlanSplitter {

    private PlanSplitter() {}

    /**
     * Walk the plan bottom-up. Insert Exchange at shard/coordinator boundary. Returns
     * DistributedPlan with exchange nodes. If no distribution needed (e.g., system index query),
     * returns null.
     *
     * @param plan the optimized Calcite plan
     * @return a {@link DistributedPlan} or null if no distribution is needed
     */
    public static DistributedPlan split(RelNode plan) {
        if (!containsOpenSearchScan(plan)) {
            return null;
        }

        // Skip DQE for plans containing operators that require OpenSearch DSL pushdown
        // and cannot be executed in a distributed manner (e.g., nested aggregation).
        if (containsUnsupportedForDQE(plan)) {
            return null;
        }

        List<Exchange> exchanges = new ArrayList<>();
        RelNode coordinatorPlan = splitRecursive(plan, exchanges);

        // If the entire plan is shard-local (no exchanges inserted), wrap it in ConcatExchange
        if (exchanges.isEmpty() && isShardLocalSubtree(coordinatorPlan)) {
            ConcatExchange exchange = new ConcatExchange(coordinatorPlan, coordinatorPlan.getRowType());
            exchanges.add(exchange);
            coordinatorPlan = new ExchangeLeaf(
                    coordinatorPlan.getCluster(),
                    coordinatorPlan.getTraitSet(),
                    exchange);
        }

        return new DistributedPlan(coordinatorPlan, exchanges);
    }

    /**
     * Recursively processes the plan bottom-up. Returns a new plan where shard-local subtrees
     * below coordinator operators have been wrapped in Exchange nodes.
     */
    private static RelNode splitRecursive(RelNode node, List<Exchange> exchanges) {
        // Leaf node: return as-is (shard-local)
        if (node.getInputs().isEmpty()) {
            return node;
        }

        // For Join: each input is an independent subtree that may need its own Exchange
        if (node instanceof Join) {
            return handleJoin((Join) node, exchanges);
        }

        // For single-input operators: classify and recurse
        if (node.getInputs().size() == 1) {
            return handleSingleInput(node, exchanges);
        }

        // Multi-input (non-join): treat as coordinator, wrap each shard-local input
        return handleMultiInput(node, exchanges);
    }

    /**
     * Handles single-input operators (the common case). Classifies the current node and decides
     * whether to insert an Exchange between it and its child.
     */
    private static RelNode handleSingleInput(RelNode node, List<Exchange> exchanges) {
        RelNode child = node.getInputs().get(0);
        RelNode processedChild = splitRecursive(child, exchanges);

        if (isShardLocal(node)) {
            // Shard-local operator: just replace the child with the processed version
            return node.copy(node.getTraitSet(), List.of(processedChild));
        }

        // Coordinator operator: if the child subtree is shard-local, insert an Exchange
        if (isShardLocalSubtree(processedChild)) {
            Exchange exchange = createExchange(node, processedChild);
            exchanges.add(exchange);
            return replaceChildWithExchangeLeaf(node, exchange);
        }

        // Child already has exchanges or is coordinator-level; just rewire
        return node.copy(node.getTraitSet(), List.of(processedChild));
    }

    /**
     * Handles Join nodes. Each join input is processed independently. If an input is a shard-local
     * subtree, it gets wrapped in a ConcatExchange.
     */
    private static RelNode handleJoin(Join join, List<Exchange> exchanges) {
        List<RelNode> newInputs = new ArrayList<>();
        for (RelNode input : join.getInputs()) {
            RelNode processed = splitRecursive(input, exchanges);
            if (isShardLocalSubtree(processed)) {
                ConcatExchange exchange =
                        new ConcatExchange(processed, processed.getRowType());
                exchanges.add(exchange);
                newInputs.add(new ExchangeLeaf(
                        join.getCluster(), processed.getTraitSet(), exchange));
            } else {
                newInputs.add(processed);
            }
        }
        return join.copy(join.getTraitSet(), newInputs);
    }

    /**
     * Handles multi-input operators (other than Join). Each shard-local input gets wrapped in a
     * ConcatExchange.
     */
    private static RelNode handleMultiInput(RelNode node, List<Exchange> exchanges) {
        List<RelNode> newInputs = new ArrayList<>();
        for (RelNode input : node.getInputs()) {
            RelNode processed = splitRecursive(input, exchanges);
            if (isShardLocalSubtree(processed)) {
                ConcatExchange exchange =
                        new ConcatExchange(processed, processed.getRowType());
                exchanges.add(exchange);
                newInputs.add(new ExchangeLeaf(
                        node.getCluster(), processed.getTraitSet(), exchange));
            } else {
                newInputs.add(processed);
            }
        }
        return node.copy(node.getTraitSet(), newInputs);
    }

    /**
     * Creates the appropriate Exchange type based on the coordinator operator.
     *
     * <ul>
     *   <li>Aggregate: try decomposition via PartialAggregate. If decomposable,
     *       use MergeAggregateExchange with the partial aggregate as the shard plan.
     *       Otherwise, ConcatExchange (raw rows to coordinator).
     *   <li>Sort with limit (fetch != null): MergeSortExchange.
     *   <li>Everything else: ConcatExchange.
     * </ul>
     */
    private static Exchange createExchange(RelNode coordinatorOp, RelNode shardSubtree) {
        if (coordinatorOp instanceof Aggregate) {
            return createAggregateExchange((Aggregate) coordinatorOp, shardSubtree);
        }

        if (coordinatorOp instanceof Sort) {
            Sort sort = (Sort) coordinatorOp;
            if (sort.fetch != null) {
                return createMergeSortExchange(sort, shardSubtree);
            }
        }

        // Default: ConcatExchange
        return new ConcatExchange(shardSubtree, shardSubtree.getRowType());
    }

    /**
     * Creates an exchange for an Aggregate operator. Attempts partial aggregate decomposition. If
     * decomposable, creates a MergeAggregateExchange with the partial aggregate on shards. If not,
     * falls back to ConcatExchange (all raw rows to coordinator).
     */
    private static Exchange createAggregateExchange(Aggregate agg, RelNode shardSubtree) {
        Optional<PartialAggregateSpec> specOpt = PartialAggregate.decompose(agg);
        if (specOpt.isPresent()) {
            PartialAggregateSpec spec = specOpt.get();
            // Re-parent the shard aggregate onto the processed shard subtree
            Aggregate shardAgg = spec.getShardAggregate();
            Aggregate reparented = shardAgg.copy(
                    shardAgg.getTraitSet(),
                    shardSubtree,
                    shardAgg.getGroupSet(),
                    shardAgg.getGroupSets(),
                    shardAgg.getAggCallList());
            return new MergeAggregateExchange(
                    reparented,
                    agg.getRowType(),
                    spec.getMergeFunctions(),
                    spec.getGroupCount());
        }

        // Non-decomposable: send raw rows to coordinator
        return new ConcatExchange(shardSubtree, shardSubtree.getRowType());
    }

    /**
     * Creates a MergeSortExchange for a Sort+Limit pattern. Each shard will perform local TopK;
     * the coordinator merge-sorts the results.
     */
    private static Exchange createMergeSortExchange(Sort sort, RelNode shardSubtree) {
        int limit = extractLimitValue(sort.fetch);
        // The shard plan is the full Sort+Limit on top of the shard subtree
        Sort shardSort = sort.copy(
                sort.getTraitSet(),
                shardSubtree,
                sort.getCollation(),
                sort.offset,
                sort.fetch);
        return new MergeSortExchange(
                shardSort,
                sort.getRowType(),
                sort.getCollation().getFieldCollations(),
                limit);
    }

    /**
     * Replaces the coordinator node's child with an ExchangeLeaf that references the Exchange. For
     * Aggregate with MergeAggregateExchange, the coordinator plan keeps the original Aggregate
     * (which becomes the final aggregate). For Sort with MergeSortExchange, the coordinator does
     * not need the Sort operator since merge-sort handles ordering.
     */
    private static RelNode replaceChildWithExchangeLeaf(RelNode coordinatorOp, Exchange exchange) {
        ExchangeLeaf leaf = new ExchangeLeaf(
                coordinatorOp.getCluster(),
                coordinatorOp.getInputs().get(0).getTraitSet(),
                exchange);

        if (exchange instanceof MergeAggregateExchange) {
            // MergeAggregateExchange.scan() returns FULLY merged results.
            // No coordinator Aggregate is needed — it would re-aggregate incorrectly.
            return leaf;
        }

        if (exchange instanceof MergeSortExchange) {
            // MergeSortExchange handles the merge-sort and limit on the coordinator side.
            // The coordinator does not need the Sort node; the ExchangeLeaf replaces it.
            return leaf;
        }

        // ConcatExchange: the coordinator operator stays, its input is the ExchangeLeaf
        return coordinatorOp.copy(coordinatorOp.getTraitSet(), List.of(leaf));
    }

    /**
     * Classifies a node as shard-local. Shard-local operators are those that can execute
     * independently on each shard without cross-shard data dependencies.
     *
     * <p>Shard-local: Scan, Filter, Project, Calc (eval).
     *
     * <p>Coordinator: Aggregate, Sort (with or without limit), Window, Join, Dedup, and any
     * unrecognized operator (safe default).
     *
     * <p>Note: Sort+Limit is classified as COORDINATOR because it triggers a MergeSortExchange
     * which handles both the shard-side local TopK and the coordinator-side merge-sort. The shard
     * copy of Sort+Limit is embedded inside the MergeSortExchange's shard plan.
     */
    static boolean isShardLocal(RelNode node) {
        // Scan nodes
        if (node instanceof AbstractCalciteIndexScan) {
            return true;
        }
        // Filter
        if (node instanceof Filter) {
            return true;
        }
        // Project
        if (node instanceof Project) {
            return true;
        }
        // Calc (fused filter+project, row-by-row with no cross-shard dependency)
        if (node instanceof Calc) {
            return true;
        }
        // LogicalSystemLimit is a system safeguard (e.g., QUERY_SIZE_LIMIT = 10000).
        // It should be applied on each shard independently to cap per-shard output,
        // not treated as a user Sort+Limit that requires MergeSortExchange coordination.
        if (node instanceof LogicalSystemLimit) {
            return true;
        }
        // Everything else is coordinator (safe default)
        return false;
    }

    /**
     * Checks whether a subtree is entirely shard-local (i.e., has no Exchange nodes and all
     * operators are shard-local). An ExchangeLeaf in the tree means an Exchange was already
     * inserted, so the subtree is not purely shard-local.
     */
    private static boolean isShardLocalSubtree(RelNode node) {
        if (node instanceof ExchangeLeaf) {
            return false;
        }
        if (!isShardLocal(node) && !node.getInputs().isEmpty()) {
            return false;
        }
        for (RelNode child : node.getInputs()) {
            if (!isShardLocalSubtree(child)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Checks whether the plan contains operators that are unsupported under DQE and require
     * fallback to the legacy single-node execution path. Currently detected patterns:
     *
     * <ul>
     *   <li>{@link CalciteEnumerableNestedAggregate}: Requires OpenSearch nested aggregation
     *       pushdown which is not available in DQE mode. The operator's {@code implement()} method
     *       throws UnsupportedOperationException when pushdown is not applied.
     * </ul>
     */
    private static boolean containsUnsupportedForDQE(RelNode node) {
        if (node instanceof CalciteEnumerableNestedAggregate) {
            return true;
        }
        for (RelNode child : node.getInputs()) {
            if (containsUnsupportedForDQE(child)) {
                return true;
            }
        }
        return false;
    }

    /** Checks whether the plan contains at least one OpenSearch scan (DSLScan or similar). */
    private static boolean containsOpenSearchScan(RelNode node) {
        if (node instanceof AbstractCalciteIndexScan) {
            return true;
        }
        for (RelNode child : node.getInputs()) {
            if (containsOpenSearchScan(child)) {
                return true;
            }
        }
        return false;
    }

    /** Extracts the integer limit value from a Sort's fetch RexNode. */
    private static int extractLimitValue(org.apache.calcite.rex.RexNode fetch) {
        if (fetch instanceof RexLiteral) {
            Integer val = ((RexLiteral) fetch).getValueAs(Integer.class);
            if (val != null) {
                return val;
            }
        }
        throw new IllegalArgumentException("Cannot extract limit value from: " + fetch);
    }

    /**
     * A placeholder RelNode that represents an Exchange in the coordinator plan tree. Implements
     * {@link EnumerableRel} so that Calcite's code generation can execute coordinator plans that
     * have operators above ExchangeLeaf nodes (e.g., Join, Project). Also implements
     * {@link Scannable} for direct invocation without code generation.
     */
    static class ExchangeLeaf extends org.apache.calcite.rel.AbstractRelNode
            implements EnumerableRel, Scannable {
        private final Exchange exchange;

        ExchangeLeaf(
                org.apache.calcite.plan.RelOptCluster cluster,
                org.apache.calcite.plan.RelTraitSet traitSet,
                Exchange exchange) {
            super(cluster, traitSet.replace(org.apache.calcite.adapter.enumerable.EnumerableConvention.INSTANCE));
            this.rowType = exchange.getRowType();
            this.exchange = exchange;
        }

        public Exchange getExchange() {
            return exchange;
        }

        @Override
        protected org.apache.calcite.rel.type.RelDataType deriveRowType() {
            return rowType;
        }

        @Override
        public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
            PhysType physType =
                    PhysTypeImpl.of(
                            implementor.getTypeFactory(),
                            getRowType(),
                            pref.preferArray());

            Expression scanOperator = implementor.stash(this, ExchangeLeaf.class);
            return implementor.result(
                    physType,
                    Blocks.toBlock(Expressions.call(scanOperator, "scan")));
        }

        @Override
        public Enumerable<@Nullable Object> scan() {
            int columnCount = getRowType().getFieldCount();
            return new AbstractEnumerable<>() {
                @Override
                public Enumerator<@Nullable Object> enumerator() {
                    Iterator<Object[]> iterator = exchange.scan();
                    return new Enumerator<>() {
                        Object current;

                        @Override
                        public Object current() {
                            return current;
                        }

                        @Override
                        public boolean moveNext() {
                            if (iterator.hasNext()) {
                                Object[] row = iterator.next();
                                // Single-column rows are optimized to scalars by Calcite
                                current = (columnCount == 1) ? row[0] : row;
                                return true;
                            }
                            return false;
                        }

                        @Override
                        public void reset() {
                            throw new UnsupportedOperationException("reset not supported");
                        }

                        @Override
                        public void close() {}
                    };
                }
            };
        }
    }
}
