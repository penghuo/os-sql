/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.planner.merge;

import java.util.List;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexNode;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Exchange operator that concatenates results from distributed hash join partitions.
 *
 * <p>After both sides of a join are shuffled by join key via {@link HashExchange}, each partition
 * performs a local join independently. This exchange simply concatenates the results from all
 * partitions, since each partition's join results are independent.
 */
public class LocalJoinExchange extends Exchange {

    private final JoinRelType joinType;
    private final RexNode condition;

    public LocalJoinExchange(
            RelOptCluster cluster,
            RelTraitSet traits,
            RelNode input,
            JoinRelType joinType,
            RexNode condition) {
        super(cluster, traits, input);
        this.joinType = joinType;
        this.condition = condition;
    }

    /** Convenience factory that creates a LocalJoinExchange with enumerable convention traits. */
    public static LocalJoinExchange create(
            RelNode input, JoinRelType joinType, RexNode condition) {
        RelOptCluster cluster = input.getCluster();
        return new LocalJoinExchange(
                cluster, enumerableTraitSet(cluster), input, joinType, condition);
    }

    @Override
    public String getExchangeType() {
        return "LOCAL_JOIN";
    }

    public JoinRelType getJoinType() {
        return joinType;
    }

    public RexNode getCondition() {
        return condition;
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw)
                .item("joinType", joinType)
                .item("condition", condition);
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new LocalJoinExchange(
                getCluster(), traitSet, sole(inputs), joinType, condition);
    }

    @Override
    public Enumerable<@Nullable Object> scan() {
        // Full implementation will be wired in T8 when shard dispatch is available
        throw new UnsupportedOperationException("LocalJoinExchange.scan() not yet implemented");
    }
}
