/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.planner.merge;

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.ImmutableBitSet;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Exchange operator that merges partial aggregate states from multiple shards.
 *
 * <p>Each shard computes partial aggregates (e.g., partial SUM, partial COUNT). The coordinator
 * merges these using function-specific logic:
 *
 * <ul>
 *   <li>SUM: sum of partial sums
 *   <li>COUNT: sum of partial counts
 *   <li>AVG: sum(partial_sums) / sum(partial_counts)
 *   <li>MIN: min of partial mins
 *   <li>MAX: max of partial maxes
 *   <li>STDDEV/VAR: Welford merge of (count, sum, sumOfSquares) partial columns
 *   <li>DISTINCT_COUNT_APPROX: HLL binary state merge
 *   <li>PERCENTILE_APPROX: t-digest binary state merge
 * </ul>
 */
public class MergeAggregateExchange extends Exchange {

    /**
     * Tracks the merge strategy for each original aggregate call.
     */
    public enum MergeStrategy {
        /** Simple function merge (SUM, COUNT, MIN, MAX). */
        SIMPLE,
        /** Welford merge for STDDEV/VAR (uses 3 partial columns: count, sum, sumSquares). */
        WELFORD,
        /** Binary state merge for HLL/t-digest (uses serialized binary state). */
        BINARY_STATE
    }

    private final ImmutableBitSet groupSet;
    private final ImmutableList<AggregateCall> aggCalls;
    private final ImmutableList<MergeStrategy> mergeStrategies;

    public MergeAggregateExchange(
            RelOptCluster cluster,
            RelTraitSet traits,
            RelNode input,
            ImmutableBitSet groupSet,
            List<AggregateCall> aggCalls,
            List<MergeStrategy> mergeStrategies) {
        super(cluster, traits, input);
        this.groupSet = groupSet;
        this.aggCalls = ImmutableList.copyOf(aggCalls);
        this.mergeStrategies = ImmutableList.copyOf(mergeStrategies);
    }

    /**
     * Legacy constructor that computes merge strategies automatically from the aggregate calls.
     */
    public MergeAggregateExchange(
            RelOptCluster cluster,
            RelTraitSet traits,
            RelNode input,
            ImmutableBitSet groupSet,
            List<AggregateCall> aggCalls) {
        this(cluster, traits, input, groupSet, aggCalls, computeMergeStrategies(aggCalls));
    }

    /**
     * Convenience factory that creates a MergeAggregateExchange with enumerable convention traits.
     */
    public static MergeAggregateExchange create(
            RelNode input, ImmutableBitSet groupSet, List<AggregateCall> aggCalls) {
        RelOptCluster cluster = input.getCluster();
        ImmutableList<MergeStrategy> strategies = computeMergeStrategies(aggCalls);
        return new MergeAggregateExchange(
                cluster, enumerableTraitSet(cluster), input, groupSet, aggCalls, strategies);
    }

    /**
     * Computes the merge strategy for each aggregate call based on its kind and name.
     */
    private static ImmutableList<MergeStrategy> computeMergeStrategies(
            List<AggregateCall> aggCalls) {
        ImmutableList.Builder<MergeStrategy> builder = ImmutableList.builder();
        for (AggregateCall call : aggCalls) {
            SqlKind kind = call.getAggregation().getKind();
            if (kind == SqlKind.STDDEV_SAMP || kind == SqlKind.STDDEV_POP
                    || kind == SqlKind.VAR_SAMP || kind == SqlKind.VAR_POP) {
                builder.add(MergeStrategy.WELFORD);
            } else {
                String name = call.getAggregation().getName();
                if ("DISTINCT_COUNT_APPROX".equalsIgnoreCase(name)
                        || "PERCENTILE_APPROX".equalsIgnoreCase(name)) {
                    builder.add(MergeStrategy.BINARY_STATE);
                } else {
                    builder.add(MergeStrategy.SIMPLE);
                }
            }
        }
        return builder.build();
    }

    @Override
    public String getExchangeType() {
        return "MERGE_AGGREGATE";
    }

    public ImmutableBitSet getGroupSet() {
        return groupSet;
    }

    public ImmutableList<AggregateCall> getAggCalls() {
        return aggCalls;
    }

    public ImmutableList<MergeStrategy> getMergeStrategies() {
        return mergeStrategies;
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw)
                .item("groupSet", groupSet)
                .item("aggCalls", aggCalls)
                .item("mergeStrategies", mergeStrategies);
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new MergeAggregateExchange(
                getCluster(), traitSet, sole(inputs), groupSet, aggCalls, mergeStrategies);
    }

    @Override
    public Enumerable<@Nullable Object> scan() {
        // Full implementation will be wired in T8 when shard dispatch is available
        throw new UnsupportedOperationException(
                "MergeAggregateExchange.scan() not yet implemented");
    }
}
