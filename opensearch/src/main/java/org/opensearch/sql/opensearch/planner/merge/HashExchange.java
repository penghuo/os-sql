/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.planner.merge;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Exchange operator that shuffles data by hash key across partitions.
 *
 * <p>Used for distributed hash joins and hash-based aggregations where rows must be co-located by
 * key before processing. Each row is assigned to a partition based on the hash of its distribution
 * key fields, ensuring that rows with the same key values end up on the same node.
 */
public class HashExchange extends Exchange {

    private final ImmutableList<Integer> distributionKeys;
    private final int numPartitions;

    public HashExchange(
            RelOptCluster cluster,
            RelTraitSet traits,
            RelNode input,
            ImmutableList<Integer> distributionKeys,
            int numPartitions) {
        super(cluster, traits, input);
        this.distributionKeys = distributionKeys;
        this.numPartitions = numPartitions;
    }

    /**
     * Convenience factory that creates a HashExchange with enumerable convention traits.
     *
     * @param input the child relational expression
     * @param distributionKeys field indices to hash on for partitioning
     * @param numPartitions the number of target partitions
     * @return a new HashExchange node
     */
    public static HashExchange create(
            RelNode input, List<Integer> distributionKeys, int numPartitions) {
        RelOptCluster cluster = input.getCluster();
        return new HashExchange(
                cluster,
                enumerableTraitSet(cluster),
                input,
                ImmutableList.copyOf(distributionKeys),
                numPartitions);
    }

    @Override
    public String getExchangeType() {
        return "HASH";
    }

    /** Returns the field indices used as distribution keys for hash partitioning. */
    public ImmutableList<Integer> getDistributionKeys() {
        return distributionKeys;
    }

    /** Returns the number of target partitions. */
    public int getNumPartitions() {
        return numPartitions;
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw)
                .item("distributionKeys", distributionKeys)
                .item("numPartitions", numPartitions);
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new HashExchange(
                getCluster(), traitSet, sole(inputs), distributionKeys, numPartitions);
    }

    @Override
    public Enumerable<@Nullable Object> scan() {
        // HashExchange results are pre-partitioned by DistributedExecutor.
        // Just concatenate all partition results.
        if (shardResults == null || shardResults.isEmpty()) {
            return Linq4j.emptyEnumerable();
        }
        List<Object[]> allRows = new ArrayList<>();
        for (ShardResult result : shardResults) {
            allRows.addAll(result.getRows());
        }
        return new AbstractEnumerable<@Nullable Object>() {
            @Override
            public Enumerator<@Nullable Object> enumerator() {
                return new Enumerator<>() {
                    private int index = -1;

                    @Override
                    public Object current() {
                        return allRows.get(index);
                    }

                    @Override
                    public boolean moveNext() {
                        return ++index < allRows.size();
                    }

                    @Override
                    public void reset() {
                        index = -1;
                    }

                    @Override
                    public void close() {}
                };
            }
        };
    }
}
