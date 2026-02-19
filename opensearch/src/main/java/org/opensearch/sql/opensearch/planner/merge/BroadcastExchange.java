/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.planner.merge;

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Exchange operator that broadcasts the small side of a join to all nodes.
 *
 * <p>Used when one side of a join is small enough to fit in memory on every node. Instead of
 * shuffling both sides by join key (HashExchange), the small side is broadcast to all nodes and
 * joined locally with each shard's portion of the large side.
 */
public class BroadcastExchange extends Exchange {

    public BroadcastExchange(RelOptCluster cluster, RelTraitSet traits, RelNode input) {
        super(cluster, traits, input);
    }

    /** Convenience factory that creates a BroadcastExchange with enumerable convention traits. */
    public static BroadcastExchange create(RelNode input) {
        RelOptCluster cluster = input.getCluster();
        return new BroadcastExchange(cluster, enumerableTraitSet(cluster), input);
    }

    @Override
    public String getExchangeType() {
        return "BROADCAST";
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new BroadcastExchange(getCluster(), traitSet, sole(inputs));
    }

    @Override
    public Enumerable<@Nullable Object> scan() {
        // The broadcast logic (sending data to all nodes) is handled by DistributedExecutor.
        // On the coordinator side, just concatenate all collected results.
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
