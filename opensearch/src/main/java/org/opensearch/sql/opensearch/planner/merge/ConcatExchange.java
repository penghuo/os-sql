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
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Exchange operator that performs unordered concatenation of shard results.
 *
 * <p>Used for simple scans, filter+project, and dedup where no ordering or aggregation merge is
 * needed on the coordinator side. Results from all shards are simply appended together.
 */
public class ConcatExchange extends Exchange {

    public ConcatExchange(RelOptCluster cluster, RelTraitSet traits, RelNode input) {
        super(cluster, traits, input);
    }

    /** Convenience factory that creates a ConcatExchange with enumerable convention traits. */
    public static ConcatExchange create(RelNode input) {
        RelOptCluster cluster = input.getCluster();
        return new ConcatExchange(cluster, enumerableTraitSet(cluster), input);
    }

    @Override
    public String getExchangeType() {
        return "CONCAT";
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new ConcatExchange(getCluster(), traitSet, sole(inputs));
    }

    @Override
    public Enumerable<@Nullable Object> scan() {
        // Full implementation will be wired in T8 when shard dispatch is available
        throw new UnsupportedOperationException("ConcatExchange.scan() not yet implemented");
    }
}
