/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.planner.merge;

import java.util.List;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rex.RexNode;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Exchange operator that performs a merge-sort of pre-sorted shard results.
 *
 * <p>Used for ORDER BY + LIMIT queries where each shard returns its TopK results already sorted.
 * The coordinator performs a priority-queue merge to produce the globally sorted result with the
 * correct limit applied.
 */
public class MergeSortExchange extends Exchange {

    private final RelCollation collation;
    private final @Nullable RexNode offset;
    private final @Nullable RexNode fetch;

    public MergeSortExchange(
            RelOptCluster cluster,
            RelTraitSet traits,
            RelNode input,
            RelCollation collation,
            @Nullable RexNode offset,
            @Nullable RexNode fetch) {
        super(cluster, traits, input);
        this.collation = collation;
        this.offset = offset;
        this.fetch = fetch;
    }

    /** Convenience factory that creates a MergeSortExchange with enumerable convention traits. */
    public static MergeSortExchange create(
            RelNode input,
            RelCollation collation,
            @Nullable RexNode offset,
            @Nullable RexNode fetch) {
        RelOptCluster cluster = input.getCluster();
        RelTraitSet traits = enumerableTraitSet(cluster).replace(collation);
        return new MergeSortExchange(cluster, traits, input, collation, offset, fetch);
    }

    @Override
    public String getExchangeType() {
        return "MERGE_SORT";
    }

    public RelCollation getCollation() {
        return collation;
    }

    public @Nullable RexNode getOffset() {
        return offset;
    }

    public @Nullable RexNode getFetch() {
        return fetch;
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw)
                .item("collation", collation)
                .itemIf("offset", offset, offset != null)
                .itemIf("fetch", fetch, fetch != null);
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new MergeSortExchange(
                getCluster(), traitSet, sole(inputs), collation, offset, fetch);
    }

    @Override
    public Enumerable<@Nullable Object> scan() {
        // Full implementation will be wired in T8 when shard dispatch is available
        throw new UnsupportedOperationException("MergeSortExchange.scan() not yet implemented");
    }
}
