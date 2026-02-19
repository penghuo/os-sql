/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.planner.merge;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rex.RexLiteral;
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

    /** Entry in the priority queue holding the current row and the source iterator index. */
    private static class MergeEntry {
        final Object[] row;
        final int iteratorIndex;

        MergeEntry(Object[] row, int iteratorIndex) {
            this.row = row;
            this.iteratorIndex = iteratorIndex;
        }
    }

    @Override
    public Enumerable<@Nullable Object> scan() {
        if (shardResults == null || shardResults.isEmpty()) {
            return Linq4j.emptyEnumerable();
        }

        int offsetVal = offset != null ? ((RexLiteral) offset).getValueAs(Integer.class) : 0;
        int fetchVal = fetch != null ? ((RexLiteral) fetch).getValueAs(Integer.class)
                : Integer.MAX_VALUE;

        Comparator<Object[]> rowComparator = buildComparator();

        // Collect iterators from each shard's pre-sorted rows
        List<Iterator<Object[]>> iterators = new ArrayList<>();
        for (ShardResult result : shardResults) {
            if (!result.getRows().isEmpty()) {
                iterators.add(result.getRows().iterator());
            }
        }

        if (iterators.isEmpty()) {
            return Linq4j.emptyEnumerable();
        }

        // Priority-queue merge
        PriorityQueue<MergeEntry> pq = new PriorityQueue<>(
                iterators.size(),
                (a, b) -> rowComparator.compare(a.row, b.row));

        // Seed the priority queue with the first row from each iterator
        for (int i = 0; i < iterators.size(); i++) {
            Iterator<Object[]> it = iterators.get(i);
            if (it.hasNext()) {
                pq.offer(new MergeEntry(it.next(), i));
            }
        }

        // Merge-sort all rows
        List<Object[]> merged = new ArrayList<>();
        while (!pq.isEmpty()) {
            MergeEntry entry = pq.poll();
            merged.add(entry.row);
            Iterator<Object[]> it = iterators.get(entry.iteratorIndex);
            if (it.hasNext()) {
                pq.offer(new MergeEntry(it.next(), entry.iteratorIndex));
            }
        }

        // Apply offset and fetch
        int start = Math.min(offsetVal, merged.size());
        int end = Math.min(start + fetchVal, merged.size());
        List<Object[]> result = merged.subList(start, end);

        return new AbstractEnumerable<@Nullable Object>() {
            @Override
            public Enumerator<@Nullable Object> enumerator() {
                return new Enumerator<>() {
                    private int index = -1;

                    @Override
                    public Object current() {
                        return result.get(index);
                    }

                    @Override
                    public boolean moveNext() {
                        return ++index < result.size();
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

    /**
     * Builds a Comparator for Object[] rows based on the collation field orderings. Supports
     * ASCENDING/DESCENDING directions and FIRST/LAST null ordering.
     */
    @SuppressWarnings("unchecked")
    private Comparator<Object[]> buildComparator() {
        List<RelFieldCollation> fieldCollations = collation.getFieldCollations();
        return (row1, row2) -> {
            for (RelFieldCollation fc : fieldCollations) {
                int idx = fc.getFieldIndex();
                Object val1 = row1[idx];
                Object val2 = row2[idx];

                // Handle nulls according to null direction
                if (val1 == null && val2 == null) {
                    continue;
                }
                if (val1 == null) {
                    return fc.nullDirection == RelFieldCollation.NullDirection.FIRST ? -1 : 1;
                }
                if (val2 == null) {
                    return fc.nullDirection == RelFieldCollation.NullDirection.FIRST ? 1 : -1;
                }

                int cmp;
                if (val1 instanceof Comparable) {
                    cmp = ((Comparable<Object>) val1).compareTo(val2);
                } else {
                    cmp = val1.toString().compareTo(val2.toString());
                }

                if (fc.getDirection() == RelFieldCollation.Direction.DESCENDING) {
                    cmp = -cmp;
                }
                if (cmp != 0) {
                    return cmp;
                }
            }
            return 0;
        };
    }
}
