/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.physical.distributed;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.OptionalLong;
import java.util.PriorityQueue;
import lombok.Getter;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.sql.ast.tree.Sort.SortOption;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.planner.physical.PhysicalPlanNodeVisitor;
import org.opensearch.sql.planner.physical.SortHelper;

/**
 * Distributed sort operator that performs a merge sort of rows arriving from multiple sorted shards.
 * Loads all rows into a PriorityQueue and drains in sorted order, optionally respecting a top-N
 * limit.
 */
public class DistributedSortOperator extends PhysicalPlan {

    @Getter private final PhysicalPlan input;
    @Getter private final List<Pair<SortOption, Expression>> sortList;
    @Getter private final OptionalLong limit;
    private final Comparator<ExprValue> comparator;
    private PriorityQueue<ExprValue> sortedQueue;
    private long emitted;

    public DistributedSortOperator(
            PhysicalPlan input,
            List<Pair<SortOption, Expression>> sortList,
            OptionalLong limit) {
        this.input = input;
        this.sortList = sortList;
        this.limit = limit;
        this.comparator = SortHelper.constructExprComparator(sortList);
    }

    @Override
    public <R, C> R accept(PhysicalPlanNodeVisitor<R, C> visitor, C context) {
        return visitor.visitDistributedSort(this, context);
    }

    @Override
    public void open() {
        super.open();
        sortedQueue = new PriorityQueue<>(1, comparator::compare);
        while (input.hasNext()) {
            sortedQueue.add(input.next());
        }
        emitted = 0;
    }

    @Override
    public boolean hasNext() {
        if (limit.isPresent() && emitted >= limit.getAsLong()) {
            return false;
        }
        return !sortedQueue.isEmpty();
    }

    @Override
    public ExprValue next() {
        if (!hasNext()) {
            throw new NoSuchElementException("Sort exhausted");
        }
        emitted++;
        return sortedQueue.poll();
    }

    @Override
    public void close() {
        input.close();
        sortedQueue = null;
    }

    @Override
    public List<PhysicalPlan> getChild() {
        return Collections.singletonList(input);
    }
}
