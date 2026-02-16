/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.physical.distributed;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.NamedExpression;
import org.opensearch.sql.expression.aggregation.NamedAggregator;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.planner.physical.PhysicalPlanNodeVisitor;
import org.opensearch.sql.storage.bindingtuple.BindingTuple;

/**
 * Partial aggregation operator for distributed execution. Reads all input rows from a single
 * shard/partition, computes partial aggregation states grouped by the group-by keys, and emits
 * rows containing the group-by values plus serialized partial state fields.
 *
 * <p>This is the first phase of a two-phase distributed aggregation. The output rows are sent
 * through an exchange to the {@link FinalAggregationOperator}.
 */
@EqualsAndHashCode(callSuper = false)
public class PartialAggregationOperator extends PhysicalPlan {

    @Getter private final PhysicalPlan input;
    @Getter private final List<NamedAggregator> aggregatorList;
    @Getter private final List<NamedExpression> groupByExprList;

    @EqualsAndHashCode.Exclude private Iterator<ExprValue> resultIterator;

    public PartialAggregationOperator(
            PhysicalPlan input,
            List<NamedAggregator> aggregatorList,
            List<NamedExpression> groupByExprList) {
        this.input = input;
        this.aggregatorList = aggregatorList;
        this.groupByExprList = groupByExprList;
    }

    @Override
    public <R, C> R accept(PhysicalPlanNodeVisitor<R, C> visitor, C context) {
        return visitor.visitPartialAggregation(this, context);
    }

    @Override
    public List<PhysicalPlan> getChild() {
        return Collections.singletonList(input);
    }

    @Override
    public void open() {
        super.open();

        // Map from group key string → (group-by values, partial states)
        Map<String, GroupEntry> groups = new LinkedHashMap<>();

        while (input.hasNext()) {
            ExprValue row = input.next();
            BindingTuple tuple = row.bindingTuples();

            // Evaluate group-by keys
            String groupKeyStr = evaluateGroupKey(tuple);
            GroupEntry entry =
                    groups.computeIfAbsent(
                            groupKeyStr,
                            k -> {
                                Map<String, ExprValue> keyValues = new LinkedHashMap<>();
                                for (NamedExpression groupBy : groupByExprList) {
                                    keyValues.put(
                                            groupBy.getNameOrAlias(),
                                            groupBy.valueOf(tuple));
                                }
                                PartialAggState[] states = new PartialAggState[aggregatorList.size()];
                                for (int i = 0; i < aggregatorList.size(); i++) {
                                    states[i] = PartialAggStateFactory.create(aggregatorList.get(i));
                                }
                                return new GroupEntry(keyValues, states);
                            });

            // Accumulate each aggregator
            for (int i = 0; i < aggregatorList.size(); i++) {
                NamedAggregator agg = aggregatorList.get(i);
                ExprValue value = agg.getArguments().get(0).valueOf(tuple);
                if (!value.isNull() && !value.isMissing() && agg.conditionValue(tuple)) {
                    entry.states[i].accumulate(value);
                }
            }
        }

        // Build result rows
        List<ExprValue> results = new ArrayList<>();
        for (GroupEntry entry : groups.values()) {
            LinkedHashMap<String, ExprValue> resultMap = new LinkedHashMap<>(entry.groupByValues);
            for (int i = 0; i < aggregatorList.size(); i++) {
                entry.states[i].toPartialResult(aggregatorList.get(i).getName(), resultMap);
            }
            results.add(ExprTupleValue.fromExprValueMap(resultMap));
        }

        // Handle no group-by: still emit one row with partial states
        if (groupByExprList.isEmpty() && groups.isEmpty()) {
            LinkedHashMap<String, ExprValue> resultMap = new LinkedHashMap<>();
            for (NamedAggregator agg : aggregatorList) {
                PartialAggState state = PartialAggStateFactory.create(agg);
                state.toPartialResult(agg.getName(), resultMap);
            }
            results.add(ExprTupleValue.fromExprValueMap(resultMap));
        }

        resultIterator = results.iterator();
    }

    @Override
    public boolean hasNext() {
        return resultIterator.hasNext();
    }

    @Override
    public ExprValue next() {
        return resultIterator.next();
    }

    private String evaluateGroupKey(BindingTuple tuple) {
        if (groupByExprList.isEmpty()) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        for (NamedExpression groupBy : groupByExprList) {
            if (sb.length() > 0) {
                sb.append('\0');
            }
            sb.append(groupBy.valueOf(tuple));
        }
        return sb.toString();
    }

    /** Internal holder for a group's key values and partial aggregation states. */
    private static class GroupEntry {
        final Map<String, ExprValue> groupByValues;
        final PartialAggState[] states;

        GroupEntry(Map<String, ExprValue> groupByValues, PartialAggState[] states) {
            this.groupByValues = groupByValues;
            this.states = states;
        }
    }
}
