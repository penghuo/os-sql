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

/**
 * Final aggregation operator for distributed execution. Reads partial aggregation result rows
 * (from the exchange), merges partial states grouped by the group-by keys, and emits the final
 * aggregation results.
 *
 * <p>This is the second phase of a two-phase distributed aggregation. The input is expected to
 * come from a {@link PartialAggregationOperator} through an exchange.
 */
@EqualsAndHashCode(callSuper = false)
public class FinalAggregationOperator extends PhysicalPlan {

    @Getter private final PhysicalPlan input;
    @Getter private final List<NamedAggregator> aggregatorList;
    @Getter private final List<NamedExpression> groupByExprList;

    @EqualsAndHashCode.Exclude private Iterator<ExprValue> resultIterator;

    public FinalAggregationOperator(
            PhysicalPlan input,
            List<NamedAggregator> aggregatorList,
            List<NamedExpression> groupByExprList) {
        this.input = input;
        this.aggregatorList = aggregatorList;
        this.groupByExprList = groupByExprList;
    }

    @Override
    public <R, C> R accept(PhysicalPlanNodeVisitor<R, C> visitor, C context) {
        return visitor.visitFinalAggregation(this, context);
    }

    @Override
    public List<PhysicalPlan> getChild() {
        return Collections.singletonList(input);
    }

    @Override
    public void open() {
        super.open();

        // Map from group key string → (group-by values, partial states for merging)
        Map<String, GroupEntry> groups = new LinkedHashMap<>();

        while (input.hasNext()) {
            ExprValue row = input.next();
            Map<String, ExprValue> tupleMap = row.tupleValue();

            // Extract group-by keys from the partial result row
            String groupKeyStr = extractGroupKey(tupleMap);
            GroupEntry entry =
                    groups.computeIfAbsent(
                            groupKeyStr,
                            k -> {
                                Map<String, ExprValue> keyValues = new LinkedHashMap<>();
                                for (NamedExpression groupBy : groupByExprList) {
                                    keyValues.put(
                                            groupBy.getNameOrAlias(),
                                            tupleMap.get(groupBy.getNameOrAlias()));
                                }
                                PartialAggState[] states = new PartialAggState[aggregatorList.size()];
                                for (int i = 0; i < aggregatorList.size(); i++) {
                                    states[i] = PartialAggStateFactory.create(aggregatorList.get(i));
                                }
                                return new GroupEntry(keyValues, states);
                            });

            // Merge partial results for each aggregator
            for (int i = 0; i < aggregatorList.size(); i++) {
                entry.states[i].mergePartialResult(
                        aggregatorList.get(i).getName(), tupleMap);
            }
        }

        // Build final result rows
        List<ExprValue> results = new ArrayList<>();
        for (GroupEntry entry : groups.values()) {
            LinkedHashMap<String, ExprValue> resultMap = new LinkedHashMap<>(entry.groupByValues);
            for (int i = 0; i < aggregatorList.size(); i++) {
                resultMap.put(aggregatorList.get(i).getName(), entry.states[i].finalResult());
            }
            results.add(ExprTupleValue.fromExprValueMap(resultMap));
        }

        // Handle no group-by with no input: emit one row with default results
        if (groupByExprList.isEmpty() && groups.isEmpty()) {
            LinkedHashMap<String, ExprValue> resultMap = new LinkedHashMap<>();
            for (NamedAggregator agg : aggregatorList) {
                PartialAggState state = PartialAggStateFactory.create(agg);
                resultMap.put(agg.getName(), state.finalResult());
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

    private String extractGroupKey(Map<String, ExprValue> tupleMap) {
        if (groupByExprList.isEmpty()) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        for (NamedExpression groupBy : groupByExprList) {
            if (sb.length() > 0) {
                sb.append('\0');
            }
            ExprValue val = tupleMap.get(groupBy.getNameOrAlias());
            sb.append(val);
        }
        return sb.toString();
    }

    /** Internal holder for a group's key values and partial aggregation states being merged. */
    private static class GroupEntry {
        final Map<String, ExprValue> groupByValues;
        final PartialAggState[] states;

        GroupEntry(Map<String, ExprValue> groupByValues, PartialAggState[] states) {
            this.groupByValues = groupByValues;
            this.states = states;
        }
    }
}
