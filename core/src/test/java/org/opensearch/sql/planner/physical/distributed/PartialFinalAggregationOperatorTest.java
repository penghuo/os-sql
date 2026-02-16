/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.physical.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.opensearch.sql.data.type.ExprCoreType.DOUBLE;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;
import static org.opensearch.sql.data.type.ExprCoreType.LONG;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.NamedExpression;
import org.opensearch.sql.expression.aggregation.AvgAggregator;
import org.opensearch.sql.expression.aggregation.CountAggregator;
import org.opensearch.sql.expression.aggregation.MaxAggregator;
import org.opensearch.sql.expression.aggregation.MinAggregator;
import org.opensearch.sql.expression.aggregation.NamedAggregator;
import org.opensearch.sql.expression.aggregation.SumAggregator;
import org.opensearch.sql.planner.physical.AggregationOperator;
import org.opensearch.sql.planner.physical.PhysicalPlan;

class PartialFinalAggregationOperatorTest {

    /** Helper to create an in-memory PhysicalPlan from a list of ExprValue rows. */
    private PhysicalPlan inMemoryPlan(List<ExprValue> rows) {
        return new PhysicalPlan() {
            private java.util.Iterator<ExprValue> iter;

            @Override
            public <R, C> R accept(
                    org.opensearch.sql.planner.physical.PhysicalPlanNodeVisitor<R, C> visitor,
                    C context) {
                return null;
            }

            @Override
            public List<PhysicalPlan> getChild() {
                return Collections.emptyList();
            }

            @Override
            public void open() {
                iter = rows.iterator();
            }

            @Override
            public boolean hasNext() {
                return iter.hasNext();
            }

            @Override
            public ExprValue next() {
                return iter.next();
            }
        };
    }

    /** Helper to create a row with given field values. */
    private ExprValue row(Object... keyValues) {
        LinkedHashMap<String, ExprValue> map = new LinkedHashMap<>();
        for (int i = 0; i < keyValues.length; i += 2) {
            String key = (String) keyValues[i];
            Object val = keyValues[i + 1];
            if (val instanceof ExprValue) {
                map.put(key, (ExprValue) val);
            } else if (val instanceof Integer) {
                map.put(key, ExprValueUtils.integerValue((Integer) val));
            } else if (val instanceof Long) {
                map.put(key, ExprValueUtils.longValue((Long) val));
            } else if (val instanceof Double) {
                map.put(key, ExprValueUtils.doubleValue((Double) val));
            } else if (val instanceof String) {
                map.put(key, ExprValueUtils.stringValue((String) val));
            }
            // extend as needed
        }
        return ExprTupleValue.fromExprValueMap(map);
    }

    /** Drain all rows from a physical plan. */
    private List<ExprValue> drain(PhysicalPlan plan) {
        plan.open();
        List<ExprValue> results = new ArrayList<>();
        while (plan.hasNext()) {
            results.add(plan.next());
        }
        plan.close();
        return results;
    }

    @Nested
    @DisplayName("No group-by aggregation")
    class NoGroupBy {

        @Test
        @DisplayName("COUNT via partial+final matches single-node aggregation")
        void countPartialFinalMatchesSingleNode() {
            List<ExprValue> data =
                    Arrays.asList(
                            row("age", 25),
                            row("age", 30),
                            row("age", 35));

            List<Expression> aggArgs = Arrays.asList(DSL.ref("age", INTEGER));
            NamedAggregator countAgg =
                    DSL.named("count(age)", new CountAggregator(aggArgs, LONG));

            List<NamedAggregator> aggList = Arrays.asList(countAgg);
            List<NamedExpression> groupBy = Collections.emptyList();

            // Single-node result
            AggregationOperator singleNode =
                    new AggregationOperator(inMemoryPlan(data), aggList, groupBy);
            List<ExprValue> singleResult = drain(singleNode);

            // Partial → Final
            PartialAggregationOperator partial =
                    new PartialAggregationOperator(inMemoryPlan(data), aggList, groupBy);
            List<ExprValue> partialResults = drain(partial);

            FinalAggregationOperator finalOp =
                    new FinalAggregationOperator(
                            inMemoryPlan(partialResults), aggList, groupBy);
            List<ExprValue> finalResult = drain(finalOp);

            assertEquals(1, singleResult.size());
            assertEquals(1, finalResult.size());
            assertEquals(
                    singleResult.get(0).tupleValue().get("count(age)").longValue(),
                    finalResult.get(0).tupleValue().get("count(age)").longValue());
        }

        @Test
        @DisplayName("SUM via partial+final matches single-node aggregation")
        void sumPartialFinalMatchesSingleNode() {
            List<ExprValue> data =
                    Arrays.asList(
                            row("age", 10),
                            row("age", 20),
                            row("age", 30));

            List<Expression> aggArgs = Arrays.asList(DSL.ref("age", INTEGER));
            NamedAggregator sumAgg =
                    DSL.named("sum(age)", new SumAggregator(aggArgs, INTEGER));

            List<NamedAggregator> aggList = Arrays.asList(sumAgg);
            List<NamedExpression> groupBy = Collections.emptyList();

            // Single-node
            List<ExprValue> singleResult =
                    drain(new AggregationOperator(inMemoryPlan(data), aggList, groupBy));

            // Partial → Final
            List<ExprValue> partialResults =
                    drain(new PartialAggregationOperator(inMemoryPlan(data), aggList, groupBy));
            List<ExprValue> finalResult =
                    drain(
                            new FinalAggregationOperator(
                                    inMemoryPlan(partialResults), aggList, groupBy));

            assertEquals(
                    singleResult.get(0).tupleValue().get("sum(age)").integerValue(),
                    finalResult.get(0).tupleValue().get("sum(age)").integerValue());
        }

        @Test
        @DisplayName("AVG via partial+final matches single-node aggregation")
        void avgPartialFinalMatchesSingleNode() {
            List<ExprValue> data =
                    Arrays.asList(
                            row("score", 10.0),
                            row("score", 20.0),
                            row("score", 30.0));

            List<Expression> aggArgs = Arrays.asList(DSL.ref("score", DOUBLE));
            NamedAggregator avgAgg =
                    DSL.named("avg(score)", new AvgAggregator(aggArgs, DOUBLE));

            List<NamedAggregator> aggList = Arrays.asList(avgAgg);
            List<NamedExpression> groupBy = Collections.emptyList();

            // Single-node
            List<ExprValue> singleResult =
                    drain(new AggregationOperator(inMemoryPlan(data), aggList, groupBy));

            // Partial → Final
            List<ExprValue> partialResults =
                    drain(new PartialAggregationOperator(inMemoryPlan(data), aggList, groupBy));
            List<ExprValue> finalResult =
                    drain(
                            new FinalAggregationOperator(
                                    inMemoryPlan(partialResults), aggList, groupBy));

            assertEquals(
                    singleResult.get(0).tupleValue().get("avg(score)").doubleValue(),
                    finalResult.get(0).tupleValue().get("avg(score)").doubleValue(),
                    0.001);
        }

        @Test
        @DisplayName("MIN and MAX via partial+final matches single-node aggregation")
        void minMaxPartialFinalMatchesSingleNode() {
            List<ExprValue> data =
                    Arrays.asList(
                            row("val", 15),
                            row("val", 5),
                            row("val", 25));

            List<Expression> aggArgs = Arrays.asList(DSL.ref("val", INTEGER));
            NamedAggregator minAgg =
                    DSL.named("min(val)", new MinAggregator(aggArgs, INTEGER));
            NamedAggregator maxAgg =
                    DSL.named("max(val)", new MaxAggregator(aggArgs, INTEGER));

            List<NamedAggregator> aggList = Arrays.asList(minAgg, maxAgg);
            List<NamedExpression> groupBy = Collections.emptyList();

            // Single-node
            List<ExprValue> singleResult =
                    drain(new AggregationOperator(inMemoryPlan(data), aggList, groupBy));

            // Partial → Final
            List<ExprValue> partialResults =
                    drain(new PartialAggregationOperator(inMemoryPlan(data), aggList, groupBy));
            List<ExprValue> finalResult =
                    drain(
                            new FinalAggregationOperator(
                                    inMemoryPlan(partialResults), aggList, groupBy));

            assertEquals(
                    singleResult.get(0).tupleValue().get("min(val)").integerValue(),
                    finalResult.get(0).tupleValue().get("min(val)").integerValue());
            assertEquals(
                    singleResult.get(0).tupleValue().get("max(val)").integerValue(),
                    finalResult.get(0).tupleValue().get("max(val)").integerValue());
        }

        @Test
        @DisplayName("Empty input produces correct defaults")
        void emptyInput() {
            List<ExprValue> data = Collections.emptyList();

            List<Expression> aggArgs = Arrays.asList(DSL.ref("age", INTEGER));
            NamedAggregator countAgg =
                    DSL.named("count(age)", new CountAggregator(aggArgs, LONG));

            List<NamedAggregator> aggList = Arrays.asList(countAgg);
            List<NamedExpression> groupBy = Collections.emptyList();

            // Partial → Final with empty input
            List<ExprValue> partialResults =
                    drain(new PartialAggregationOperator(inMemoryPlan(data), aggList, groupBy));
            assertEquals(1, partialResults.size());

            List<ExprValue> finalResult =
                    drain(
                            new FinalAggregationOperator(
                                    inMemoryPlan(partialResults), aggList, groupBy));
            assertEquals(1, finalResult.size());
            assertEquals(0L, finalResult.get(0).tupleValue().get("count(age)").longValue());
        }
    }

    @Nested
    @DisplayName("Multi-partition aggregation (simulated)")
    class MultiPartition {

        @Test
        @DisplayName("COUNT from multiple partitions merges correctly")
        void countMultiPartition() {
            // Partition 1
            List<ExprValue> part1 = Arrays.asList(row("x", 1), row("x", 2), row("x", 3));
            // Partition 2
            List<ExprValue> part2 = Arrays.asList(row("x", 4), row("x", 5));

            List<Expression> aggArgs = Arrays.asList(DSL.ref("x", INTEGER));
            NamedAggregator countAgg =
                    DSL.named("count(x)", new CountAggregator(aggArgs, LONG));
            List<NamedAggregator> aggList = Arrays.asList(countAgg);
            List<NamedExpression> groupBy = Collections.emptyList();

            // Partial from partition 1
            List<ExprValue> partial1 =
                    drain(new PartialAggregationOperator(inMemoryPlan(part1), aggList, groupBy));
            // Partial from partition 2
            List<ExprValue> partial2 =
                    drain(new PartialAggregationOperator(inMemoryPlan(part2), aggList, groupBy));

            // Merge all partial results
            List<ExprValue> allPartials = new ArrayList<>();
            allPartials.addAll(partial1);
            allPartials.addAll(partial2);

            List<ExprValue> finalResult =
                    drain(
                            new FinalAggregationOperator(
                                    inMemoryPlan(allPartials), aggList, groupBy));

            assertEquals(1, finalResult.size());
            assertEquals(5L, finalResult.get(0).tupleValue().get("count(x)").longValue());
        }

        @Test
        @DisplayName("AVG from multiple partitions produces correct weighted average")
        void avgMultiPartition() {
            // Partition 1: values 10, 20 → avg should consider sum=30, count=2
            List<ExprValue> part1 = Arrays.asList(row("v", 10.0), row("v", 20.0));
            // Partition 2: values 30 → sum=30, count=1
            List<ExprValue> part2 = Arrays.asList(row("v", 30.0));

            List<Expression> aggArgs = Arrays.asList(DSL.ref("v", DOUBLE));
            NamedAggregator avgAgg =
                    DSL.named("avg(v)", new AvgAggregator(aggArgs, DOUBLE));
            List<NamedAggregator> aggList = Arrays.asList(avgAgg);
            List<NamedExpression> groupBy = Collections.emptyList();

            List<ExprValue> partial1 =
                    drain(new PartialAggregationOperator(inMemoryPlan(part1), aggList, groupBy));
            List<ExprValue> partial2 =
                    drain(new PartialAggregationOperator(inMemoryPlan(part2), aggList, groupBy));

            List<ExprValue> allPartials = new ArrayList<>();
            allPartials.addAll(partial1);
            allPartials.addAll(partial2);

            List<ExprValue> finalResult =
                    drain(
                            new FinalAggregationOperator(
                                    inMemoryPlan(allPartials), aggList, groupBy));

            // (10 + 20 + 30) / 3 = 20.0
            assertEquals(20.0, finalResult.get(0).tupleValue().get("avg(v)").doubleValue(), 0.001);
        }
    }

    @Nested
    @DisplayName("Group-by aggregation")
    class GroupByTests {

        @Test
        @DisplayName("COUNT with group-by matches single-node result")
        void countWithGroupBy() {
            List<ExprValue> data =
                    Arrays.asList(
                            row("dept", "eng", "age", 25),
                            row("dept", "eng", "age", 30),
                            row("dept", "sales", "age", 35),
                            row("dept", "sales", "age", 40),
                            row("dept", "sales", "age", 45));

            List<Expression> aggArgs = Arrays.asList(DSL.ref("age", INTEGER));
            NamedAggregator countAgg =
                    DSL.named("count(age)", new CountAggregator(aggArgs, LONG));
            List<NamedAggregator> aggList = Arrays.asList(countAgg);
            List<NamedExpression> groupBy =
                    Arrays.asList(DSL.named("dept", DSL.ref("dept", STRING)));

            // Single-node
            List<ExprValue> singleResult =
                    drain(new AggregationOperator(inMemoryPlan(data), aggList, groupBy));

            // Partial → Final
            List<ExprValue> partialResults =
                    drain(new PartialAggregationOperator(inMemoryPlan(data), aggList, groupBy));
            List<ExprValue> finalResult =
                    drain(
                            new FinalAggregationOperator(
                                    inMemoryPlan(partialResults), aggList, groupBy));

            // Compare as sets since order may differ
            Map<String, Long> singleMap = new HashMap<>();
            for (ExprValue row : singleResult) {
                singleMap.put(
                        row.tupleValue().get("dept").stringValue(),
                        row.tupleValue().get("count(age)").longValue());
            }

            Map<String, Long> finalMap = new HashMap<>();
            for (ExprValue row : finalResult) {
                finalMap.put(
                        row.tupleValue().get("dept").stringValue(),
                        row.tupleValue().get("count(age)").longValue());
            }

            assertEquals(singleMap, finalMap);
            assertEquals(2L, finalMap.get("eng"));
            assertEquals(3L, finalMap.get("sales"));
        }

        @Test
        @DisplayName("Multi-partition group-by merges correctly")
        void multiPartitionGroupBy() {
            // Partition 1: eng=2, sales=1
            List<ExprValue> part1 =
                    Arrays.asList(
                            row("dept", "eng", "age", 25),
                            row("dept", "eng", "age", 30),
                            row("dept", "sales", "age", 35));
            // Partition 2: sales=2
            List<ExprValue> part2 =
                    Arrays.asList(
                            row("dept", "sales", "age", 40),
                            row("dept", "sales", "age", 45));

            List<Expression> aggArgs = Arrays.asList(DSL.ref("age", INTEGER));
            NamedAggregator countAgg =
                    DSL.named("count(age)", new CountAggregator(aggArgs, LONG));
            List<NamedAggregator> aggList = Arrays.asList(countAgg);
            List<NamedExpression> groupBy =
                    Arrays.asList(DSL.named("dept", DSL.ref("dept", STRING)));

            List<ExprValue> partial1 =
                    drain(new PartialAggregationOperator(inMemoryPlan(part1), aggList, groupBy));
            List<ExprValue> partial2 =
                    drain(new PartialAggregationOperator(inMemoryPlan(part2), aggList, groupBy));

            List<ExprValue> allPartials = new ArrayList<>();
            allPartials.addAll(partial1);
            allPartials.addAll(partial2);

            List<ExprValue> finalResult =
                    drain(
                            new FinalAggregationOperator(
                                    inMemoryPlan(allPartials), aggList, groupBy));

            Map<String, Long> resultMap = new HashMap<>();
            for (ExprValue row : finalResult) {
                resultMap.put(
                        row.tupleValue().get("dept").stringValue(),
                        row.tupleValue().get("count(age)").longValue());
            }

            assertEquals(2L, resultMap.get("eng"));
            assertEquals(3L, resultMap.get("sales"));
        }

        @Test
        @DisplayName("Multiple aggregations with group-by")
        void multipleAggsWithGroupBy() {
            List<ExprValue> data =
                    Arrays.asList(
                            row("dept", "eng", "salary", 100.0),
                            row("dept", "eng", "salary", 200.0),
                            row("dept", "sales", "salary", 150.0));

            List<Expression> aggArgs = Arrays.asList(DSL.ref("salary", DOUBLE));
            NamedAggregator sumAgg =
                    DSL.named("sum(salary)", new SumAggregator(aggArgs, DOUBLE));
            NamedAggregator countAgg =
                    DSL.named("count(salary)", new CountAggregator(aggArgs, LONG));
            NamedAggregator avgAgg =
                    DSL.named("avg(salary)", new AvgAggregator(aggArgs, DOUBLE));

            List<NamedAggregator> aggList = Arrays.asList(sumAgg, countAgg, avgAgg);
            List<NamedExpression> groupBy =
                    Arrays.asList(DSL.named("dept", DSL.ref("dept", STRING)));

            // Partial → Final
            List<ExprValue> partialResults =
                    drain(new PartialAggregationOperator(inMemoryPlan(data), aggList, groupBy));
            List<ExprValue> finalResult =
                    drain(
                            new FinalAggregationOperator(
                                    inMemoryPlan(partialResults), aggList, groupBy));

            Map<String, ExprValue> engRow = null;
            Map<String, ExprValue> salesRow = null;
            for (ExprValue row : finalResult) {
                String dept = row.tupleValue().get("dept").stringValue();
                if ("eng".equals(dept)) {
                    engRow = row.tupleValue();
                } else if ("sales".equals(dept)) {
                    salesRow = row.tupleValue();
                }
            }

            // eng: sum=300, count=2, avg=150
            assertEquals(300.0, engRow.get("sum(salary)").doubleValue(), 0.001);
            assertEquals(2L, engRow.get("count(salary)").longValue());
            assertEquals(150.0, engRow.get("avg(salary)").doubleValue(), 0.001);

            // sales: sum=150, count=1, avg=150
            assertEquals(150.0, salesRow.get("sum(salary)").doubleValue(), 0.001);
            assertEquals(1L, salesRow.get("count(salary)").longValue());
            assertEquals(150.0, salesRow.get("avg(salary)").doubleValue(), 0.001);
        }
    }
}
