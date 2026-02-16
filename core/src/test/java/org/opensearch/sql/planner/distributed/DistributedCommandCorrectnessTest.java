/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.ast.tree.Sort.SortOption;
import org.opensearch.sql.data.model.ExprIntegerValue;
import org.opensearch.sql.data.model.ExprLongValue;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.NamedExpression;
import org.opensearch.sql.expression.ReferenceExpression;
import org.opensearch.sql.expression.aggregation.Aggregator;
import org.opensearch.sql.expression.aggregation.AvgAggregator;
import org.opensearch.sql.expression.aggregation.CountAggregator;
import org.opensearch.sql.expression.aggregation.MaxAggregator;
import org.opensearch.sql.expression.aggregation.MinAggregator;
import org.opensearch.sql.expression.aggregation.NamedAggregator;
import org.opensearch.sql.expression.aggregation.SumAggregator;
import org.opensearch.sql.planner.physical.AggregationOperator;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.planner.physical.distributed.DistributedExchangeOperator;
import org.opensearch.sql.planner.physical.distributed.DistributedHashJoinOperator;
import org.opensearch.sql.planner.physical.distributed.DistributedSortOperator;
import org.opensearch.sql.planner.physical.distributed.ExchangeBuffer;
import org.opensearch.sql.planner.physical.distributed.FinalAggregationOperator;
import org.opensearch.sql.planner.physical.distributed.JoinType;
import org.opensearch.sql.planner.physical.distributed.PartialAggregationOperator;

/**
 * End-to-end correctness tests for Tier 1 PPL commands through the distributed execution path.
 * Each test verifies that a distributed execution (partial agg → exchange → final agg) produces
 * identical results to a single-node execution path.
 */
class DistributedCommandCorrectnessTest {

    /** Creates a row (ExprTupleValue) from field name/value pairs. */
    private static ExprValue row(Object... pairs) {
        LinkedHashMap<String, ExprValue> map = new LinkedHashMap<>();
        for (int i = 0; i < pairs.length; i += 2) {
            String key = (String) pairs[i];
            Object val = pairs[i + 1];
            if (val instanceof Integer) {
                map.put(key, new ExprIntegerValue((Integer) val));
            } else if (val instanceof Long) {
                map.put(key, new ExprLongValue((Long) val));
            } else if (val instanceof String) {
                map.put(key, new ExprStringValue((String) val));
            } else if (val instanceof ExprValue) {
                map.put(key, (ExprValue) val);
            } else if (val instanceof Double) {
                map.put(key, ExprValueUtils.doubleValue((Double) val));
            }
        }
        return ExprTupleValue.fromExprValueMap(map);
    }

    /**
     * Feeds ExprValue rows into an ExchangeBuffer and marks complete. Must be called after the
     * exchange operator has been opened (so the buffer exists).
     */
    private static void feedExchange(DistributedExchangeOperator exchange, List<ExprValue> rows) {
        ExchangeBuffer buffer = exchange.getBuffer();
        buffer.offer(rows);
        buffer.markAllComplete();
    }

    /**
     * Opens a FinalAggregationOperator that reads from an exchange, feeding the exchange
     * concurrently so the blocking buffer reads don't deadlock.
     */
    private static void openWithConcurrentFeed(
            FinalAggregationOperator finalAgg,
            DistributedExchangeOperator exchange,
            List<ExprValue> partialResults) {
        // Feed data in a separate thread since finalAgg.open() blocks reading from exchange
        Thread feeder = new Thread(() -> {
            // Wait briefly for the exchange to be opened by super.open()
            while (exchange.getBuffer() == null) {
                Thread.yield();
            }
            feedExchange(exchange, partialResults);
        });
        feeder.setDaemon(true);
        feeder.start();
        finalAgg.open();
    }

    /**
     * Opens a DistributedSortOperator that reads from an exchange, feeding data concurrently.
     */
    private static void openSortWithConcurrentFeed(
            DistributedSortOperator sort,
            DistributedExchangeOperator exchange,
            List<ExprValue> rows) {
        Thread feeder = new Thread(() -> {
            while (exchange.getBuffer() == null) {
                Thread.yield();
            }
            feedExchange(exchange, rows);
        });
        feeder.setDaemon(true);
        feeder.start();
        sort.open();
    }

    /**
     * Opens a DistributedHashJoinOperator that reads from two exchanges, feeding both concurrently.
     */
    private static void openJoinWithConcurrentFeed(
            DistributedHashJoinOperator join,
            DistributedExchangeOperator buildExchange,
            DistributedExchangeOperator probeExchange,
            List<ExprValue> buildRows,
            List<ExprValue> probeRows) {
        Thread buildFeeder = new Thread(() -> {
            while (buildExchange.getBuffer() == null) {
                Thread.yield();
            }
            feedExchange(buildExchange, buildRows);
        });
        Thread probeFeeder = new Thread(() -> {
            while (probeExchange.getBuffer() == null) {
                Thread.yield();
            }
            feedExchange(probeExchange, probeRows);
        });
        buildFeeder.setDaemon(true);
        probeFeeder.setDaemon(true);
        buildFeeder.start();
        probeFeeder.start();
        join.open();
    }

    /** Consumes all rows from a PhysicalPlan and returns them as a list. */
    private static List<ExprValue> consumeAll(PhysicalPlan plan) {
        List<ExprValue> results = new ArrayList<>();
        while (plan.hasNext()) {
            results.add(plan.next());
        }
        return results;
    }

    /** Creates a simple in-memory PhysicalPlan from a list of rows. */
    private static PhysicalPlan memoryPlan(List<ExprValue> rows) {
        return new PhysicalPlan() {
            private java.util.Iterator<ExprValue> iterator;

            @Override
            public <R, C> R accept(
                    org.opensearch.sql.planner.physical.PhysicalPlanNodeVisitor<R, C> visitor,
                    C context) {
                return null;
            }

            @Override
            public void open() {
                iterator = rows.iterator();
            }

            @Override
            public boolean hasNext() {
                return iterator != null && iterator.hasNext();
            }

            @Override
            public ExprValue next() {
                return iterator.next();
            }

            @Override
            public void close() {
                iterator = null;
            }

            @Override
            public List<PhysicalPlan> getChild() {
                return Collections.emptyList();
            }
        };
    }

    @SuppressWarnings("unchecked")
    private static NamedAggregator namedAgg(String name, String func, String fieldName,
            ExprCoreType type) {
        List<Expression> args = Collections.singletonList(new ReferenceExpression(fieldName, type));
        Aggregator<?> agg;
        switch (func) {
            case "count":
                agg = new CountAggregator(args, ExprCoreType.LONG);
                break;
            case "sum":
                agg = new SumAggregator(args, type);
                break;
            case "avg":
                agg = new AvgAggregator(args, ExprCoreType.DOUBLE);
                break;
            case "min":
                agg = new MinAggregator(args, type);
                break;
            case "max":
                agg = new MaxAggregator(args, type);
                break;
            default:
                throw new IllegalArgumentException("Unknown func: " + func);
        }
        return new NamedAggregator(name, (Aggregator) agg);
    }

    private static NamedExpression namedExpr(String name, ExprCoreType type) {
        return new NamedExpression(name, new ReferenceExpression(name, type));
    }

    @Nested
    @DisplayName("stats command: COUNT")
    class StatsCount {

        @Test
        @DisplayName("COUNT(*) across 3 shards matches single-node")
        void countAllDistributed() {
            // Data: 3 shards with varying row counts
            List<ExprValue> shard1 = Arrays.asList(
                    row("id", 1, "amount", 10),
                    row("id", 2, "amount", 20));
            List<ExprValue> shard2 = Arrays.asList(
                    row("id", 3, "amount", 30));
            List<ExprValue> shard3 = Arrays.asList(
                    row("id", 4, "amount", 40),
                    row("id", 5, "amount", 50),
                    row("id", 6, "amount", 60));

            List<NamedAggregator> aggs = Collections.singletonList(
                    namedAgg("count()", "count", "*", ExprCoreType.INTEGER));
            List<NamedExpression> groupBy = Collections.emptyList();

            // Single-node: combine all rows
            List<ExprValue> allRows = new ArrayList<>();
            allRows.addAll(shard1);
            allRows.addAll(shard2);
            allRows.addAll(shard3);
            AggregationOperator singleNode = new AggregationOperator(
                    memoryPlan(allRows), aggs, groupBy);
            singleNode.open();
            List<ExprValue> singleNodeResults = consumeAll(singleNode);
            singleNode.close();

            // Distributed: partial agg on each shard → final agg
            List<ExprValue> partialResults = new ArrayList<>();
            for (List<ExprValue> shard : Arrays.asList(shard1, shard2, shard3)) {
                PartialAggregationOperator partial = new PartialAggregationOperator(
                        memoryPlan(shard), aggs, groupBy);
                partial.open();
                partialResults.addAll(consumeAll(partial));
                partial.close();
            }

            DistributedExchangeOperator exchange =
                    new DistributedExchangeOperator(ExchangeSpec.gather(), 1);
            FinalAggregationOperator finalAgg = new FinalAggregationOperator(
                    exchange, aggs, groupBy);
            openWithConcurrentFeed(finalAgg, exchange, partialResults);
            List<ExprValue> distResults = consumeAll(finalAgg);
            finalAgg.close();

            // Compare
            assertEquals(singleNodeResults.size(), distResults.size(),
                    "Row count should match");
            // Both should be {count()=6}
            assertEquals(
                    singleNodeResults.get(0).tupleValue().get("count()"),
                    distResults.get(0).tupleValue().get("count()"),
                    "COUNT result should match");
        }

        @Test
        @DisplayName("COUNT with GROUP BY matches single-node")
        void countGroupByDistributed() {
            List<ExprValue> shard1 = Arrays.asList(
                    row("category", "A", "id", 1),
                    row("category", "B", "id", 2));
            List<ExprValue> shard2 = Arrays.asList(
                    row("category", "A", "id", 3),
                    row("category", "A", "id", 4));
            List<ExprValue> shard3 = Arrays.asList(
                    row("category", "B", "id", 5));

            List<NamedAggregator> aggs = Collections.singletonList(
                    namedAgg("cnt", "count", "id", ExprCoreType.INTEGER));
            List<NamedExpression> groupBy = Collections.singletonList(
                    namedExpr("category", ExprCoreType.STRING));

            // Single-node
            List<ExprValue> allRows = new ArrayList<>();
            allRows.addAll(shard1);
            allRows.addAll(shard2);
            allRows.addAll(shard3);
            AggregationOperator singleNode = new AggregationOperator(
                    memoryPlan(allRows), aggs, groupBy);
            singleNode.open();
            List<ExprValue> singleNodeResults = consumeAll(singleNode);
            singleNode.close();

            // Distributed
            List<ExprValue> partialResults = new ArrayList<>();
            for (List<ExprValue> shard : Arrays.asList(shard1, shard2, shard3)) {
                PartialAggregationOperator partial = new PartialAggregationOperator(
                        memoryPlan(shard), aggs, groupBy);
                partial.open();
                partialResults.addAll(consumeAll(partial));
                partial.close();
            }

            DistributedExchangeOperator exchange =
                    new DistributedExchangeOperator(ExchangeSpec.gather(), 1);
            FinalAggregationOperator finalAgg = new FinalAggregationOperator(
                    exchange, aggs, groupBy);
            openWithConcurrentFeed(finalAgg, exchange, partialResults);
            List<ExprValue> distResults = consumeAll(finalAgg);
            finalAgg.close();

            // Both should have 2 groups: A=3, B=2
            assertEquals(singleNodeResults.size(), distResults.size());
            // Sort by category for stable comparison
            Map<String, ExprValue> singleMap = toGroupMap(singleNodeResults, "category");
            Map<String, ExprValue> distMap = toGroupMap(distResults, "category");
            assertEquals(singleMap.get("A").tupleValue().get("cnt"),
                    distMap.get("A").tupleValue().get("cnt"), "COUNT for A");
            assertEquals(singleMap.get("B").tupleValue().get("cnt"),
                    distMap.get("B").tupleValue().get("cnt"), "COUNT for B");
        }
    }

    @Nested
    @DisplayName("stats command: SUM")
    class StatsSum {

        @Test
        @DisplayName("SUM across 3 shards matches single-node")
        void sumDistributed() {
            List<ExprValue> shard1 = Arrays.asList(
                    row("amount", 10), row("amount", 20));
            List<ExprValue> shard2 = Arrays.asList(
                    row("amount", 30));
            List<ExprValue> shard3 = Arrays.asList(
                    row("amount", 40), row("amount", 50));

            List<NamedAggregator> aggs = Collections.singletonList(
                    namedAgg("total", "sum", "amount", ExprCoreType.INTEGER));
            List<NamedExpression> groupBy = Collections.emptyList();

            // Single-node
            List<ExprValue> allRows = new ArrayList<>();
            allRows.addAll(shard1);
            allRows.addAll(shard2);
            allRows.addAll(shard3);
            AggregationOperator singleNode = new AggregationOperator(
                    memoryPlan(allRows), aggs, groupBy);
            singleNode.open();
            List<ExprValue> singleNodeResults = consumeAll(singleNode);
            singleNode.close();

            // Distributed
            List<ExprValue> partialResults = new ArrayList<>();
            for (List<ExprValue> shard : Arrays.asList(shard1, shard2, shard3)) {
                PartialAggregationOperator partial = new PartialAggregationOperator(
                        memoryPlan(shard), aggs, groupBy);
                partial.open();
                partialResults.addAll(consumeAll(partial));
                partial.close();
            }

            DistributedExchangeOperator exchange =
                    new DistributedExchangeOperator(ExchangeSpec.gather(), 1);
            FinalAggregationOperator finalAgg = new FinalAggregationOperator(
                    exchange, aggs, groupBy);
            openWithConcurrentFeed(finalAgg, exchange, partialResults);
            List<ExprValue> distResults = consumeAll(finalAgg);
            finalAgg.close();

            // SUM should be 150
            assertEquals(
                    singleNodeResults.get(0).tupleValue().get("total"),
                    distResults.get(0).tupleValue().get("total"),
                    "SUM result should match");
        }
    }

    @Nested
    @DisplayName("stats command: AVG")
    class StatsAvg {

        @Test
        @DisplayName("AVG across 3 shards matches single-node")
        void avgDistributed() {
            List<ExprValue> shard1 = Arrays.asList(
                    row("amount", 10), row("amount", 20));
            List<ExprValue> shard2 = Arrays.asList(
                    row("amount", 30));
            List<ExprValue> shard3 = Arrays.asList(
                    row("amount", 40), row("amount", 50));

            List<NamedAggregator> aggs = Collections.singletonList(
                    namedAgg("average", "avg", "amount", ExprCoreType.INTEGER));
            List<NamedExpression> groupBy = Collections.emptyList();

            // Single-node
            List<ExprValue> allRows = new ArrayList<>();
            allRows.addAll(shard1);
            allRows.addAll(shard2);
            allRows.addAll(shard3);
            AggregationOperator singleNode = new AggregationOperator(
                    memoryPlan(allRows), aggs, groupBy);
            singleNode.open();
            List<ExprValue> singleNodeResults = consumeAll(singleNode);
            singleNode.close();

            // Distributed
            List<ExprValue> partialResults = new ArrayList<>();
            for (List<ExprValue> shard : Arrays.asList(shard1, shard2, shard3)) {
                PartialAggregationOperator partial = new PartialAggregationOperator(
                        memoryPlan(shard), aggs, groupBy);
                partial.open();
                partialResults.addAll(consumeAll(partial));
                partial.close();
            }

            DistributedExchangeOperator exchange =
                    new DistributedExchangeOperator(ExchangeSpec.gather(), 1);
            FinalAggregationOperator finalAgg = new FinalAggregationOperator(
                    exchange, aggs, groupBy);
            openWithConcurrentFeed(finalAgg, exchange, partialResults);
            List<ExprValue> distResults = consumeAll(finalAgg);
            finalAgg.close();

            // AVG should be 30.0
            assertEquals(
                    singleNodeResults.get(0).tupleValue().get("average"),
                    distResults.get(0).tupleValue().get("average"),
                    "AVG result should match");
        }
    }

    @Nested
    @DisplayName("stats command: MIN/MAX")
    class StatsMinMax {

        @Test
        @DisplayName("MIN and MAX across 3 shards match single-node")
        void minMaxDistributed() {
            List<ExprValue> shard1 = Arrays.asList(
                    row("amount", 10), row("amount", 50));
            List<ExprValue> shard2 = Arrays.asList(
                    row("amount", 5), row("amount", 30));
            List<ExprValue> shard3 = Arrays.asList(
                    row("amount", 100), row("amount", 7));

            List<NamedAggregator> aggs = Arrays.asList(
                    namedAgg("min_val", "min", "amount", ExprCoreType.INTEGER),
                    namedAgg("max_val", "max", "amount", ExprCoreType.INTEGER));
            List<NamedExpression> groupBy = Collections.emptyList();

            // Single-node
            List<ExprValue> allRows = new ArrayList<>();
            allRows.addAll(shard1);
            allRows.addAll(shard2);
            allRows.addAll(shard3);
            AggregationOperator singleNode = new AggregationOperator(
                    memoryPlan(allRows), aggs, groupBy);
            singleNode.open();
            List<ExprValue> singleNodeResults = consumeAll(singleNode);
            singleNode.close();

            // Distributed
            List<ExprValue> partialResults = new ArrayList<>();
            for (List<ExprValue> shard : Arrays.asList(shard1, shard2, shard3)) {
                PartialAggregationOperator partial = new PartialAggregationOperator(
                        memoryPlan(shard), aggs, groupBy);
                partial.open();
                partialResults.addAll(consumeAll(partial));
                partial.close();
            }

            DistributedExchangeOperator exchange =
                    new DistributedExchangeOperator(ExchangeSpec.gather(), 1);
            FinalAggregationOperator finalAgg = new FinalAggregationOperator(
                    exchange, aggs, groupBy);
            openWithConcurrentFeed(finalAgg, exchange, partialResults);
            List<ExprValue> distResults = consumeAll(finalAgg);
            finalAgg.close();

            // MIN=5, MAX=100
            Map<String, ExprValue> singleRow = singleNodeResults.get(0).tupleValue();
            Map<String, ExprValue> distRow = distResults.get(0).tupleValue();
            assertEquals(singleRow.get("min_val"), distRow.get("min_val"), "MIN should match");
            assertEquals(singleRow.get("max_val"), distRow.get("max_val"), "MAX should match");
        }
    }

    @Nested
    @DisplayName("sort + head command")
    class SortAndHead {

        @Test
        @DisplayName("Distributed sort with limit produces correct top-N")
        void sortWithLimit() {
            // Simulate 3 shards each contributing locally sorted data
            List<ExprValue> shard1Sorted = Arrays.asList(
                    row("amount", 10), row("amount", 30), row("amount", 50));
            List<ExprValue> shard2Sorted = Arrays.asList(
                    row("amount", 5), row("amount", 25), row("amount", 45));
            List<ExprValue> shard3Sorted = Arrays.asList(
                    row("amount", 15), row("amount", 35));

            // Combine all shard results
            List<ExprValue> allSortedResults = new ArrayList<>();
            allSortedResults.addAll(shard1Sorted);
            allSortedResults.addAll(shard2Sorted);
            allSortedResults.addAll(shard3Sorted);

            // Create exchange and sort operator with limit 3, ascending
            DistributedExchangeOperator exchange =
                    new DistributedExchangeOperator(ExchangeSpec.gather(), 1);
            List<Pair<SortOption, Expression>> sortSpec = Collections.singletonList(
                    Pair.of(SortOption.DEFAULT_ASC,
                            new ReferenceExpression("amount", ExprCoreType.INTEGER)));

            DistributedSortOperator sort = new DistributedSortOperator(
                    exchange, sortSpec, OptionalLong.of(3));
            openSortWithConcurrentFeed(sort, exchange, allSortedResults);
            List<ExprValue> results = consumeAll(sort);
            sort.close();

            // Top 3 ascending should be: 5, 10, 15
            assertEquals(3, results.size());
            assertEquals(new ExprIntegerValue(5),
                    results.get(0).tupleValue().get("amount"));
            assertEquals(new ExprIntegerValue(10),
                    results.get(1).tupleValue().get("amount"));
            assertEquals(new ExprIntegerValue(15),
                    results.get(2).tupleValue().get("amount"));
        }

        @Test
        @DisplayName("Distributed sort descending with limit")
        void sortDescWithLimit() {
            List<ExprValue> allResults = Arrays.asList(
                    row("amount", 10), row("amount", 50), row("amount", 30),
                    row("amount", 90), row("amount", 70));

            DistributedExchangeOperator exchange =
                    new DistributedExchangeOperator(ExchangeSpec.gather(), 1);

            List<Pair<SortOption, Expression>> sortSpec = Collections.singletonList(
                    Pair.of(SortOption.DEFAULT_DESC,
                            new ReferenceExpression("amount", ExprCoreType.INTEGER)));

            DistributedSortOperator sort = new DistributedSortOperator(
                    exchange, sortSpec, OptionalLong.of(2));
            openSortWithConcurrentFeed(sort, exchange, allResults);
            List<ExprValue> results = consumeAll(sort);
            sort.close();

            // Top 2 descending: 90, 70
            assertEquals(2, results.size());
            assertEquals(new ExprIntegerValue(90),
                    results.get(0).tupleValue().get("amount"));
            assertEquals(new ExprIntegerValue(70),
                    results.get(1).tupleValue().get("amount"));
        }
    }

    @Nested
    @DisplayName("join command")
    class Join {

        @Test
        @DisplayName("Distributed INNER join produces correct matches")
        void innerJoin() {
            List<ExprValue> buildRows = Arrays.asList(
                    row("dept_id", 1, "dept_name", "Engineering"),
                    row("dept_id", 2, "dept_name", "Marketing"),
                    row("dept_id", 3, "dept_name", "Sales"));

            List<ExprValue> probeRows = Arrays.asList(
                    row("emp_name", "Alice", "dept_id", 1),
                    row("emp_name", "Bob", "dept_id", 2),
                    row("emp_name", "Carol", "dept_id", 1),
                    row("emp_name", "Dave", "dept_id", 4));  // No match

            // Feed into exchanges
            DistributedExchangeOperator buildExchange =
                    new DistributedExchangeOperator(ExchangeSpec.gather(), 1);
            DistributedExchangeOperator probeExchange =
                    new DistributedExchangeOperator(ExchangeSpec.gather(), 1);

            List<Expression> buildKeys = Collections.singletonList(
                    new ReferenceExpression("dept_id", ExprCoreType.INTEGER));
            List<Expression> probeKeys = Collections.singletonList(
                    new ReferenceExpression("dept_id", ExprCoreType.INTEGER));

            DistributedHashJoinOperator join = new DistributedHashJoinOperator(
                    buildExchange, probeExchange, buildKeys, probeKeys, JoinType.INNER);

            openJoinWithConcurrentFeed(join, buildExchange, probeExchange,
                    buildRows, probeRows);
            List<ExprValue> results = consumeAll(join);
            join.close();

            // Should have 3 matches (Alice→Eng, Bob→Marketing, Carol→Eng)
            assertEquals(3, results.size(), "INNER join should produce 3 matches");
        }

        @Test
        @DisplayName("Distributed LEFT join includes non-matching probe rows")
        void leftJoin() {
            List<ExprValue> buildRows = Arrays.asList(
                    row("dept_id", 1, "dept_name", "Engineering"));

            List<ExprValue> probeRows = Arrays.asList(
                    row("emp_name", "Alice", "dept_id", 1),
                    row("emp_name", "Bob", "dept_id", 2));  // No match

            DistributedExchangeOperator buildExchange =
                    new DistributedExchangeOperator(ExchangeSpec.gather(), 1);
            DistributedExchangeOperator probeExchange =
                    new DistributedExchangeOperator(ExchangeSpec.gather(), 1);

            List<Expression> buildKeys = Collections.singletonList(
                    new ReferenceExpression("dept_id", ExprCoreType.INTEGER));
            List<Expression> probeKeys = Collections.singletonList(
                    new ReferenceExpression("dept_id", ExprCoreType.INTEGER));

            DistributedHashJoinOperator join = new DistributedHashJoinOperator(
                    buildExchange, probeExchange, buildKeys, probeKeys, JoinType.LEFT);

            openJoinWithConcurrentFeed(join, buildExchange, probeExchange,
                    buildRows, probeRows);
            List<ExprValue> results = consumeAll(join);
            join.close();

            // Should have 2 rows: Alice matched, Bob with null build columns
            assertEquals(2, results.size(), "LEFT join should produce 2 rows");
        }
    }

    @Nested
    @DisplayName("Edge cases")
    class EdgeCases {

        @Test
        @DisplayName("Empty input produces correct aggregation defaults")
        void emptyInput() {
            List<NamedAggregator> aggs = Collections.singletonList(
                    namedAgg("cnt", "count", "*", ExprCoreType.INTEGER));
            List<NamedExpression> groupBy = Collections.emptyList();

            // Single-node: empty input
            AggregationOperator singleNode = new AggregationOperator(
                    memoryPlan(Collections.emptyList()), aggs, groupBy);
            singleNode.open();
            List<ExprValue> singleNodeResults = consumeAll(singleNode);
            singleNode.close();

            // Distributed: empty partial results
            DistributedExchangeOperator exchange =
                    new DistributedExchangeOperator(ExchangeSpec.gather(), 1);
            FinalAggregationOperator finalAgg = new FinalAggregationOperator(
                    exchange, aggs, groupBy);
            openWithConcurrentFeed(finalAgg, exchange, Collections.emptyList());
            List<ExprValue> distResults = consumeAll(finalAgg);
            finalAgg.close();

            // Both should produce one row with count=0
            assertEquals(1, singleNodeResults.size(), "Single-node should emit 1 row");
            assertEquals(1, distResults.size(), "Distributed should emit 1 row");
        }

        @Test
        @DisplayName("Multiple aggregations in single query")
        void multipleAggregations() {
            List<ExprValue> shard1 = Arrays.asList(
                    row("amount", 10), row("amount", 20));
            List<ExprValue> shard2 = Arrays.asList(
                    row("amount", 30), row("amount", 40));

            List<NamedAggregator> aggs = Arrays.asList(
                    namedAgg("cnt", "count", "amount", ExprCoreType.INTEGER),
                    namedAgg("total", "sum", "amount", ExprCoreType.INTEGER),
                    namedAgg("minimum", "min", "amount", ExprCoreType.INTEGER),
                    namedAgg("maximum", "max", "amount", ExprCoreType.INTEGER));
            List<NamedExpression> groupBy = Collections.emptyList();

            // Single-node
            List<ExprValue> allRows = new ArrayList<>();
            allRows.addAll(shard1);
            allRows.addAll(shard2);
            AggregationOperator singleNode = new AggregationOperator(
                    memoryPlan(allRows), aggs, groupBy);
            singleNode.open();
            List<ExprValue> singleNodeResults = consumeAll(singleNode);
            singleNode.close();

            // Distributed
            List<ExprValue> partialResults = new ArrayList<>();
            for (List<ExprValue> shard : Arrays.asList(shard1, shard2)) {
                PartialAggregationOperator partial = new PartialAggregationOperator(
                        memoryPlan(shard), aggs, groupBy);
                partial.open();
                partialResults.addAll(consumeAll(partial));
                partial.close();
            }

            DistributedExchangeOperator exchange =
                    new DistributedExchangeOperator(ExchangeSpec.gather(), 1);
            FinalAggregationOperator finalAgg = new FinalAggregationOperator(
                    exchange, aggs, groupBy);
            openWithConcurrentFeed(finalAgg, exchange, partialResults);
            List<ExprValue> distResults = consumeAll(finalAgg);
            finalAgg.close();

            Map<String, ExprValue> singleRow = singleNodeResults.get(0).tupleValue();
            Map<String, ExprValue> distRow = distResults.get(0).tupleValue();
            assertEquals(singleRow.get("cnt"), distRow.get("cnt"), "COUNT");
            assertEquals(singleRow.get("total"), distRow.get("total"), "SUM");
            assertEquals(singleRow.get("minimum"), distRow.get("minimum"), "MIN");
            assertEquals(singleRow.get("maximum"), distRow.get("maximum"), "MAX");
        }
    }

    /** Helper to index results by a group-by key for stable comparison. */
    private static Map<String, ExprValue> toGroupMap(List<ExprValue> rows, String keyField) {
        Map<String, ExprValue> map = new LinkedHashMap<>();
        for (ExprValue row : rows) {
            String key = row.tupleValue().get(keyField).stringValue();
            map.put(key, row);
        }
        return map;
    }
}
