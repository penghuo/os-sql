/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.physical.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.OptionalLong;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.ast.tree.Sort.NullOrder;
import org.opensearch.sql.ast.tree.Sort.SortOption;
import org.opensearch.sql.ast.tree.Sort.SortOrder;
import org.opensearch.sql.data.model.ExprNullValue;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.ReferenceExpression;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.planner.physical.PhysicalPlanNodeVisitor;
import org.opensearch.sql.planner.physical.SortOperator;

class DistributedSortOperatorTest {

    /** Helper to create a row with a single integer field. */
    private static ExprValue row(String field, int value) {
        LinkedHashMap<String, ExprValue> map = new LinkedHashMap<>();
        map.put(field, ExprValueUtils.integerValue(value));
        return ExprTupleValue.fromExprValueMap(map);
    }

    /** Helper to create a row with multiple fields. */
    private static ExprValue row(Object... fieldsAndValues) {
        LinkedHashMap<String, ExprValue> map = new LinkedHashMap<>();
        for (int i = 0; i < fieldsAndValues.length; i += 2) {
            String key = (String) fieldsAndValues[i];
            Object val = fieldsAndValues[i + 1];
            if (val instanceof ExprValue) {
                map.put(key, (ExprValue) val);
            } else if (val instanceof Integer) {
                map.put(key, ExprValueUtils.integerValue((Integer) val));
            } else if (val instanceof String) {
                map.put(key, ExprValueUtils.stringValue((String) val));
            }
        }
        return ExprTupleValue.fromExprValueMap(map);
    }

    /** Collect all results from a sort operator. */
    private static List<ExprValue> collect(PhysicalPlan op) {
        op.open();
        List<ExprValue> results = new ArrayList<>();
        while (op.hasNext()) {
            results.add(op.next());
        }
        op.close();
        return results;
    }

    /** In-memory PhysicalPlan that returns a fixed list of rows. */
    private static class TestPlan extends PhysicalPlan {
        private final List<ExprValue> rows;
        private Iterator<ExprValue> iterator;

        TestPlan(List<ExprValue> rows) {
            this.rows = rows;
        }

        @Override
        public <R, C> R accept(PhysicalPlanNodeVisitor<R, C> visitor, C context) {
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
    }

    @Test
    @DisplayName("Sort ascending on single field")
    void sortAscending() {
        TestPlan input = new TestPlan(Arrays.asList(
                row("val", 3),
                row("val", 1),
                row("val", 2)));

        Expression sortExpr = new ReferenceExpression("val", ExprCoreType.INTEGER);
        List<Pair<SortOption, Expression>> sortList = List.of(
                Pair.of(SortOption.DEFAULT_ASC, sortExpr));

        DistributedSortOperator op = new DistributedSortOperator(
                input, sortList, OptionalLong.empty());

        List<ExprValue> results = collect(op);
        assertEquals(3, results.size());
        assertEquals(ExprValueUtils.integerValue(1), results.get(0).tupleValue().get("val"));
        assertEquals(ExprValueUtils.integerValue(2), results.get(1).tupleValue().get("val"));
        assertEquals(ExprValueUtils.integerValue(3), results.get(2).tupleValue().get("val"));
    }

    @Test
    @DisplayName("Sort descending on single field")
    void sortDescending() {
        TestPlan input = new TestPlan(Arrays.asList(
                row("val", 1),
                row("val", 3),
                row("val", 2)));

        Expression sortExpr = new ReferenceExpression("val", ExprCoreType.INTEGER);
        List<Pair<SortOption, Expression>> sortList = List.of(
                Pair.of(SortOption.DEFAULT_DESC, sortExpr));

        DistributedSortOperator op = new DistributedSortOperator(
                input, sortList, OptionalLong.empty());

        List<ExprValue> results = collect(op);
        assertEquals(3, results.size());
        assertEquals(ExprValueUtils.integerValue(3), results.get(0).tupleValue().get("val"));
        assertEquals(ExprValueUtils.integerValue(2), results.get(1).tupleValue().get("val"));
        assertEquals(ExprValueUtils.integerValue(1), results.get(2).tupleValue().get("val"));
    }

    @Test
    @DisplayName("Sort with multiple fields")
    void sortMultipleFields() {
        TestPlan input = new TestPlan(Arrays.asList(
                row("a", 2, "b", 1),
                row("a", 1, "b", 2),
                row("a", 1, "b", 1),
                row("a", 2, "b", 2)));

        Expression sortA = new ReferenceExpression("a", ExprCoreType.INTEGER);
        Expression sortB = new ReferenceExpression("b", ExprCoreType.INTEGER);
        List<Pair<SortOption, Expression>> sortList = List.of(
                Pair.of(SortOption.DEFAULT_ASC, sortA),
                Pair.of(SortOption.DEFAULT_ASC, sortB));

        DistributedSortOperator op = new DistributedSortOperator(
                input, sortList, OptionalLong.empty());

        List<ExprValue> results = collect(op);
        assertEquals(4, results.size());

        // Expected order: (1,1), (1,2), (2,1), (2,2)
        assertEquals(ExprValueUtils.integerValue(1), results.get(0).tupleValue().get("a"));
        assertEquals(ExprValueUtils.integerValue(1), results.get(0).tupleValue().get("b"));
        assertEquals(ExprValueUtils.integerValue(1), results.get(1).tupleValue().get("a"));
        assertEquals(ExprValueUtils.integerValue(2), results.get(1).tupleValue().get("b"));
        assertEquals(ExprValueUtils.integerValue(2), results.get(2).tupleValue().get("a"));
        assertEquals(ExprValueUtils.integerValue(1), results.get(2).tupleValue().get("b"));
        assertEquals(ExprValueUtils.integerValue(2), results.get(3).tupleValue().get("a"));
        assertEquals(ExprValueUtils.integerValue(2), results.get(3).tupleValue().get("b"));
    }

    @Test
    @DisplayName("Sort with LIMIT returns only top-N rows")
    void sortWithLimit() {
        TestPlan input = new TestPlan(Arrays.asList(
                row("val", 5),
                row("val", 3),
                row("val", 1),
                row("val", 4),
                row("val", 2)));

        Expression sortExpr = new ReferenceExpression("val", ExprCoreType.INTEGER);
        List<Pair<SortOption, Expression>> sortList = List.of(
                Pair.of(SortOption.DEFAULT_ASC, sortExpr));

        DistributedSortOperator op = new DistributedSortOperator(
                input, sortList, OptionalLong.of(3));

        List<ExprValue> results = collect(op);
        assertEquals(3, results.size());
        assertEquals(ExprValueUtils.integerValue(1), results.get(0).tupleValue().get("val"));
        assertEquals(ExprValueUtils.integerValue(2), results.get(1).tupleValue().get("val"));
        assertEquals(ExprValueUtils.integerValue(3), results.get(2).tupleValue().get("val"));
    }

    @Test
    @DisplayName("Sort with LIMIT of 0 returns no rows")
    void sortWithLimitZero() {
        TestPlan input = new TestPlan(Arrays.asList(
                row("val", 1),
                row("val", 2)));

        Expression sortExpr = new ReferenceExpression("val", ExprCoreType.INTEGER);
        List<Pair<SortOption, Expression>> sortList = List.of(
                Pair.of(SortOption.DEFAULT_ASC, sortExpr));

        DistributedSortOperator op = new DistributedSortOperator(
                input, sortList, OptionalLong.of(0));

        List<ExprValue> results = collect(op);
        assertEquals(0, results.size());
    }

    @Test
    @DisplayName("Sort with LIMIT exceeding input returns all rows")
    void sortWithLimitExceedsInput() {
        TestPlan input = new TestPlan(Arrays.asList(
                row("val", 2),
                row("val", 1)));

        Expression sortExpr = new ReferenceExpression("val", ExprCoreType.INTEGER);
        List<Pair<SortOption, Expression>> sortList = List.of(
                Pair.of(SortOption.DEFAULT_ASC, sortExpr));

        DistributedSortOperator op = new DistributedSortOperator(
                input, sortList, OptionalLong.of(100));

        List<ExprValue> results = collect(op);
        assertEquals(2, results.size());
        assertEquals(ExprValueUtils.integerValue(1), results.get(0).tupleValue().get("val"));
        assertEquals(ExprValueUtils.integerValue(2), results.get(1).tupleValue().get("val"));
    }

    @Test
    @DisplayName("NULL handling: NULL_FIRST")
    void nullFirst() {
        LinkedHashMap<String, ExprValue> nullRow = new LinkedHashMap<>();
        nullRow.put("val", ExprNullValue.of());

        TestPlan input = new TestPlan(Arrays.asList(
                row("val", 2),
                ExprTupleValue.fromExprValueMap(nullRow),
                row("val", 1)));

        Expression sortExpr = new ReferenceExpression("val", ExprCoreType.INTEGER);
        SortOption nullFirst = new SortOption(SortOrder.ASC, NullOrder.NULL_FIRST);
        List<Pair<SortOption, Expression>> sortList = List.of(
                Pair.of(nullFirst, sortExpr));

        DistributedSortOperator op = new DistributedSortOperator(
                input, sortList, OptionalLong.empty());

        List<ExprValue> results = collect(op);
        assertEquals(3, results.size());
        assertTrue(results.get(0).tupleValue().get("val").isNull());
        assertEquals(ExprValueUtils.integerValue(1), results.get(1).tupleValue().get("val"));
        assertEquals(ExprValueUtils.integerValue(2), results.get(2).tupleValue().get("val"));
    }

    @Test
    @DisplayName("NULL handling: NULL_LAST")
    void nullLast() {
        LinkedHashMap<String, ExprValue> nullRow = new LinkedHashMap<>();
        nullRow.put("val", ExprNullValue.of());

        TestPlan input = new TestPlan(Arrays.asList(
                row("val", 2),
                ExprTupleValue.fromExprValueMap(nullRow),
                row("val", 1)));

        Expression sortExpr = new ReferenceExpression("val", ExprCoreType.INTEGER);
        SortOption nullLast = new SortOption(SortOrder.ASC, NullOrder.NULL_LAST);
        List<Pair<SortOption, Expression>> sortList = List.of(
                Pair.of(nullLast, sortExpr));

        DistributedSortOperator op = new DistributedSortOperator(
                input, sortList, OptionalLong.empty());

        List<ExprValue> results = collect(op);
        assertEquals(3, results.size());
        assertEquals(ExprValueUtils.integerValue(1), results.get(0).tupleValue().get("val"));
        assertEquals(ExprValueUtils.integerValue(2), results.get(1).tupleValue().get("val"));
        assertTrue(results.get(2).tupleValue().get("val").isNull());
    }

    @Test
    @DisplayName("Empty input")
    void emptyInput() {
        TestPlan input = new TestPlan(Collections.emptyList());

        Expression sortExpr = new ReferenceExpression("val", ExprCoreType.INTEGER);
        List<Pair<SortOption, Expression>> sortList = List.of(
                Pair.of(SortOption.DEFAULT_ASC, sortExpr));

        DistributedSortOperator op = new DistributedSortOperator(
                input, sortList, OptionalLong.empty());

        List<ExprValue> results = collect(op);
        assertEquals(0, results.size());
    }

    @Test
    @DisplayName("Single row input")
    void singleRow() {
        TestPlan input = new TestPlan(Arrays.asList(row("val", 42)));

        Expression sortExpr = new ReferenceExpression("val", ExprCoreType.INTEGER);
        List<Pair<SortOption, Expression>> sortList = List.of(
                Pair.of(SortOption.DEFAULT_ASC, sortExpr));

        DistributedSortOperator op = new DistributedSortOperator(
                input, sortList, OptionalLong.empty());

        List<ExprValue> results = collect(op);
        assertEquals(1, results.size());
        assertEquals(ExprValueUtils.integerValue(42), results.get(0).tupleValue().get("val"));
    }

    @Test
    @DisplayName("next() throws NoSuchElementException when exhausted")
    void nextThrowsWhenExhausted() {
        TestPlan input = new TestPlan(Collections.emptyList());

        Expression sortExpr = new ReferenceExpression("val", ExprCoreType.INTEGER);
        List<Pair<SortOption, Expression>> sortList = List.of(
                Pair.of(SortOption.DEFAULT_ASC, sortExpr));

        DistributedSortOperator op = new DistributedSortOperator(
                input, sortList, OptionalLong.empty());
        op.open();

        assertFalse(op.hasNext());
        assertThrows(NoSuchElementException.class, op::next);
        op.close();
    }

    @Test
    @DisplayName("Distributed sort matches single-node SortOperator on same data")
    void matchesSingleNodeSort() {
        List<ExprValue> data = Arrays.asList(
                row("val", 5),
                row("val", 3),
                row("val", 1),
                row("val", 4),
                row("val", 2));

        Expression sortExpr = new ReferenceExpression("val", ExprCoreType.INTEGER);
        List<Pair<SortOption, Expression>> sortList = List.of(
                Pair.of(SortOption.DEFAULT_ASC, sortExpr));

        // Single-node SortOperator
        SortOperator singleNode = new SortOperator(
                new TestPlan(data), sortList);
        List<ExprValue> singleNodeResults = collect(singleNode);

        // Distributed sort
        DistributedSortOperator distributed = new DistributedSortOperator(
                new TestPlan(data), sortList, OptionalLong.empty());
        List<ExprValue> distributedResults = collect(distributed);

        assertEquals(singleNodeResults.size(), distributedResults.size());
        for (int i = 0; i < singleNodeResults.size(); i++) {
            assertEquals(
                    singleNodeResults.get(i).tupleValue().get("val"),
                    distributedResults.get(i).tupleValue().get("val"));
        }
    }

    @Test
    @DisplayName("getChild returns input child")
    void getChildReturnsInput() {
        TestPlan input = new TestPlan(Collections.emptyList());

        DistributedSortOperator op = new DistributedSortOperator(
                input, Collections.emptyList(), OptionalLong.empty());

        assertEquals(1, op.getChild().size());
        assertEquals(input, op.getChild().get(0));
    }
}
