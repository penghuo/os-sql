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
import java.util.Map;
import java.util.NoSuchElementException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.data.model.ExprNullValue;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.ReferenceExpression;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.planner.physical.PhysicalPlanNodeVisitor;

class DistributedHashJoinOperatorTest {

    /** Helper to create a row (ExprTupleValue) from field name/value pairs. */
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
            } else if (val == null) {
                map.put(key, ExprNullValue.of());
            }
        }
        return ExprTupleValue.fromExprValueMap(map);
    }

    /** Collect all results from a join operator. */
    private static List<ExprValue> collect(DistributedHashJoinOperator op) {
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

    // --- INNER JOIN tests ---

    @Test
    @DisplayName("INNER join with matching rows")
    void innerJoinWithMatches() {
        TestPlan build = new TestPlan(Arrays.asList(
                row("id", 1, "name", "Alice"),
                row("id", 2, "name", "Bob")));
        TestPlan probe = new TestPlan(Arrays.asList(
                row("id", 1, "dept", "Eng"),
                row("id", 2, "dept", "Sales")));

        Expression buildKey = new ReferenceExpression("id", ExprCoreType.INTEGER);
        Expression probeKey = new ReferenceExpression("id", ExprCoreType.INTEGER);

        DistributedHashJoinOperator op = new DistributedHashJoinOperator(
                build, probe, List.of(buildKey), List.of(probeKey), JoinType.INNER);

        List<ExprValue> results = collect(op);
        assertEquals(2, results.size());

        // Probe row 1 (id=1) matches build row (id=1, name=Alice)
        Map<String, ExprValue> row1 = results.get(0).tupleValue();
        assertEquals(ExprValueUtils.integerValue(1), row1.get("id"));
        assertEquals(ExprValueUtils.stringValue("Eng"), row1.get("dept"));
        assertEquals(ExprValueUtils.stringValue("Alice"), row1.get("name"));

        // Probe row 2 (id=2) matches build row (id=2, name=Bob)
        Map<String, ExprValue> row2 = results.get(1).tupleValue();
        assertEquals(ExprValueUtils.integerValue(2), row2.get("id"));
        assertEquals(ExprValueUtils.stringValue("Sales"), row2.get("dept"));
        assertEquals(ExprValueUtils.stringValue("Bob"), row2.get("name"));
    }

    @Test
    @DisplayName("INNER join with non-matching rows produces no output")
    void innerJoinNoMatches() {
        TestPlan build = new TestPlan(Arrays.asList(
                row("id", 1, "name", "Alice")));
        TestPlan probe = new TestPlan(Arrays.asList(
                row("id", 99, "dept", "Eng")));

        Expression buildKey = new ReferenceExpression("id", ExprCoreType.INTEGER);
        Expression probeKey = new ReferenceExpression("id", ExprCoreType.INTEGER);

        DistributedHashJoinOperator op = new DistributedHashJoinOperator(
                build, probe, List.of(buildKey), List.of(probeKey), JoinType.INNER);

        List<ExprValue> results = collect(op);
        assertEquals(0, results.size());
    }

    @Test
    @DisplayName("INNER join with 1:N (multiple matches for same key)")
    void innerJoinOneToMany() {
        TestPlan build = new TestPlan(Arrays.asList(
                row("id", 1, "item", "A"),
                row("id", 1, "item", "B"),
                row("id", 1, "item", "C")));
        TestPlan probe = new TestPlan(Arrays.asList(
                row("id", 1, "buyer", "X")));

        Expression buildKey = new ReferenceExpression("id", ExprCoreType.INTEGER);
        Expression probeKey = new ReferenceExpression("id", ExprCoreType.INTEGER);

        DistributedHashJoinOperator op = new DistributedHashJoinOperator(
                build, probe, List.of(buildKey), List.of(probeKey), JoinType.INNER);

        List<ExprValue> results = collect(op);
        assertEquals(3, results.size());

        // All three build rows should match the single probe row
        for (ExprValue result : results) {
            assertEquals(ExprValueUtils.stringValue("X"), result.tupleValue().get("buyer"));
        }
        assertEquals(ExprValueUtils.stringValue("A"), results.get(0).tupleValue().get("item"));
        assertEquals(ExprValueUtils.stringValue("B"), results.get(1).tupleValue().get("item"));
        assertEquals(ExprValueUtils.stringValue("C"), results.get(2).tupleValue().get("item"));
    }

    @Test
    @DisplayName("INNER join: NULL key on probe side does not match")
    void innerJoinNullKeyNoMatch() {
        TestPlan build = new TestPlan(Arrays.asList(
                row("id", 1, "name", "Alice")));
        TestPlan probe = new TestPlan(Arrays.asList(
                row("id", ExprNullValue.of(), "dept", "Eng")));

        Expression buildKey = new ReferenceExpression("id", ExprCoreType.INTEGER);
        Expression probeKey = new ReferenceExpression("id", ExprCoreType.INTEGER);

        DistributedHashJoinOperator op = new DistributedHashJoinOperator(
                build, probe, List.of(buildKey), List.of(probeKey), JoinType.INNER);

        List<ExprValue> results = collect(op);
        assertEquals(0, results.size());
    }

    // --- LEFT JOIN tests ---

    @Test
    @DisplayName("LEFT join: unmatched probe rows get null build columns")
    void leftJoinUnmatchedProbe() {
        TestPlan build = new TestPlan(Arrays.asList(
                row("id", 1, "name", "Alice")));
        TestPlan probe = new TestPlan(Arrays.asList(
                row("id", 1, "dept", "Eng"),
                row("id", 99, "dept", "HR")));

        Expression buildKey = new ReferenceExpression("id", ExprCoreType.INTEGER);
        Expression probeKey = new ReferenceExpression("id", ExprCoreType.INTEGER);

        DistributedHashJoinOperator op = new DistributedHashJoinOperator(
                build, probe, List.of(buildKey), List.of(probeKey), JoinType.LEFT);

        List<ExprValue> results = collect(op);
        assertEquals(2, results.size());

        // First row: matched
        Map<String, ExprValue> matched = results.get(0).tupleValue();
        assertEquals(ExprValueUtils.stringValue("Alice"), matched.get("name"));
        assertEquals(ExprValueUtils.stringValue("Eng"), matched.get("dept"));

        // Second row: unmatched probe, build columns are NULL
        Map<String, ExprValue> unmatched = results.get(1).tupleValue();
        assertEquals(ExprValueUtils.stringValue("HR"), unmatched.get("dept"));
        assertTrue(unmatched.get("name").isNull());
    }

    @Test
    @DisplayName("LEFT join: all probe rows unmatched")
    void leftJoinAllUnmatched() {
        TestPlan build = new TestPlan(Collections.emptyList());
        TestPlan probe = new TestPlan(Arrays.asList(
                row("id", 1, "dept", "Eng"),
                row("id", 2, "dept", "Sales")));

        Expression buildKey = new ReferenceExpression("id", ExprCoreType.INTEGER);
        Expression probeKey = new ReferenceExpression("id", ExprCoreType.INTEGER);

        DistributedHashJoinOperator op = new DistributedHashJoinOperator(
                build, probe, List.of(buildKey), List.of(probeKey), JoinType.LEFT);

        List<ExprValue> results = collect(op);
        assertEquals(2, results.size());

        // Both rows should have probe columns but no build columns (empty build side)
        for (ExprValue result : results) {
            assertTrue(result.tupleValue().containsKey("dept"));
        }
    }

    // --- RIGHT JOIN tests ---

    @Test
    @DisplayName("RIGHT join: unmatched build rows get null probe columns")
    void rightJoinUnmatchedBuild() {
        TestPlan build = new TestPlan(Arrays.asList(
                row("id", 1, "name", "Alice"),
                row("id", 2, "name", "Bob")));
        TestPlan probe = new TestPlan(Arrays.asList(
                row("id", 1, "dept", "Eng")));

        Expression buildKey = new ReferenceExpression("id", ExprCoreType.INTEGER);
        Expression probeKey = new ReferenceExpression("id", ExprCoreType.INTEGER);

        DistributedHashJoinOperator op = new DistributedHashJoinOperator(
                build, probe, List.of(buildKey), List.of(probeKey), JoinType.RIGHT);

        List<ExprValue> results = collect(op);
        assertEquals(2, results.size());

        // First row: matched (probe id=1 + build id=1)
        Map<String, ExprValue> matched = results.get(0).tupleValue();
        assertEquals(ExprValueUtils.stringValue("Alice"), matched.get("name"));
        assertEquals(ExprValueUtils.stringValue("Eng"), matched.get("dept"));

        // Second row: unmatched build (id=2), probe columns NULL
        Map<String, ExprValue> unmatched = results.get(1).tupleValue();
        assertEquals(ExprValueUtils.stringValue("Bob"), unmatched.get("name"));
        assertTrue(unmatched.get("dept").isNull());
    }

    @Test
    @DisplayName("RIGHT join: build row with NULL key emitted as unmatched")
    void rightJoinNullBuildKey() {
        TestPlan build = new TestPlan(Arrays.asList(
                row("id", ExprNullValue.of(), "name", "Ghost")));
        TestPlan probe = new TestPlan(Arrays.asList(
                row("id", 1, "dept", "Eng")));

        Expression buildKey = new ReferenceExpression("id", ExprCoreType.INTEGER);
        Expression probeKey = new ReferenceExpression("id", ExprCoreType.INTEGER);

        DistributedHashJoinOperator op = new DistributedHashJoinOperator(
                build, probe, List.of(buildKey), List.of(probeKey), JoinType.RIGHT);

        List<ExprValue> results = collect(op);
        // No match for probe (id=1), no match for build (NULL key)
        // Only the unmatched build row should be emitted
        assertEquals(1, results.size());
        Map<String, ExprValue> row = results.get(0).tupleValue();
        assertEquals(ExprValueUtils.stringValue("Ghost"), row.get("name"));
        assertTrue(row.get("dept").isNull());
    }

    // --- CROSS JOIN tests ---

    @Test
    @DisplayName("CROSS join produces cartesian product")
    void crossJoin() {
        TestPlan build = new TestPlan(Arrays.asList(
                row("x", 1),
                row("x", 2)));
        TestPlan probe = new TestPlan(Arrays.asList(
                row("y", 10),
                row("y", 20),
                row("y", 30)));

        DistributedHashJoinOperator op = new DistributedHashJoinOperator(
                build, probe, Collections.emptyList(), Collections.emptyList(), JoinType.CROSS);

        List<ExprValue> results = collect(op);
        assertEquals(6, results.size()); // 2 x 3 = 6
    }

    // --- Edge cases ---

    @Test
    @DisplayName("Empty build side produces no INNER join results")
    void emptyBuildSide() {
        TestPlan build = new TestPlan(Collections.emptyList());
        TestPlan probe = new TestPlan(Arrays.asList(
                row("id", 1, "dept", "Eng")));

        Expression buildKey = new ReferenceExpression("id", ExprCoreType.INTEGER);
        Expression probeKey = new ReferenceExpression("id", ExprCoreType.INTEGER);

        DistributedHashJoinOperator op = new DistributedHashJoinOperator(
                build, probe, List.of(buildKey), List.of(probeKey), JoinType.INNER);

        List<ExprValue> results = collect(op);
        assertEquals(0, results.size());
    }

    @Test
    @DisplayName("Empty probe side produces no INNER join results")
    void emptyProbeSide() {
        TestPlan build = new TestPlan(Arrays.asList(
                row("id", 1, "name", "Alice")));
        TestPlan probe = new TestPlan(Collections.emptyList());

        Expression buildKey = new ReferenceExpression("id", ExprCoreType.INTEGER);
        Expression probeKey = new ReferenceExpression("id", ExprCoreType.INTEGER);

        DistributedHashJoinOperator op = new DistributedHashJoinOperator(
                build, probe, List.of(buildKey), List.of(probeKey), JoinType.INNER);

        List<ExprValue> results = collect(op);
        assertEquals(0, results.size());
    }

    @Test
    @DisplayName("Empty probe side with RIGHT join emits all build rows")
    void emptyProbeSideRightJoin() {
        TestPlan build = new TestPlan(Arrays.asList(
                row("id", 1, "name", "Alice"),
                row("id", 2, "name", "Bob")));
        TestPlan probe = new TestPlan(Collections.emptyList());

        Expression buildKey = new ReferenceExpression("id", ExprCoreType.INTEGER);
        Expression probeKey = new ReferenceExpression("id", ExprCoreType.INTEGER);

        DistributedHashJoinOperator op = new DistributedHashJoinOperator(
                build, probe, List.of(buildKey), List.of(probeKey), JoinType.RIGHT);

        List<ExprValue> results = collect(op);
        assertEquals(2, results.size());
        // Both build rows emitted as unmatched
        for (ExprValue result : results) {
            assertTrue(result.tupleValue().containsKey("name"));
        }
    }

    @Test
    @DisplayName("Both sides empty produces no results")
    void bothSidesEmpty() {
        TestPlan build = new TestPlan(Collections.emptyList());
        TestPlan probe = new TestPlan(Collections.emptyList());

        Expression buildKey = new ReferenceExpression("id", ExprCoreType.INTEGER);
        Expression probeKey = new ReferenceExpression("id", ExprCoreType.INTEGER);

        DistributedHashJoinOperator op = new DistributedHashJoinOperator(
                build, probe, List.of(buildKey), List.of(probeKey), JoinType.INNER);

        List<ExprValue> results = collect(op);
        assertEquals(0, results.size());
    }

    @Test
    @DisplayName("getChild returns both children")
    void getChildReturnsBoth() {
        TestPlan build = new TestPlan(Collections.emptyList());
        TestPlan probe = new TestPlan(Collections.emptyList());

        DistributedHashJoinOperator op = new DistributedHashJoinOperator(
                build, probe, Collections.emptyList(), Collections.emptyList(), JoinType.INNER);

        List<PhysicalPlan> children = op.getChild();
        assertEquals(2, children.size());
        assertEquals(build, children.get(0));
        assertEquals(probe, children.get(1));
    }

    @Test
    @DisplayName("next() throws NoSuchElementException when exhausted")
    void nextThrowsWhenExhausted() {
        TestPlan build = new TestPlan(Collections.emptyList());
        TestPlan probe = new TestPlan(Collections.emptyList());

        Expression buildKey = new ReferenceExpression("id", ExprCoreType.INTEGER);
        Expression probeKey = new ReferenceExpression("id", ExprCoreType.INTEGER);

        DistributedHashJoinOperator op = new DistributedHashJoinOperator(
                build, probe, List.of(buildKey), List.of(probeKey), JoinType.INNER);
        op.open();

        assertFalse(op.hasNext());
        assertThrows(NoSuchElementException.class, op::next);
        op.close();
    }

    @Test
    @DisplayName("INNER join with composite key (multiple join columns)")
    void innerJoinCompositeKey() {
        TestPlan build = new TestPlan(Arrays.asList(
                row("a", 1, "b", 10, "val", "X"),
                row("a", 1, "b", 20, "val", "Y"),
                row("a", 2, "b", 10, "val", "Z")));
        TestPlan probe = new TestPlan(Arrays.asList(
                row("a", 1, "b", 10, "info", "p1"),
                row("a", 2, "b", 10, "info", "p2"),
                row("a", 3, "b", 30, "info", "p3")));

        List<Expression> buildKeys = List.of(
                new ReferenceExpression("a", ExprCoreType.INTEGER),
                new ReferenceExpression("b", ExprCoreType.INTEGER));
        List<Expression> probeKeys = List.of(
                new ReferenceExpression("a", ExprCoreType.INTEGER),
                new ReferenceExpression("b", ExprCoreType.INTEGER));

        DistributedHashJoinOperator op = new DistributedHashJoinOperator(
                build, probe, buildKeys, probeKeys, JoinType.INNER);

        List<ExprValue> results = collect(op);
        assertEquals(2, results.size());

        // (a=1, b=10) matches build row with val=X
        assertEquals(ExprValueUtils.stringValue("X"), results.get(0).tupleValue().get("val"));
        assertEquals(ExprValueUtils.stringValue("p1"), results.get(0).tupleValue().get("info"));

        // (a=2, b=10) matches build row with val=Z
        assertEquals(ExprValueUtils.stringValue("Z"), results.get(1).tupleValue().get("val"));
        assertEquals(ExprValueUtils.stringValue("p2"), results.get(1).tupleValue().get("info"));
    }

    @Test
    @DisplayName("M:N join produces M*N rows per matching key")
    void manyToManyJoin() {
        TestPlan build = new TestPlan(Arrays.asList(
                row("id", 1, "bval", "b1"),
                row("id", 1, "bval", "b2")));
        TestPlan probe = new TestPlan(Arrays.asList(
                row("id", 1, "pval", "p1"),
                row("id", 1, "pval", "p2"),
                row("id", 1, "pval", "p3")));

        Expression buildKey = new ReferenceExpression("id", ExprCoreType.INTEGER);
        Expression probeKey = new ReferenceExpression("id", ExprCoreType.INTEGER);

        DistributedHashJoinOperator op = new DistributedHashJoinOperator(
                build, probe, List.of(buildKey), List.of(probeKey), JoinType.INNER);

        List<ExprValue> results = collect(op);
        // 3 probe rows x 2 build rows = 6
        assertEquals(6, results.size());
    }
}
