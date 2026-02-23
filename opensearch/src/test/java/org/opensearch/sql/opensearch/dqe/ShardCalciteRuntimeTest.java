/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.dqe;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.type.SqlTypeName;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.opensearch.sql.calcite.plan.Scannable;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;
import org.opensearch.sql.opensearch.client.OpenSearchClient;
import org.opensearch.sql.opensearch.dqe.serde.RelNodeSerializer;
import org.opensearch.sql.opensearch.storage.OpenSearchIndex;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class ShardCalciteRuntimeTest {

    private ShardCalciteRuntime runtime;
    private RelOptCluster cluster;
    private RexBuilder rexBuilder;
    private OpenSearchIndex mockOsIndex;

    @BeforeEach
    void setUp() {
        runtime = new ShardCalciteRuntime();
        rexBuilder = new RexBuilder(OpenSearchTypeFactory.TYPE_FACTORY);
        VolcanoPlanner planner = new VolcanoPlanner();
        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
        cluster = RelOptCluster.create(planner, rexBuilder);

        OpenSearchClient mockClient = mock(OpenSearchClient.class);
        org.opensearch.sql.common.setting.Settings mockSettings =
                mock(org.opensearch.sql.common.setting.Settings.class);
        mockOsIndex = mock(OpenSearchIndex.class);
        // Provide a minimal row type for schema resolution
        RelDataType minimalRowType =
                OpenSearchTypeFactory.TYPE_FACTORY
                        .builder()
                        .add("dummy", SqlTypeName.VARCHAR)
                        .build();
        when(mockOsIndex.getRowType(any())).thenReturn(minimalRowType);
        when(mockOsIndex.getFieldTypes()).thenReturn(Map.of());
    }

    @Test
    @DisplayName("Deserialize and execute a simple scan plan returns rows")
    void execute_simpleScanPlan_returnsRows() {
        // Create a mock Scannable RelNode that returns known rows
        RelDataType rowType =
                OpenSearchTypeFactory.TYPE_FACTORY
                        .builder()
                        .add("name", SqlTypeName.VARCHAR)
                        .add("age", SqlTypeName.INTEGER)
                        .build();

        Object[][] expectedData = {
            {"Alice", 30},
            {"Bob", 25},
            {"Charlie", 35}
        };

        ScannableRelNode mockPlan = new ScannableRelNode(cluster, rowType, expectedData);
        String fakePlanJson = "{\"rels\":[{\"fake\":\"plan\"}]}";

        try (MockedStatic<RelNodeSerializer> serializer = mockStatic(RelNodeSerializer.class)) {
            serializer
                    .when(() -> RelNodeSerializer.deserialize(eq(fakePlanJson), any(), any()))
                    .thenReturn(mockPlan);

            ShardCalciteRuntime.Result result = runtime.execute(fakePlanJson, "test-index", 0, mockOsIndex);

            assertFalse(result.hasError(), "Result should not have an error");
            assertNull(result.getError());
            assertEquals(3, result.getRows().size());
            assertEquals(List.of("name", "age"), result.getColumnNames());
            assertEquals(
                    List.of(SqlTypeName.VARCHAR, SqlTypeName.INTEGER), result.getColumnTypes());

            // Verify row data
            assertEquals("Alice", result.getRows().get(0)[0]);
            assertEquals(30, result.getRows().get(0)[1]);
            assertEquals("Bob", result.getRows().get(1)[0]);
            assertEquals(25, result.getRows().get(1)[1]);
            assertEquals("Charlie", result.getRows().get(2)[0]);
            assertEquals(35, result.getRows().get(2)[1]);
        }
    }

    @Test
    @DisplayName("Deserialize and execute filter + project plan returns filtered/projected rows")
    void execute_filterProjectPlan_returnsFilteredProjectedRows() {
        // Simulate a plan that returns only projected columns after filtering
        RelDataType rowType =
                OpenSearchTypeFactory.TYPE_FACTORY
                        .builder()
                        .add("name", SqlTypeName.VARCHAR)
                        .build();

        // After filter + project, only "Alice" remains with just the name column
        Object[][] filteredData = {{"Alice"}};

        ScannableRelNode mockPlan = new ScannableRelNode(cluster, rowType, filteredData);
        String fakePlanJson = "{\"rels\":[{\"filter-project\":\"plan\"}]}";

        try (MockedStatic<RelNodeSerializer> serializer = mockStatic(RelNodeSerializer.class)) {
            serializer
                    .when(() -> RelNodeSerializer.deserialize(eq(fakePlanJson), any(), any()))
                    .thenReturn(mockPlan);

            ShardCalciteRuntime.Result result = runtime.execute(fakePlanJson, "test-index", 0, mockOsIndex);

            assertFalse(result.hasError());
            assertEquals(1, result.getRows().size());
            assertEquals(List.of("name"), result.getColumnNames());
            assertEquals(List.of(SqlTypeName.VARCHAR), result.getColumnTypes());

            // Single-column result — verify wrapped correctly
            assertEquals("Alice", result.getRows().get(0)[0]);
        }
    }

    @Test
    @DisplayName("Deserialize and execute partial aggregate plan returns aggregated rows")
    void execute_partialAggregatePlan_returnsAggregatedRows() {
        // Simulate a partial aggregate result: SUM(salary) grouped by department
        RelDataType rowType =
                OpenSearchTypeFactory.TYPE_FACTORY
                        .builder()
                        .add("department", SqlTypeName.VARCHAR)
                        .add("total_salary", SqlTypeName.DOUBLE)
                        .add("cnt", SqlTypeName.BIGINT)
                        .build();

        Object[][] aggData = {
            {"Engineering", 250000.0, 5L},
            {"Marketing", 150000.0, 3L}
        };

        ScannableRelNode mockPlan = new ScannableRelNode(cluster, rowType, aggData);
        String fakePlanJson = "{\"rels\":[{\"partial-agg\":\"plan\"}]}";

        try (MockedStatic<RelNodeSerializer> serializer = mockStatic(RelNodeSerializer.class)) {
            serializer
                    .when(() -> RelNodeSerializer.deserialize(eq(fakePlanJson), any(), any()))
                    .thenReturn(mockPlan);

            ShardCalciteRuntime.Result result = runtime.execute(fakePlanJson, "test-index", 0, mockOsIndex);

            assertFalse(result.hasError());
            assertEquals(2, result.getRows().size());
            assertEquals(
                    List.of("department", "total_salary", "cnt"), result.getColumnNames());
            assertEquals(
                    List.of(SqlTypeName.VARCHAR, SqlTypeName.DOUBLE, SqlTypeName.BIGINT),
                    result.getColumnTypes());

            assertEquals("Engineering", result.getRows().get(0)[0]);
            assertEquals(250000.0, result.getRows().get(0)[1]);
            assertEquals(5L, result.getRows().get(0)[2]);
        }
    }

    @Test
    @DisplayName("Malformed plan JSON returns error in result, not an exception")
    void execute_malformedPlanJson_returnsError() {
        ShardCalciteRuntime.Result result =
                runtime.execute("not valid json", "test-index", 0, mockOsIndex);

        assertTrue(result.hasError(), "Result should have an error");
        assertNotNull(result.getError());
        assertTrue(result.getRows().isEmpty());
        assertTrue(result.getColumnNames().isEmpty());
        assertTrue(result.getColumnTypes().isEmpty());
    }

    @Test
    @DisplayName("Exception during execution returns error in result, not an exception")
    void execute_exceptionDuringExecution_returnsError() {
        // Create a Scannable that throws during scan()
        RelDataType rowType =
                OpenSearchTypeFactory.TYPE_FACTORY
                        .builder()
                        .add("col1", SqlTypeName.VARCHAR)
                        .build();

        ScannableRelNode throwingPlan =
                new ScannableRelNode(cluster, rowType, null) {
                    @Override
                    public Enumerable<@Nullable Object> scan() {
                        throw new RuntimeException("Simulated shard execution failure");
                    }
                };

        String fakePlanJson = "{\"rels\":[{\"will-fail\":\"plan\"}]}";

        try (MockedStatic<RelNodeSerializer> serializer = mockStatic(RelNodeSerializer.class)) {
            serializer
                    .when(() -> RelNodeSerializer.deserialize(eq(fakePlanJson), any(), any()))
                    .thenReturn(throwingPlan);

            ShardCalciteRuntime.Result result = runtime.execute(fakePlanJson, "test-index", 0, mockOsIndex);

            assertTrue(result.hasError());
            assertNotNull(result.getError());
            assertTrue(result.getError().getMessage().contains("Simulated shard execution failure"));
            assertTrue(result.getRows().isEmpty());
        }
    }

    @Test
    @DisplayName("Non-Scannable plan without scan leaf is handled by Interpreter")
    void execute_nonScannablePlan_noLeaf_handledByInterpreter() {
        // LogicalValues is not Scannable, so it falls through to the Interpreter path.
        // The Interpreter can handle LogicalValues (empty relation) and returns zero rows.
        RelDataType rowType =
                OpenSearchTypeFactory.TYPE_FACTORY
                        .builder()
                        .add("x", SqlTypeName.INTEGER)
                        .build();
        LogicalValues logicalValues = LogicalValues.createEmpty(cluster, rowType);

        String fakePlanJson = "{\"rels\":[{\"not-scannable\":\"plan\"}]}";

        try (MockedStatic<RelNodeSerializer> serializer = mockStatic(RelNodeSerializer.class)) {
            serializer
                    .when(() -> RelNodeSerializer.deserialize(eq(fakePlanJson), any(), any()))
                    .thenReturn(logicalValues);

            ShardCalciteRuntime.Result result = runtime.execute(fakePlanJson, "test-index", 0, mockOsIndex);

            // Interpreter handles LogicalValues (empty relation) and returns zero rows.
            assertFalse(result.hasError(), "Interpreter should handle LogicalValues without error");
            assertEquals(0, result.getRows().size());
            assertEquals(List.of("x"), result.getColumnNames());
            assertEquals(List.of(SqlTypeName.INTEGER), result.getColumnTypes());
        }
    }

    @Test
    @DisplayName("Empty result set returns zero rows without error")
    void execute_emptyResultSet_returnsZeroRows() {
        RelDataType rowType =
                OpenSearchTypeFactory.TYPE_FACTORY
                        .builder()
                        .add("name", SqlTypeName.VARCHAR)
                        .add("value", SqlTypeName.INTEGER)
                        .build();

        ScannableRelNode mockPlan = new ScannableRelNode(cluster, rowType, new Object[0][]);
        String fakePlanJson = "{\"rels\":[{\"empty\":\"plan\"}]}";

        try (MockedStatic<RelNodeSerializer> serializer = mockStatic(RelNodeSerializer.class)) {
            serializer
                    .when(() -> RelNodeSerializer.deserialize(eq(fakePlanJson), any(), any()))
                    .thenReturn(mockPlan);

            ShardCalciteRuntime.Result result = runtime.execute(fakePlanJson, "test-index", 0, mockOsIndex);

            assertFalse(result.hasError());
            assertEquals(0, result.getRows().size());
            assertEquals(List.of("name", "value"), result.getColumnNames());
            assertEquals(
                    List.of(SqlTypeName.VARCHAR, SqlTypeName.INTEGER), result.getColumnTypes());
        }
    }

    @Test
    @DisplayName("Interpreter single-column result returned as Object[] is not double-wrapped")
    void execute_interpreterSingleColumn_notDoubleWrapped() {
        // The Interpreter always returns Object[] rows, even for single-column results.
        // Verify that collectRows does not double-wrap them.
        RelDataType rowType =
                OpenSearchTypeFactory.TYPE_FACTORY
                        .builder()
                        .add("cnt", SqlTypeName.BIGINT)
                        .build();

        // Simulate Interpreter-style output: Object[]{42L} for single-column
        InterpreterStyleRelNode mockPlan = new InterpreterStyleRelNode(
                cluster, rowType, new Object[][] {{42L}, {99L}});
        String fakePlanJson = "{\"rels\":[{\"interpreter-single-col\":\"plan\"}]}";

        try (MockedStatic<RelNodeSerializer> serializer = mockStatic(RelNodeSerializer.class)) {
            serializer
                    .when(() -> RelNodeSerializer.deserialize(eq(fakePlanJson), any(), any()))
                    .thenReturn(mockPlan);

            ShardCalciteRuntime.Result result = runtime.execute(fakePlanJson, "test-index", 0, mockOsIndex);

            assertFalse(result.hasError(), "Result should not have an error");
            assertEquals(2, result.getRows().size());

            // Critical: row[0] must be the scalar 42L, NOT an Object[]
            Object firstCell = result.getRows().get(0)[0];
            assertFalse(firstCell instanceof Object[],
                    "Single-column cell must be scalar, not Object[]. Got: " + firstCell.getClass());
            assertEquals(42L, firstCell);
            assertEquals(99L, result.getRows().get(1)[0]);
        }
    }

    @Test
    @DisplayName("createCluster produces a valid cluster with OpenSearchTypeFactory")
    void createCluster_returnsValidCluster() {
        RelOptCluster cluster = ShardCalciteRuntime.createCluster();

        assertNotNull(cluster);
        assertNotNull(cluster.getRexBuilder());
        assertNotNull(cluster.getPlanner());
        assertEquals(
                OpenSearchTypeFactory.TYPE_FACTORY, cluster.getRexBuilder().getTypeFactory());
    }

    /**
     * A test-only RelNode that simulates Interpreter-style output: always returns Object[] rows
     * even for single-column results. Used to verify that collectRows does not double-wrap them.
     * Implements Scannable but returns Enumerable<Object[]> cast to Enumerable<Object>.
     */
    private static class InterpreterStyleRelNode extends LogicalValues implements Scannable {
        private final Object[][] data;
        private final RelDataType rowType;

        InterpreterStyleRelNode(RelOptCluster cluster, RelDataType rowType, Object[][] data) {
            super(
                    cluster,
                    cluster.traitSetOf(org.apache.calcite.plan.Convention.NONE),
                    rowType,
                    com.google.common.collect.ImmutableList.of());
            this.rowType = rowType;
            this.data = data;
        }

        @Override
        public RelDataType deriveRowType() {
            return rowType;
        }

        @Override
        @SuppressWarnings("unchecked")
        public Enumerable<@Nullable Object> scan() {
            // Return Object[] for EVERY row (including single-column), matching Interpreter behavior
            return new AbstractEnumerable<>() {
                @Override
                public Enumerator<Object> enumerator() {
                    return new Enumerator<>() {
                        private int index = -1;

                        @Override
                        public Object current() {
                            // Always return Object[], even for single-column (Interpreter style)
                            return data[index];
                        }

                        @Override
                        public boolean moveNext() {
                            return data != null && ++index < data.length;
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
    }

    /**
     * A test-only RelNode that implements Scannable to allow unit testing of ShardCalciteRuntime
     * without requiring a real OpenSearch cluster or DSLScan with live data.
     */
    private static class ScannableRelNode extends LogicalValues implements Scannable {
        private final Object[][] data;
        private final RelDataType rowType;

        ScannableRelNode(RelOptCluster cluster, RelDataType rowType, Object[][] data) {
            super(
                    cluster,
                    cluster.traitSetOf(org.apache.calcite.plan.Convention.NONE),
                    rowType,
                    com.google.common.collect.ImmutableList.of());
            this.rowType = rowType;
            this.data = data;
        }

        @Override
        public RelDataType deriveRowType() {
            return rowType;
        }

        @Override
        public Enumerable<@Nullable Object> scan() {
            final int columnCount = rowType.getFieldCount();
            return new AbstractEnumerable<>() {
                @Override
                public Enumerator<Object> enumerator() {
                    return new Enumerator<>() {
                        private int index = -1;

                        @Override
                        public Object current() {
                            if (columnCount == 1) {
                                return data[index][0];
                            }
                            return data[index];
                        }

                        @Override
                        public boolean moveNext() {
                            return data != null && ++index < data.length;
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
    }
}
