/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.OptionalLong;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.ast.tree.Sort.SortOption;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.NamedExpression;
import org.opensearch.sql.expression.aggregation.NamedAggregator;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.planner.physical.distributed.DistributedExchangeOperator;
import org.opensearch.sql.planner.physical.distributed.DistributedHashJoinOperator;
import org.opensearch.sql.planner.physical.distributed.DistributedSortOperator;
import org.opensearch.sql.planner.physical.distributed.FinalAggregationOperator;
import org.opensearch.sql.planner.physical.distributed.JoinType;

class DistributedPlanConverterTest {

    private DistributedPlanConverter converter;

    @BeforeEach
    void setUp() {
        converter = new DistributedPlanConverter();
    }

    @Test
    @DisplayName("mapSqlTypeToExprType maps common SQL types correctly")
    void testMapSqlTypeToExprType() {
        RelDataType intType = mockRelDataType(SqlTypeName.INTEGER);
        assertEquals(ExprCoreType.INTEGER, DistributedPlanConverter.mapSqlTypeToExprType(intType));

        RelDataType bigintType = mockRelDataType(SqlTypeName.BIGINT);
        assertEquals(ExprCoreType.LONG, DistributedPlanConverter.mapSqlTypeToExprType(bigintType));

        RelDataType doubleType = mockRelDataType(SqlTypeName.DOUBLE);
        assertEquals(ExprCoreType.DOUBLE, DistributedPlanConverter.mapSqlTypeToExprType(doubleType));

        RelDataType varcharType = mockRelDataType(SqlTypeName.VARCHAR);
        assertEquals(ExprCoreType.STRING, DistributedPlanConverter.mapSqlTypeToExprType(varcharType));

        RelDataType boolType = mockRelDataType(SqlTypeName.BOOLEAN);
        assertEquals(ExprCoreType.BOOLEAN, DistributedPlanConverter.mapSqlTypeToExprType(boolType));

        RelDataType dateType = mockRelDataType(SqlTypeName.DATE);
        assertEquals(ExprCoreType.DATE, DistributedPlanConverter.mapSqlTypeToExprType(dateType));

        RelDataType timestampType = mockRelDataType(SqlTypeName.TIMESTAMP);
        assertEquals(ExprCoreType.TIMESTAMP,
                DistributedPlanConverter.mapSqlTypeToExprType(timestampType));
    }

    @Test
    @DisplayName("mapSqlTypeToExprType returns UNKNOWN for unsupported types")
    void testMapSqlTypeToExprTypeUnknown() {
        RelDataType arrayType = mockRelDataType(SqlTypeName.ARRAY);
        assertEquals(ExprCoreType.UNKNOWN, DistributedPlanConverter.mapSqlTypeToExprType(arrayType));
    }

    @Test
    @DisplayName("extractNamedAggregators handles COUNT aggregate")
    void testExtractCountAggregator() {
        LogicalAggregate aggregate = mockAggregate(
                Arrays.asList(mockField("id", SqlTypeName.INTEGER)),
                ImmutableBitSet.of(),
                Arrays.asList(mockAggCall(SqlKind.COUNT, Collections.emptyList(), "count_all")),
                Arrays.asList(mockField("count_all", SqlTypeName.BIGINT))
        );

        List<NamedAggregator> aggregators = converter.extractNamedAggregators(aggregate);
        assertEquals(1, aggregators.size());
        assertEquals("count_all", aggregators.get(0).getName());
        assertEquals("count", aggregators.get(0).getFunctionName().getFunctionName());
    }

    @Test
    @DisplayName("extractNamedAggregators handles SUM aggregate with group-by")
    void testExtractSumAggregatorWithGroupBy() {
        LogicalAggregate aggregate = mockAggregate(
                Arrays.asList(
                        mockField("category", SqlTypeName.VARCHAR),
                        mockField("amount", SqlTypeName.DOUBLE)),
                ImmutableBitSet.of(0),
                Arrays.asList(mockAggCall(SqlKind.SUM, Arrays.asList(1), "total")),
                Arrays.asList(
                        mockField("category", SqlTypeName.VARCHAR),
                        mockField("total", SqlTypeName.DOUBLE))
        );

        List<NamedAggregator> aggregators = converter.extractNamedAggregators(aggregate);
        assertEquals(1, aggregators.size());
        assertEquals("total", aggregators.get(0).getName());
        assertEquals("sum", aggregators.get(0).getFunctionName().getFunctionName());
    }

    @Test
    @DisplayName("extractGroupByExpressions extracts group-by fields")
    void testExtractGroupByExpressions() {
        LogicalAggregate aggregate = mockAggregate(
                Arrays.asList(
                        mockField("category", SqlTypeName.VARCHAR),
                        mockField("amount", SqlTypeName.DOUBLE)),
                ImmutableBitSet.of(0),
                Arrays.asList(mockAggCall(SqlKind.SUM, Arrays.asList(1), "total")),
                Arrays.asList(
                        mockField("category", SqlTypeName.VARCHAR),
                        mockField("total", SqlTypeName.DOUBLE))
        );

        List<NamedExpression> groupBys = converter.extractGroupByExpressions(aggregate);
        assertEquals(1, groupBys.size());
        assertEquals("category", groupBys.get(0).getNameOrAlias());
    }

    @Test
    @DisplayName("extractSortSpec extracts ASC and DESC sort options")
    void testExtractSortSpec() {
        LogicalSort sort = mockSort(
                Arrays.asList(mockField("amount", SqlTypeName.DOUBLE)),
                Arrays.asList(
                        new RelFieldCollation(0, RelFieldCollation.Direction.ASCENDING)),
                null
        );

        List<Pair<SortOption, Expression>> sortSpec = converter.extractSortSpec(sort);
        assertEquals(1, sortSpec.size());
        assertEquals(SortOption.DEFAULT_ASC, sortSpec.get(0).getLeft());
    }

    @Test
    @DisplayName("extractSortSpec handles DESC direction")
    void testExtractSortSpecDesc() {
        LogicalSort sort = mockSort(
                Arrays.asList(mockField("amount", SqlTypeName.DOUBLE)),
                Arrays.asList(
                        new RelFieldCollation(0, RelFieldCollation.Direction.DESCENDING)),
                null
        );

        List<Pair<SortOption, Expression>> sortSpec = converter.extractSortSpec(sort);
        assertEquals(1, sortSpec.size());
        assertEquals(SortOption.DEFAULT_DESC, sortSpec.get(0).getLeft());
    }

    @Test
    @DisplayName("extractLimit returns empty when no fetch")
    void testExtractLimitEmpty() {
        LogicalSort sort = mockSort(
                Arrays.asList(mockField("x", SqlTypeName.INTEGER)),
                Arrays.asList(new RelFieldCollation(0)),
                null
        );

        OptionalLong limit = converter.extractLimit(sort);
        assertTrue(limit.isEmpty());
    }

    @Test
    @DisplayName("buildCoordinatorPlan creates FinalAggregationOperator for LogicalAggregate")
    void testBuildCoordinatorPlanAggregate() {
        LogicalAggregate aggregate = mockAggregate(
                Arrays.asList(mockField("amount", SqlTypeName.DOUBLE)),
                ImmutableBitSet.of(),
                Arrays.asList(mockAggCall(SqlKind.COUNT, Collections.emptyList(), "cnt")),
                Arrays.asList(mockField("cnt", SqlTypeName.BIGINT))
        );
        Fragment fragment = Fragment.single(0, Collections.emptyList(), ExchangeSpec.gather(), 100);

        PhysicalPlan plan = converter.buildCoordinatorPlan(aggregate, fragment, 3);
        assertNotNull(plan);
        assertTrue(plan instanceof FinalAggregationOperator);

        FinalAggregationOperator finalAgg = (FinalAggregationOperator) plan;
        assertEquals(1, finalAgg.getAggregatorList().size());
        assertEquals("cnt", finalAgg.getAggregatorList().get(0).getName());
        assertTrue(finalAgg.getGroupByExprList().isEmpty());
        assertTrue(finalAgg.getInput() instanceof DistributedExchangeOperator);
    }

    @Test
    @DisplayName("buildCoordinatorPlan creates DistributedSortOperator for LogicalSort")
    void testBuildCoordinatorPlanSort() {
        RelNode inputRel = mock(RelNode.class);
        when(inputRel.getInputs()).thenReturn(Collections.emptyList());

        LogicalSort sort = mockSort(
                Arrays.asList(mockField("amount", SqlTypeName.DOUBLE)),
                Arrays.asList(new RelFieldCollation(0, RelFieldCollation.Direction.ASCENDING)),
                null
        );

        Fragment fragment = Fragment.single(0, Collections.emptyList(), ExchangeSpec.gather(), 100);

        PhysicalPlan plan = converter.buildCoordinatorPlan(sort, fragment, 3);
        assertNotNull(plan);
        assertTrue(plan instanceof DistributedSortOperator);
    }

    @Test
    @DisplayName("mapJoinType maps Calcite join types correctly")
    void testMapJoinType() {
        LogicalJoin innerJoin = mockJoin(JoinRelType.INNER);
        assertEquals(JoinType.INNER, converter.mapJoinType(innerJoin));

        LogicalJoin leftJoin = mockJoin(JoinRelType.LEFT);
        assertEquals(JoinType.LEFT, converter.mapJoinType(leftJoin));

        LogicalJoin rightJoin = mockJoin(JoinRelType.RIGHT);
        assertEquals(JoinType.RIGHT, converter.mapJoinType(rightJoin));
    }

    @Test
    @DisplayName("Multiple aggregations extracted correctly")
    void testMultipleAggregations() {
        LogicalAggregate aggregate = mockAggregate(
                Arrays.asList(
                        mockField("category", SqlTypeName.VARCHAR),
                        mockField("amount", SqlTypeName.DOUBLE)),
                ImmutableBitSet.of(0),
                Arrays.asList(
                        mockAggCall(SqlKind.COUNT, Collections.emptyList(), "cnt"),
                        mockAggCall(SqlKind.SUM, Arrays.asList(1), "total"),
                        mockAggCall(SqlKind.AVG, Arrays.asList(1), "avg_amount")),
                Arrays.asList(
                        mockField("category", SqlTypeName.VARCHAR),
                        mockField("cnt", SqlTypeName.BIGINT),
                        mockField("total", SqlTypeName.DOUBLE),
                        mockField("avg_amount", SqlTypeName.DOUBLE))
        );

        List<NamedAggregator> aggregators = converter.extractNamedAggregators(aggregate);
        assertEquals(3, aggregators.size());
        assertEquals("cnt", aggregators.get(0).getName());
        assertEquals("total", aggregators.get(1).getName());
        assertEquals("avg_amount", aggregators.get(2).getName());
    }

    @Test
    @DisplayName("Unsupported aggregation throws UnsupportedOperationException")
    void testUnsupportedAggregation() {
        LogicalAggregate aggregate = mockAggregate(
                Arrays.asList(mockField("x", SqlTypeName.INTEGER)),
                ImmutableBitSet.of(),
                // COLLECT is not a standard supported agg
                Arrays.asList(mockAggCallWithKind(SqlKind.LISTAGG, Collections.emptyList(),
                        "collected")),
                Arrays.asList(mockField("collected", SqlTypeName.VARCHAR))
        );

        assertThrows(UnsupportedOperationException.class,
                () -> converter.extractNamedAggregators(aggregate));
    }

    // --- Helper methods ---

    private RelDataTypeField mockField(String name, SqlTypeName typeName) {
        RelDataType type = mockRelDataType(typeName);
        return new RelDataTypeFieldImpl(name, 0, type);
    }

    private RelDataType mockRelDataType(SqlTypeName typeName) {
        RelDataType type = mock(RelDataType.class);
        when(type.getSqlTypeName()).thenReturn(typeName);
        return type;
    }

    private AggregateCall mockAggCall(SqlKind kind, List<Integer> argList, String name) {
        return mockAggCallWithKind(kind, argList, name);
    }

    private AggregateCall mockAggCallWithKind(
            SqlKind kind, List<Integer> argList, String name) {
        AggregateCall call = mock(AggregateCall.class);
        org.apache.calcite.sql.SqlAggFunction aggFunc = mock(
                org.apache.calcite.sql.SqlAggFunction.class);
        when(aggFunc.getKind()).thenReturn(kind);
        when(call.getAggregation()).thenReturn(aggFunc);
        when(call.getArgList()).thenReturn(argList);
        when(call.getName()).thenReturn(name);
        return call;
    }

    private LogicalAggregate mockAggregate(
            List<RelDataTypeField> inputFields,
            ImmutableBitSet groupSet,
            List<AggregateCall> aggCalls,
            List<RelDataTypeField> outputFields) {
        LogicalAggregate agg = mock(LogicalAggregate.class);

        // Input row type
        RelNode input = mock(RelNode.class);
        RelDataType inputRowType = mock(RelDataType.class);
        when(inputRowType.getFieldList()).thenReturn(inputFields);
        when(input.getRowType()).thenReturn(inputRowType);
        when(input.getInputs()).thenReturn(Collections.emptyList());
        when(agg.getInput()).thenReturn(input);

        // Output row type
        RelDataType outputRowType = mock(RelDataType.class);
        when(outputRowType.getFieldList()).thenReturn(outputFields);
        when(agg.getRowType()).thenReturn(outputRowType);

        // Group set and agg calls
        when(agg.getGroupSet()).thenReturn(groupSet);
        when(agg.getGroupCount()).thenReturn(groupSet.cardinality());
        when(agg.getAggCallList()).thenReturn(aggCalls);
        when(agg.getInputs()).thenReturn(Collections.singletonList(input));

        return agg;
    }

    private LogicalSort mockSort(
            List<RelDataTypeField> inputFields,
            List<RelFieldCollation> collations,
            RexLiteral fetch) {
        LogicalSort sort = mock(LogicalSort.class);

        RelNode input = mock(RelNode.class);
        RelDataType inputRowType = mock(RelDataType.class);
        when(inputRowType.getFieldList()).thenReturn(inputFields);
        when(input.getRowType()).thenReturn(inputRowType);
        when(input.getInputs()).thenReturn(Collections.emptyList());
        when(sort.getInput()).thenReturn(input);
        when(sort.getInputs()).thenReturn(Collections.singletonList(input));

        RelCollation collation = RelCollations.of(collations);
        when(sort.getCollation()).thenReturn(collation);

        // Use reflection-free approach for fetch
        try {
            java.lang.reflect.Field fetchField = LogicalSort.class.getSuperclass()
                    .getDeclaredField("fetch");
            fetchField.setAccessible(true);
            fetchField.set(sort, fetch);
        } catch (Exception e) {
            // In mock, fetch field access via mock
        }
        // Also mock the public getter if available
        when(sort.getRowType()).thenReturn(inputRowType);

        return sort;
    }

    private LogicalJoin mockJoin(JoinRelType joinType) {
        LogicalJoin join = mock(LogicalJoin.class);
        when(join.getJoinType()).thenReturn(joinType);

        RexNode condition = mock(RexNode.class);
        when(join.getCondition()).thenReturn(condition);

        RelNode left = mock(RelNode.class);
        RelNode right = mock(RelNode.class);
        RelDataType leftType = mock(RelDataType.class);
        RelDataType rightType = mock(RelDataType.class);
        when(leftType.getFieldList()).thenReturn(Collections.emptyList());
        when(leftType.getFieldCount()).thenReturn(0);
        when(rightType.getFieldList()).thenReturn(Collections.emptyList());
        when(rightType.getFieldCount()).thenReturn(0);
        when(left.getRowType()).thenReturn(leftType);
        when(right.getRowType()).thenReturn(rightType);
        when(left.getInputs()).thenReturn(Collections.emptyList());
        when(right.getInputs()).thenReturn(Collections.emptyList());
        when(join.getLeft()).thenReturn(left);
        when(join.getRight()).thenReturn(right);
        when(join.getInputs()).thenReturn(Arrays.asList(left, right));

        RelDataType joinRowType = mock(RelDataType.class);
        when(joinRowType.getFieldList()).thenReturn(Collections.emptyList());
        when(join.getRowType()).thenReturn(joinRowType);

        return join;
    }
}
