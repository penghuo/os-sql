/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.dqe;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import java.util.Collections;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.opensearch.dqe.exchange.ConcatExchange;
import org.opensearch.sql.opensearch.dqe.exchange.MergeAggregateExchange;
import org.opensearch.sql.opensearch.dqe.exchange.MergeSortExchange;
import org.opensearch.sql.opensearch.storage.scan.AbstractCalciteIndexScan;

class PlanSplitterTest {

    private RelDataTypeFactory typeFactory;
    private RelBuilder relBuilder;
    private RelOptCluster cluster;
    private RelDataType tableRowType;

    @BeforeEach
    void setUp() {
        typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);

        tableRowType =
                typeFactory.createStructType(
                        List.of(
                                typeFactory.createSqlType(SqlTypeName.INTEGER),
                                typeFactory.createSqlType(SqlTypeName.VARCHAR, 100),
                                typeFactory.createSqlType(SqlTypeName.DOUBLE)),
                        List.of("a", "b", "c"));

        SchemaPlus root = Frameworks.createRootSchema(true);
        root.add(
                "t",
                new AbstractTable() {
                    @Override
                    public RelDataType getRowType(RelDataTypeFactory tf) {
                        return tableRowType;
                    }
                });
        relBuilder = RelBuilder.create(Frameworks.newConfigBuilder().defaultSchema(root).build());
        cluster = relBuilder.scan("t").build().getCluster();
    }

    /**
     * Builds a plan using RelBuilder (which creates valid Calcite nodes) and then swaps the leaf
     * TableScan with a mock AbstractCalciteIndexScan. This ensures all intermediate nodes have
     * proper metadata while the leaf is recognized as an OpenSearch scan by PlanSplitter.
     */
    private RelNode injectDSLScan(RelNode plan) {
        return plan.accept(
                new RelShuttleImpl() {
                    @Override
                    public RelNode visit(TableScan scan) {
                        AbstractCalciteIndexScan dslScan =
                                mock(AbstractCalciteIndexScan.class, RETURNS_DEEP_STUBS);
                        when(dslScan.getRowType()).thenReturn(scan.getRowType());
                        when(dslScan.getInputs()).thenReturn(Collections.emptyList());
                        when(dslScan.getTraitSet()).thenReturn(scan.getTraitSet());
                        when(dslScan.getCluster()).thenReturn(scan.getCluster());
                        return dslScan;
                    }
                });
    }

    // ============================= Tests =============================

    @Test
    @DisplayName("Simple filter returns ConcatExchange above DSLScan")
    void simpleFilter_concatExchange() {
        RelNode plan =
                injectDSLScan(
                        relBuilder
                                .scan("t")
                                .filter(
                                        relBuilder.call(
                                                SqlStdOperatorTable.EQUALS,
                                                relBuilder.field("a"),
                                                relBuilder.literal(1)))
                                .build());

        DistributedPlan result = PlanSplitter.split(plan);

        assertNotNull(result);
        assertEquals(1, result.getExchanges().size());
        assertInstanceOf(ConcatExchange.class, result.getExchanges().get(0));
        assertNotNull(result.getExchanges().get(0).getShardPlan());
    }

    @Test
    @DisplayName("Filter + agg (COUNT by b) returns MergeAggregateExchange with partial agg")
    void filterPlusAgg_mergeAggregateExchange() {
        RelNode plan =
                injectDSLScan(
                        relBuilder
                                .scan("t")
                                .filter(
                                        relBuilder.call(
                                                SqlStdOperatorTable.EQUALS,
                                                relBuilder.field("a"),
                                                relBuilder.literal(1)))
                                .aggregate(
                                        relBuilder.groupKey(relBuilder.field("b")),
                                        relBuilder.countStar("cnt"))
                                .build());

        DistributedPlan result = PlanSplitter.split(plan);

        assertNotNull(result);
        assertEquals(1, result.getExchanges().size());
        assertInstanceOf(MergeAggregateExchange.class, result.getExchanges().get(0));

        MergeAggregateExchange mergeAgg = (MergeAggregateExchange) result.getExchanges().get(0);
        assertEquals(1, mergeAgg.getGroupCount());
        assertInstanceOf(Aggregate.class, mergeAgg.getShardPlan());
    }

    @Test
    @DisplayName("Filter + eval + agg returns MergeAggregateExchange with project above DSLScan")
    void filterEvalAgg_mergeAggregateExchange() {
        RelNode plan =
                injectDSLScan(
                        relBuilder
                                .scan("t")
                                .filter(
                                        relBuilder.call(
                                                SqlStdOperatorTable.EQUALS,
                                                relBuilder.field("a"),
                                                relBuilder.literal(1)))
                                .project(relBuilder.field("a"), relBuilder.field("b"))
                                .aggregate(
                                        relBuilder.groupKey(relBuilder.field("b")),
                                        relBuilder.countStar("cnt"))
                                .build());

        DistributedPlan result = PlanSplitter.split(plan);

        assertNotNull(result);
        assertEquals(1, result.getExchanges().size());
        assertInstanceOf(MergeAggregateExchange.class, result.getExchanges().get(0));
        assertNotNull(result.getExchanges().get(0).getShardPlan());
    }

    @Test
    @DisplayName("Sort + limit returns MergeSortExchange")
    void sortWithLimit_mergeSortExchange() {
        RelNode plan =
                injectDSLScan(
                        relBuilder
                                .scan("t")
                                .sort(relBuilder.field("a"))
                                .limit(0, 10)
                                .build());

        DistributedPlan result = PlanSplitter.split(plan);

        assertNotNull(result);
        assertEquals(1, result.getExchanges().size());
        assertInstanceOf(MergeSortExchange.class, result.getExchanges().get(0));

        MergeSortExchange mse = (MergeSortExchange) result.getExchanges().get(0);
        assertEquals(10, mse.getLimit());
        assertTrue(mse.getCollations().size() > 0);
    }

    @Test
    @DisplayName("Sort without limit returns ConcatExchange (sort on coordinator)")
    void sortNoLimit_concatExchange() {
        RelNode plan =
                injectDSLScan(
                        relBuilder.scan("t").sort(relBuilder.field("a")).build());

        DistributedPlan result = PlanSplitter.split(plan);

        assertNotNull(result);
        assertEquals(1, result.getExchanges().size());
        assertInstanceOf(ConcatExchange.class, result.getExchanges().get(0));
    }

    @Test
    @DisplayName("Window function returns ConcatExchange (window on coordinator)")
    void windowFunction_concatExchange() {
        // Build a mock Window node above a real scan
        RelNode scan = injectDSLScan(relBuilder.scan("t").build());

        org.apache.calcite.rel.core.Window window = mock(org.apache.calcite.rel.core.Window.class);
        when(window.getInputs()).thenReturn(List.of(scan));
        when(window.getRowType()).thenReturn(tableRowType);
        when(window.getTraitSet()).thenReturn(cluster.traitSet());
        when(window.getCluster()).thenReturn(cluster);
        when(window.copy(any(RelTraitSet.class), anyList()))
                .thenAnswer(
                        inv -> {
                            org.apache.calcite.rel.core.Window w2 =
                                    mock(org.apache.calcite.rel.core.Window.class);
                            List<RelNode> newInputs = inv.getArgument(1);
                            when(w2.getInputs()).thenReturn(newInputs);
                            when(w2.getRowType()).thenReturn(tableRowType);
                            when(w2.getTraitSet()).thenReturn(inv.getArgument(0));
                            when(w2.getCluster()).thenReturn(cluster);
                            return w2;
                        });

        DistributedPlan result = PlanSplitter.split(window);

        assertNotNull(result);
        assertEquals(1, result.getExchanges().size());
        assertInstanceOf(ConcatExchange.class, result.getExchanges().get(0));
    }

    @Test
    @DisplayName("Join returns ConcatExchange on both inputs")
    void join_concatExchangeOnBothInputs() {
        // Build two separate scans
        RelNode leftPlan = injectDSLScan(relBuilder.scan("t").build());
        RelNode rightPlan = injectDSLScan(relBuilder.scan("t").build());

        // Build join manually using RelBuilder
        relBuilder.push(leftPlan);
        relBuilder.push(rightPlan);
        RelNode join =
                relBuilder
                        .join(
                                org.apache.calcite.rel.core.JoinRelType.INNER,
                                relBuilder.call(
                                        SqlStdOperatorTable.EQUALS,
                                        relBuilder.field(2, 0, "a"),
                                        relBuilder.field(2, 1, "a")))
                        .build();

        DistributedPlan result = PlanSplitter.split(join);

        assertNotNull(result);
        assertEquals(2, result.getExchanges().size());
        assertInstanceOf(ConcatExchange.class, result.getExchanges().get(0));
        assertInstanceOf(ConcatExchange.class, result.getExchanges().get(1));
    }

    @Test
    @DisplayName("Unrecognized operator returns ConcatExchange (safe default)")
    void unrecognizedOp_concatExchange() {
        RelNode scan = injectDSLScan(relBuilder.scan("t").build());

        // Create a custom operator that PlanSplitter won't recognize
        RelNode unknown =
                new AbstractRelNode(cluster, cluster.traitSet()) {
                    @Override
                    public List<RelNode> getInputs() {
                        return List.of(scan);
                    }

                    @Override
                    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
                        RelNode newInput = inputs.get(0);
                        AbstractRelNode copy =
                                new AbstractRelNode(cluster, traitSet) {
                                    @Override
                                    public List<RelNode> getInputs() {
                                        return List.of(newInput);
                                    }

                                    @Override
                                    public RelNode copy(
                                            RelTraitSet ts, List<RelNode> ins) {
                                        return this;
                                    }

                                    @Override
                                    protected RelDataType deriveRowType() {
                                        return tableRowType;
                                    }
                                };
                        return copy;
                    }

                    @Override
                    protected RelDataType deriveRowType() {
                        return tableRowType;
                    }
                };

        DistributedPlan result = PlanSplitter.split(unknown);

        assertNotNull(result);
        assertEquals(1, result.getExchanges().size());
        assertInstanceOf(ConcatExchange.class, result.getExchanges().get(0));
    }

    @Test
    @DisplayName("Non-decomposable agg returns ConcatExchange (raw rows to coordinator)")
    void nonDecomposableAgg_concatExchange() {
        // Build scan then aggregate with VAR_POP (non-decomposable)
        relBuilder.scan("t");
        AggregateCall varPopCall =
                AggregateCall.create(
                        SqlStdOperatorTable.VAR_POP,
                        false,
                        false,
                        false,
                        ImmutableList.of(),
                        ImmutableList.of(0),
                        -1,
                        null,
                        org.apache.calcite.rel.RelCollations.EMPTY,
                        typeFactory.createTypeWithNullability(
                                typeFactory.createSqlType(SqlTypeName.DOUBLE), true),
                        "var_pop_a");
        RelNode plan =
                injectDSLScan(
                        relBuilder
                                .aggregate(
                                        relBuilder.groupKey(),
                                        List.of(varPopCall))
                                .build());

        DistributedPlan result = PlanSplitter.split(plan);

        assertNotNull(result);
        assertEquals(1, result.getExchanges().size());
        assertInstanceOf(ConcatExchange.class, result.getExchanges().get(0));
    }

    @Test
    @DisplayName("Calc (eval) between scan and agg stays on shard side")
    void calcBetweenScanAndAgg_staysOnShard() {
        RelNode plan =
                injectDSLScan(
                        relBuilder
                                .scan("t")
                                .filter(
                                        relBuilder.call(
                                                SqlStdOperatorTable.GREATER_THAN,
                                                relBuilder.field("a"),
                                                relBuilder.literal(0)))
                                .project(relBuilder.field("a"), relBuilder.field("b"))
                                .aggregate(
                                        relBuilder.groupKey(relBuilder.field("b")),
                                        relBuilder.countStar("cnt"))
                                .build());

        DistributedPlan result = PlanSplitter.split(plan);

        assertNotNull(result);
        assertEquals(1, result.getExchanges().size());
        assertInstanceOf(MergeAggregateExchange.class, result.getExchanges().get(0));

        // The shard plan includes the partial aggregate above filter/project/scan
        RelNode shardPlan = result.getExchanges().get(0).getShardPlan();
        assertInstanceOf(Aggregate.class, shardPlan);
    }

    @Test
    @DisplayName("Returns null for non-OpenSearch scan (system index)")
    void nonOpenSearchScan_returnsNull() {
        // Build a plan with a real table scan (not an AbstractCalciteIndexScan)
        // Don't inject DSLScan -- keep the vanilla TableScan
        RelNode plan =
                relBuilder
                        .scan("t")
                        .filter(
                                relBuilder.call(
                                        SqlStdOperatorTable.EQUALS,
                                        relBuilder.field("a"),
                                        relBuilder.literal(1)))
                        .build();

        DistributedPlan result = PlanSplitter.split(plan);

        assertNull(result);
    }

    @Test
    @DisplayName("isShardLocal correctly classifies scan as shard-local")
    void isShardLocal_classifiesScan() {
        AbstractCalciteIndexScan scan = mock(AbstractCalciteIndexScan.class, RETURNS_DEEP_STUBS);
        assertTrue(PlanSplitter.isShardLocal(scan));
    }

    @Test
    @DisplayName("isShardLocal correctly classifies filter as shard-local")
    void isShardLocal_classifiesFilter() {
        RelNode plan =
                relBuilder
                        .scan("t")
                        .filter(
                                relBuilder.call(
                                        SqlStdOperatorTable.EQUALS,
                                        relBuilder.field("a"),
                                        relBuilder.literal(1)))
                        .build();
        // plan is a LogicalFilter
        assertTrue(PlanSplitter.isShardLocal(plan));
    }

    @Test
    @DisplayName("isShardLocal correctly classifies project as shard-local")
    void isShardLocal_classifiesProject() {
        RelNode plan =
                relBuilder
                        .scan("t")
                        .project(relBuilder.field("a"), relBuilder.field("b"))
                        .build();
        // plan is a LogicalProject
        assertTrue(PlanSplitter.isShardLocal(plan));
    }

    @Test
    @DisplayName("isShardLocal correctly classifies LogicalSystemLimit as shard-local")
    void isShardLocal_classifiesLogicalSystemLimit() {
        RelNode scan = relBuilder.scan("t").build();
        org.opensearch.sql.calcite.plan.rel.LogicalSystemLimit sysLimit =
                org.opensearch.sql.calcite.plan.rel.LogicalSystemLimit.create(
                        org.opensearch.sql.calcite.plan.rel.LogicalSystemLimit.SystemLimitType
                                .QUERY_SIZE_LIMIT,
                        scan,
                        relBuilder.literal(10000));
        assertTrue(PlanSplitter.isShardLocal(sysLimit));
    }

    @Test
    @DisplayName("LogicalSystemLimit wraps entire shard-local plan in ConcatExchange (not MergeSortExchange)")
    void logicalSystemLimit_concatExchange() {
        // Build a plan with LogicalSystemLimit on top of a DSLScan
        RelNode scanPlan = injectDSLScan(relBuilder.scan("t").build());
        org.opensearch.sql.calcite.plan.rel.LogicalSystemLimit sysLimit =
                org.opensearch.sql.calcite.plan.rel.LogicalSystemLimit.create(
                        org.opensearch.sql.calcite.plan.rel.LogicalSystemLimit.SystemLimitType
                                .QUERY_SIZE_LIMIT,
                        scanPlan,
                        relBuilder.literal(10000));

        DistributedPlan result = PlanSplitter.split(sysLimit);

        assertNotNull(result);
        assertEquals(1, result.getExchanges().size());
        // LogicalSystemLimit is shard-local, so the entire plan (including the limit)
        // is wrapped in a ConcatExchange, NOT a MergeSortExchange
        assertInstanceOf(ConcatExchange.class, result.getExchanges().get(0));
    }

    @Test
    @DisplayName("Decomposable agg coordinator plan is ExchangeLeaf (no redundant Aggregate)")
    void decomposableAgg_coordinatorPlanIsExchangeLeaf() {
        RelNode plan =
                injectDSLScan(
                        relBuilder
                                .scan("t")
                                .aggregate(
                                        relBuilder.groupKey(relBuilder.field("b")),
                                        relBuilder.countStar("cnt"))
                                .build());

        DistributedPlan result = PlanSplitter.split(plan);

        assertNotNull(result);
        assertInstanceOf(PlanSplitter.ExchangeLeaf.class, result.getCoordinatorPlan());
    }

    @Test
    @DisplayName("Sort above decomposable agg produces Sort → ExchangeLeaf coordinator plan")
    void sortAboveDecomposableAgg_sortAboveExchangeLeaf() {
        RelNode plan =
                injectDSLScan(
                        relBuilder
                                .scan("t")
                                .aggregate(
                                        relBuilder.groupKey(relBuilder.field("b")),
                                        relBuilder.countStar("cnt"))
                                .sort(relBuilder.field("cnt"))
                                .build());

        DistributedPlan result = PlanSplitter.split(plan);

        assertNotNull(result);
        // Coordinator plan should be Sort → ExchangeLeaf (sort without limit uses ConcatExchange)
        // The aggregate is absorbed into MergeAggregateExchange; Sort stays on coordinator
        RelNode coordinator = result.getCoordinatorPlan();
        assertInstanceOf(org.apache.calcite.rel.core.Sort.class, coordinator);
        assertInstanceOf(PlanSplitter.ExchangeLeaf.class, coordinator.getInputs().get(0));
    }

    @Test
    @DisplayName("CalciteEnumerableNestedAggregate causes DQE skip (returns null)")
    void nestedAggregate_skipsDQE() {
        RelNode scan = injectDSLScan(relBuilder.scan("t").build());

        // Create a mock CalciteEnumerableNestedAggregate above the scan
        org.opensearch.sql.opensearch.planner.physical.CalciteEnumerableNestedAggregate nestedAgg =
                mock(
                        org.opensearch.sql.opensearch.planner.physical
                                .CalciteEnumerableNestedAggregate.class,
                        RETURNS_DEEP_STUBS);
        when(nestedAgg.getInputs()).thenReturn(List.of(scan));
        when(nestedAgg.getRowType()).thenReturn(tableRowType);
        when(nestedAgg.getTraitSet()).thenReturn(cluster.traitSet());
        when(nestedAgg.getCluster()).thenReturn(cluster);

        DistributedPlan result = PlanSplitter.split(nestedAgg);

        // Nested aggregation requires OpenSearch pushdown, so DQE should be skipped entirely
        assertNull(result);
    }
}
