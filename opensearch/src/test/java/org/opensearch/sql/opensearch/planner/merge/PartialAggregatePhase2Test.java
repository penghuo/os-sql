/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.planner.merge;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.TYPE_FACTORY;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.opensearch.storage.serde.RelNodeSerializer;

/**
 * Tests for Phase 2 additions to PartialAggregate: STDDEV/VAR decomposition and approximate UDAF
 * support.
 */
class PartialAggregatePhase2Test {

    private RexBuilder rexBuilder;
    private RelOptCluster cluster;
    private RelTraitSet enumerableTraits;

    @BeforeEach
    void setUp() {
        rexBuilder = new RexBuilder(TYPE_FACTORY);
        VolcanoPlanner planner = new VolcanoPlanner();
        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
        cluster = RelOptCluster.create(planner, rexBuilder);
        enumerableTraits = cluster.traitSetOf(EnumerableConvention.INSTANCE);
    }

    private RelNode createScan(String... namesAndTypes) {
        ImmutableList.Builder<RelDataType> types = ImmutableList.builder();
        ImmutableList.Builder<String> names = ImmutableList.builder();
        for (int i = 0; i < namesAndTypes.length; i += 2) {
            names.add(namesAndTypes[i]);
            types.add(TYPE_FACTORY.createSqlType(SqlTypeName.valueOf(namesAndTypes[i + 1])));
        }
        RelDataType rowType = TYPE_FACTORY.createStructType(types.build(), names.build());
        return new RelNodeSerializer.ShardScanPlaceholder(cluster, enumerableTraits, rowType);
    }

    @Nested
    @DisplayName("STDDEV/VAR decomposition")
    class StddevVarTests {

        @Test
        @DisplayName("isDecomposable returns true for STDDEV_POP")
        void testStddevPopIsDecomposable() {
            AggregateCall call =
                    AggregateCall.create(
                            SqlStdOperatorTable.STDDEV_POP,
                            false,
                            false,
                            false,
                            ImmutableList.of(),
                            ImmutableList.of(1),
                            -1,
                            null,
                            RelCollations.EMPTY,
                            TYPE_FACTORY.createSqlType(SqlTypeName.DOUBLE),
                            "stddev_pop_val");
            assertTrue(PartialAggregate.isDecomposable(call));
        }

        @Test
        @DisplayName("isDecomposable returns true for STDDEV_SAMP")
        void testStddevSampIsDecomposable() {
            AggregateCall call =
                    AggregateCall.create(
                            SqlStdOperatorTable.STDDEV_SAMP,
                            false,
                            false,
                            false,
                            ImmutableList.of(),
                            ImmutableList.of(1),
                            -1,
                            null,
                            RelCollations.EMPTY,
                            TYPE_FACTORY.createSqlType(SqlTypeName.DOUBLE),
                            "stddev_samp_val");
            assertTrue(PartialAggregate.isDecomposable(call));
        }

        @Test
        @DisplayName("isDecomposable returns true for VAR_POP")
        void testVarPopIsDecomposable() {
            AggregateCall call =
                    AggregateCall.create(
                            SqlStdOperatorTable.VAR_POP,
                            false,
                            false,
                            false,
                            ImmutableList.of(),
                            ImmutableList.of(1),
                            -1,
                            null,
                            RelCollations.EMPTY,
                            TYPE_FACTORY.createSqlType(SqlTypeName.DOUBLE),
                            "var_pop_val");
            assertTrue(PartialAggregate.isDecomposable(call));
        }

        @Test
        @DisplayName("isDecomposable returns true for VAR_SAMP")
        void testVarSampIsDecomposable() {
            AggregateCall call =
                    AggregateCall.create(
                            SqlStdOperatorTable.VAR_SAMP,
                            false,
                            false,
                            false,
                            ImmutableList.of(),
                            ImmutableList.of(1),
                            -1,
                            null,
                            RelCollations.EMPTY,
                            TYPE_FACTORY.createSqlType(SqlTypeName.DOUBLE),
                            "var_samp_val");
            assertTrue(PartialAggregate.isDecomposable(call));
        }

        @Test
        @DisplayName("STDDEV decomposes into 3 partial calls: COUNT, SUM, SUM_SQUARES")
        void testStddevDecomposesIntoThreeCalls() {
            RelNode scan = createScan("group_key", "INTEGER", "value", "DOUBLE");
            AggregateCall stddevCall =
                    AggregateCall.create(
                            SqlStdOperatorTable.STDDEV_POP,
                            false,
                            false,
                            false,
                            ImmutableList.of(),
                            ImmutableList.of(1),
                            -1,
                            null,
                            RelCollations.EMPTY,
                            TYPE_FACTORY.createSqlType(SqlTypeName.DOUBLE),
                            "stddev_val");

            // Use LogicalAggregate which supports all aggregate functions (unlike
            // EnumerableAggregate which rejects STDDEV_POP)
            LogicalAggregate agg =
                    LogicalAggregate.create(
                            scan,
                            ImmutableList.of(),
                            ImmutableBitSet.of(0),
                            null,
                            ImmutableList.of(stddevCall));

            PartialAggregate partial = PartialAggregate.create(agg);
            assertNotNull(partial);
            // STDDEV decomposes into 3 calls: count, sum, sumSquares
            assertEquals(3, partial.getAggCallList().size());

            AggregateCall countCall = partial.getAggCallList().get(0);
            AggregateCall sumCall = partial.getAggCallList().get(1);
            AggregateCall sumSquaresCall = partial.getAggCallList().get(2);

            assertEquals(SqlKind.COUNT, countCall.getAggregation().getKind());
            assertEquals(SqlKind.SUM, sumCall.getAggregation().getKind());
            assertEquals(SqlKind.SUM, sumSquaresCall.getAggregation().getKind());

            assertTrue(countCall.getName().contains("$count"));
            assertTrue(sumCall.getName().contains("$sum"));
            assertTrue(sumSquaresCall.getName().contains("$sumSquares"));
        }
    }

    @Nested
    @DisplayName("MergeStrategy computation")
    class MergeStrategyTests {

        @Test
        @DisplayName("STDDEV uses WELFORD merge strategy")
        void testStddevUsesWelfordStrategy() {
            AggregateCall call =
                    AggregateCall.create(
                            SqlStdOperatorTable.STDDEV_POP,
                            false,
                            false,
                            false,
                            ImmutableList.of(),
                            ImmutableList.of(0),
                            -1,
                            null,
                            RelCollations.EMPTY,
                            TYPE_FACTORY.createSqlType(SqlTypeName.DOUBLE),
                            "stddev_val");

            MergeAggregateExchange exchange =
                    MergeAggregateExchange.create(
                            createScan("value", "DOUBLE"),
                            ImmutableBitSet.of(),
                            ImmutableList.of(call));

            assertEquals(1, exchange.getMergeStrategies().size());
            assertEquals(
                    MergeAggregateExchange.MergeStrategy.WELFORD,
                    exchange.getMergeStrategies().get(0));
        }

        @Test
        @DisplayName("SUM uses SIMPLE merge strategy")
        void testSumUsesSimpleStrategy() {
            AggregateCall call =
                    AggregateCall.create(
                            SqlStdOperatorTable.SUM,
                            false,
                            false,
                            false,
                            ImmutableList.of(),
                            ImmutableList.of(0),
                            -1,
                            null,
                            RelCollations.EMPTY,
                            TYPE_FACTORY.createSqlType(SqlTypeName.DOUBLE),
                            "sum_val");

            MergeAggregateExchange exchange =
                    MergeAggregateExchange.create(
                            createScan("value", "DOUBLE"),
                            ImmutableBitSet.of(),
                            ImmutableList.of(call));

            assertEquals(1, exchange.getMergeStrategies().size());
            assertEquals(
                    MergeAggregateExchange.MergeStrategy.SIMPLE,
                    exchange.getMergeStrategies().get(0));
        }
    }

    @Nested
    @DisplayName("getMergeFunction for STDDEV/VAR")
    class GetMergeFunctionTests {

        @Test
        @DisplayName("STDDEV_POP partial columns merge via SUM")
        void testStddevPopMergeFunction() {
            // STDDEV/VAR partial columns (count, sum, sumSquares) all merge via SUM
            assertEquals(
                    SqlStdOperatorTable.SUM,
                    PartialAggregate.getMergeFunction(SqlKind.STDDEV_POP));
        }

        @Test
        @DisplayName("VAR_SAMP partial columns merge via SUM")
        void testVarSampMergeFunction() {
            assertEquals(
                    SqlStdOperatorTable.SUM,
                    PartialAggregate.getMergeFunction(SqlKind.VAR_SAMP));
        }
    }
}
