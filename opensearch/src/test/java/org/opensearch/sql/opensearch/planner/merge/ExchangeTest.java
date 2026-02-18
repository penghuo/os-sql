/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.planner.merge;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.lenient;
import static org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.TYPE_FACTORY;

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ExchangeTest {

    private RexBuilder rexBuilder;
    private RelOptCluster cluster;

    @Mock private RelNode mockInput;

    @BeforeEach
    void setUp() {
        rexBuilder = new RexBuilder(TYPE_FACTORY);
        cluster = RelOptCluster.create(new VolcanoPlanner(), rexBuilder);
        // Configure mock so factory methods (which call input.getCluster()) work
        lenient().when(mockInput.getCluster()).thenReturn(cluster);
    }

    @Nested
    @DisplayName("ConcatExchange tests")
    class ConcatExchangeTests {

        @Test
        @DisplayName("create ConcatExchange with proper fields")
        void testCreateConcatExchange() {
            ConcatExchange exchange =
                    new ConcatExchange(
                            cluster,
                            cluster.traitSetOf(EnumerableConvention.INSTANCE),
                            mockInput);
            assertEquals("CONCAT", exchange.getExchangeType());
            assertInstanceOf(ConcatExchange.class, exchange);
        }

        @Test
        @DisplayName("copy produces equivalent ConcatExchange")
        void testCopyConcatExchange() {
            ConcatExchange exchange =
                    new ConcatExchange(
                            cluster,
                            cluster.traitSetOf(EnumerableConvention.INSTANCE),
                            mockInput);
            RelNode copied =
                    exchange.copy(
                            cluster.traitSetOf(EnumerableConvention.INSTANCE),
                            List.of(mockInput));
            assertNotSame(exchange, copied);
            assertInstanceOf(ConcatExchange.class, copied);
            assertEquals(
                    exchange.getExchangeType(),
                    ((ConcatExchange) copied).getExchangeType());
        }

        @Test
        @DisplayName("scan throws UnsupportedOperationException")
        void testScanNotYetImplemented() {
            ConcatExchange exchange =
                    new ConcatExchange(
                            cluster,
                            cluster.traitSetOf(EnumerableConvention.INSTANCE),
                            mockInput);
            assertThrows(UnsupportedOperationException.class, exchange::scan);
        }

        @Test
        @DisplayName("factory method creates ConcatExchange")
        void testFactoryMethod() {
            ConcatExchange exchange = ConcatExchange.create(mockInput);
            assertEquals("CONCAT", exchange.getExchangeType());
        }
    }

    @Nested
    @DisplayName("MergeSortExchange tests")
    class MergeSortExchangeTests {

        @Test
        @DisplayName("create MergeSortExchange with collation")
        void testCreateMergeSortExchange() {
            RelCollation collation =
                    RelCollations.of(
                            new RelFieldCollation(
                                    0, RelFieldCollation.Direction.ASCENDING));
            RexNode fetch = rexBuilder.makeLiteral(10, TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER));

            MergeSortExchange exchange =
                    new MergeSortExchange(
                            cluster,
                            cluster.traitSetOf(EnumerableConvention.INSTANCE)
                                    .replace(collation),
                            mockInput,
                            collation,
                            null,
                            fetch);

            assertEquals("MERGE_SORT", exchange.getExchangeType());
            assertEquals(collation, exchange.getCollation());
            assertEquals(fetch, exchange.getFetch());
            assertEquals(null, exchange.getOffset());
        }

        @Test
        @DisplayName("copy preserves collation and fetch")
        void testCopyMergeSortExchange() {
            RelCollation collation =
                    RelCollations.of(
                            new RelFieldCollation(
                                    0, RelFieldCollation.Direction.DESCENDING));
            RexNode fetch = rexBuilder.makeLiteral(5, TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER));
            RexNode offset = rexBuilder.makeLiteral(2, TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER));

            MergeSortExchange exchange =
                    new MergeSortExchange(
                            cluster,
                            cluster.traitSetOf(EnumerableConvention.INSTANCE)
                                    .replace(collation),
                            mockInput,
                            collation,
                            offset,
                            fetch);

            RelNode copied =
                    exchange.copy(
                            cluster.traitSetOf(EnumerableConvention.INSTANCE)
                                    .replace(collation),
                            List.of(mockInput));
            assertInstanceOf(MergeSortExchange.class, copied);
            MergeSortExchange copiedExchange = (MergeSortExchange) copied;
            assertEquals(collation, copiedExchange.getCollation());
            assertEquals(fetch, copiedExchange.getFetch());
            assertEquals(offset, copiedExchange.getOffset());
        }

        @Test
        @DisplayName("scan throws UnsupportedOperationException")
        void testScanNotYetImplemented() {
            RelCollation collation = RelCollations.EMPTY;
            MergeSortExchange exchange =
                    new MergeSortExchange(
                            cluster,
                            cluster.traitSetOf(EnumerableConvention.INSTANCE),
                            mockInput,
                            collation,
                            null,
                            null);
            assertThrows(UnsupportedOperationException.class, exchange::scan);
        }

        @Test
        @DisplayName("factory method creates MergeSortExchange with proper traits")
        void testFactoryMethod() {
            RelCollation collation =
                    RelCollations.of(
                            new RelFieldCollation(
                                    0, RelFieldCollation.Direction.ASCENDING));
            RexNode fetch = rexBuilder.makeLiteral(10, TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER));

            MergeSortExchange exchange =
                    MergeSortExchange.create(mockInput, collation, null, fetch);
            assertEquals("MERGE_SORT", exchange.getExchangeType());
            assertEquals(collation, exchange.getCollation());
            assertEquals(fetch, exchange.getFetch());
        }
    }

    @Nested
    @DisplayName("MergeAggregateExchange tests")
    class MergeAggregateExchangeTests {

        @Test
        @DisplayName("create MergeAggregateExchange with groupSet and aggCalls")
        void testCreateMergeAggregateExchange() {
            ImmutableBitSet groupSet = ImmutableBitSet.of(0);
            AggregateCall sumCall =
                    AggregateCall.create(
                            SqlStdOperatorTable.SUM,
                            false,
                            false,
                            false,
                            ImmutableList.of(),
                            ImmutableList.of(1),
                            -1,
                            null,
                            RelCollations.EMPTY,
                            TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER),
                            "total");

            MergeAggregateExchange exchange =
                    new MergeAggregateExchange(
                            cluster,
                            cluster.traitSetOf(EnumerableConvention.INSTANCE),
                            mockInput,
                            groupSet,
                            ImmutableList.of(sumCall));

            assertEquals("MERGE_AGGREGATE", exchange.getExchangeType());
            assertEquals(groupSet, exchange.getGroupSet());
            assertEquals(1, exchange.getAggCalls().size());
            assertEquals("total", exchange.getAggCalls().get(0).getName());
        }

        @Test
        @DisplayName("copy preserves groupSet and aggCalls")
        void testCopyMergeAggregateExchange() {
            ImmutableBitSet groupSet = ImmutableBitSet.of(0, 1);
            AggregateCall countCall =
                    AggregateCall.create(
                            SqlStdOperatorTable.COUNT,
                            false,
                            false,
                            false,
                            ImmutableList.of(),
                            ImmutableList.of(),
                            -1,
                            null,
                            RelCollations.EMPTY,
                            TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT),
                            "cnt");

            MergeAggregateExchange exchange =
                    new MergeAggregateExchange(
                            cluster,
                            cluster.traitSetOf(EnumerableConvention.INSTANCE),
                            mockInput,
                            groupSet,
                            ImmutableList.of(countCall));

            RelNode copied =
                    exchange.copy(
                            cluster.traitSetOf(EnumerableConvention.INSTANCE),
                            List.of(mockInput));
            assertInstanceOf(MergeAggregateExchange.class, copied);
            MergeAggregateExchange copiedExchange = (MergeAggregateExchange) copied;
            assertEquals(groupSet, copiedExchange.getGroupSet());
            assertEquals(1, copiedExchange.getAggCalls().size());
            assertEquals("cnt", copiedExchange.getAggCalls().get(0).getName());
        }

        @Test
        @DisplayName("scan throws UnsupportedOperationException")
        void testScanNotYetImplemented() {
            MergeAggregateExchange exchange =
                    new MergeAggregateExchange(
                            cluster,
                            cluster.traitSetOf(EnumerableConvention.INSTANCE),
                            mockInput,
                            ImmutableBitSet.of(),
                            ImmutableList.of());
            assertThrows(UnsupportedOperationException.class, exchange::scan);
        }

        @Test
        @DisplayName("factory method creates MergeAggregateExchange")
        void testFactoryMethod() {
            ImmutableBitSet groupSet = ImmutableBitSet.of(0);
            AggregateCall minCall =
                    AggregateCall.create(
                            SqlStdOperatorTable.MIN,
                            false,
                            false,
                            false,
                            ImmutableList.of(),
                            ImmutableList.of(1),
                            -1,
                            null,
                            RelCollations.EMPTY,
                            TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER),
                            "min_val");

            MergeAggregateExchange exchange =
                    MergeAggregateExchange.create(
                            mockInput, groupSet, ImmutableList.of(minCall));
            assertEquals("MERGE_AGGREGATE", exchange.getExchangeType());
            assertEquals(groupSet, exchange.getGroupSet());
            assertEquals(1, exchange.getAggCalls().size());
            assertEquals("min_val", exchange.getAggCalls().get(0).getName());
        }
    }
}
