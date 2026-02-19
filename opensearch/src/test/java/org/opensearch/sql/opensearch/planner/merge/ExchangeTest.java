/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.planner.merge;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.lenient;
import static org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.TYPE_FACTORY;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
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
        @DisplayName("scan returns empty enumerable when no shard results set")
        void testScanEmptyWithoutShardResults() {
            ConcatExchange exchange =
                    new ConcatExchange(
                            cluster,
                            cluster.traitSetOf(EnumerableConvention.INSTANCE),
                            mockInput);
            assertFalse(exchange.scan().enumerator().moveNext());
        }

        @Test
        @DisplayName("factory method creates ConcatExchange")
        void testFactoryMethod() {
            ConcatExchange exchange = ConcatExchange.create(mockInput);
            assertEquals("CONCAT", exchange.getExchangeType());
        }

        @Test
        @DisplayName("scan concatenates rows from multiple shards")
        void testScanWithMultipleShardResults() {
            ConcatExchange exchange =
                    new ConcatExchange(
                            cluster,
                            cluster.traitSetOf(EnumerableConvention.INSTANCE),
                            mockInput);

            Exchange.ShardResult shard0 =
                    new Exchange.ShardResult(
                            List.of(
                                    new Object[] {"Alice", 30},
                                    new Object[] {"Bob", 25}),
                            List.of("name", "age"),
                            0);
            Exchange.ShardResult shard1 =
                    new Exchange.ShardResult(
                            Arrays.<Object[]>asList(new Object[] {"Charlie", 35}),
                            List.of("name", "age"),
                            1);
            exchange.setShardResults(List.of(shard0, shard1));

            List<Object[]> results = collectResults(exchange.scan());

            assertEquals(3, results.size());
            assertArrayEquals(new Object[] {"Alice", 30}, results.get(0));
            assertArrayEquals(new Object[] {"Bob", 25}, results.get(1));
            assertArrayEquals(new Object[] {"Charlie", 35}, results.get(2));
        }

        @Test
        @DisplayName("scan with single shard returns all its rows")
        void testScanWithSingleShard() {
            ConcatExchange exchange =
                    new ConcatExchange(
                            cluster,
                            cluster.traitSetOf(EnumerableConvention.INSTANCE),
                            mockInput);

            Exchange.ShardResult shard0 =
                    new Exchange.ShardResult(
                            List.of(
                                    new Object[] {1, "x"},
                                    new Object[] {2, "y"},
                                    new Object[] {3, "z"}),
                            List.of("id", "val"),
                            0);
            exchange.setShardResults(List.of(shard0));

            List<Object[]> results = collectResults(exchange.scan());

            assertEquals(3, results.size());
            assertArrayEquals(new Object[] {1, "x"}, results.get(0));
            assertArrayEquals(new Object[] {2, "y"}, results.get(1));
            assertArrayEquals(new Object[] {3, "z"}, results.get(2));
        }

        @Test
        @DisplayName("scan returns empty when shard results have empty rows")
        void testScanWithEmptyShardResults() {
            ConcatExchange exchange =
                    new ConcatExchange(
                            cluster,
                            cluster.traitSetOf(EnumerableConvention.INSTANCE),
                            mockInput);

            Exchange.ShardResult shard0 =
                    new Exchange.ShardResult(List.of(), List.of("id"), 0);
            Exchange.ShardResult shard1 =
                    new Exchange.ShardResult(List.of(), List.of("id"), 1);
            exchange.setShardResults(List.of(shard0, shard1));

            List<Object[]> results = collectResults(exchange.scan());

            assertTrue(results.isEmpty());
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
        @DisplayName("scan returns empty enumerable when no shard results set")
        void testScanEmptyWithoutShardResults() {
            RelCollation collation = RelCollations.EMPTY;
            MergeSortExchange exchange =
                    new MergeSortExchange(
                            cluster,
                            cluster.traitSetOf(EnumerableConvention.INSTANCE),
                            mockInput,
                            collation,
                            null,
                            null);
            assertFalse(exchange.scan().enumerator().moveNext());
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

        @Test
        @DisplayName("scan merge-sorts ascending results from multiple shards")
        void testScanMergeSortAscending() {
            RelCollation collation =
                    RelCollations.of(
                            new RelFieldCollation(
                                    0, RelFieldCollation.Direction.ASCENDING));

            MergeSortExchange exchange =
                    new MergeSortExchange(
                            cluster,
                            cluster.traitSetOf(EnumerableConvention.INSTANCE)
                                    .replace(collation),
                            mockInput,
                            collation,
                            null,
                            null);

            // Shard 0: pre-sorted ascending by column 0
            Exchange.ShardResult shard0 =
                    new Exchange.ShardResult(
                            List.of(
                                    new Object[] {1, "a"},
                                    new Object[] {3, "c"},
                                    new Object[] {5, "e"}),
                            List.of("id", "val"),
                            0);
            // Shard 1: pre-sorted ascending by column 0
            Exchange.ShardResult shard1 =
                    new Exchange.ShardResult(
                            List.of(
                                    new Object[] {2, "b"},
                                    new Object[] {4, "d"},
                                    new Object[] {6, "f"}),
                            List.of("id", "val"),
                            1);
            exchange.setShardResults(List.of(shard0, shard1));

            List<Object[]> results = collectResults(exchange.scan());

            assertEquals(6, results.size());
            // Verify globally sorted order
            for (int i = 0; i < results.size(); i++) {
                assertEquals(i + 1, results.get(i)[0]);
            }
        }

        @Test
        @DisplayName("scan merge-sorts descending results from multiple shards")
        void testScanMergeSortDescending() {
            RelCollation collation =
                    RelCollations.of(
                            new RelFieldCollation(
                                    0, RelFieldCollation.Direction.DESCENDING));

            MergeSortExchange exchange =
                    new MergeSortExchange(
                            cluster,
                            cluster.traitSetOf(EnumerableConvention.INSTANCE)
                                    .replace(collation),
                            mockInput,
                            collation,
                            null,
                            null);

            Exchange.ShardResult shard0 =
                    new Exchange.ShardResult(
                            List.of(
                                    new Object[] {5, "e"},
                                    new Object[] {3, "c"},
                                    new Object[] {1, "a"}),
                            List.of("id", "val"),
                            0);
            Exchange.ShardResult shard1 =
                    new Exchange.ShardResult(
                            List.of(
                                    new Object[] {6, "f"},
                                    new Object[] {4, "d"},
                                    new Object[] {2, "b"}),
                            List.of("id", "val"),
                            1);
            exchange.setShardResults(List.of(shard0, shard1));

            List<Object[]> results = collectResults(exchange.scan());

            assertEquals(6, results.size());
            // Verify descending order: 6, 5, 4, 3, 2, 1
            for (int i = 0; i < results.size(); i++) {
                assertEquals(6 - i, results.get(i)[0]);
            }
        }

        @Test
        @DisplayName("scan applies offset and fetch to merge-sorted results")
        void testScanMergeSortWithOffsetAndFetch() {
            RelCollation collation =
                    RelCollations.of(
                            new RelFieldCollation(
                                    0, RelFieldCollation.Direction.ASCENDING));
            RexNode offset =
                    rexBuilder.makeLiteral(
                            2, TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER));
            RexNode fetch =
                    rexBuilder.makeLiteral(
                            3, TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER));

            MergeSortExchange exchange =
                    new MergeSortExchange(
                            cluster,
                            cluster.traitSetOf(EnumerableConvention.INSTANCE)
                                    .replace(collation),
                            mockInput,
                            collation,
                            offset,
                            fetch);

            Exchange.ShardResult shard0 =
                    new Exchange.ShardResult(
                            List.of(
                                    new Object[] {1, "a"},
                                    new Object[] {3, "c"},
                                    new Object[] {5, "e"}),
                            List.of("id", "val"),
                            0);
            Exchange.ShardResult shard1 =
                    new Exchange.ShardResult(
                            List.of(
                                    new Object[] {2, "b"},
                                    new Object[] {4, "d"},
                                    new Object[] {6, "f"}),
                            List.of("id", "val"),
                            1);
            exchange.setShardResults(List.of(shard0, shard1));

            List<Object[]> results = collectResults(exchange.scan());

            // offset=2 skips [1,a] and [2,b]; fetch=3 takes [3,c], [4,d], [5,e]
            assertEquals(3, results.size());
            assertEquals(3, results.get(0)[0]);
            assertEquals(4, results.get(1)[0]);
            assertEquals(5, results.get(2)[0]);
        }

        @Test
        @DisplayName("scan applies fetch only (no offset)")
        void testScanMergeSortWithFetchOnly() {
            RelCollation collation =
                    RelCollations.of(
                            new RelFieldCollation(
                                    0, RelFieldCollation.Direction.ASCENDING));
            RexNode fetch =
                    rexBuilder.makeLiteral(
                            2, TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER));

            MergeSortExchange exchange =
                    new MergeSortExchange(
                            cluster,
                            cluster.traitSetOf(EnumerableConvention.INSTANCE)
                                    .replace(collation),
                            mockInput,
                            collation,
                            null,
                            fetch);

            Exchange.ShardResult shard0 =
                    new Exchange.ShardResult(
                            List.of(
                                    new Object[] {1, "a"},
                                    new Object[] {3, "c"}),
                            List.of("id", "val"),
                            0);
            Exchange.ShardResult shard1 =
                    new Exchange.ShardResult(
                            List.of(
                                    new Object[] {2, "b"},
                                    new Object[] {4, "d"}),
                            List.of("id", "val"),
                            1);
            exchange.setShardResults(List.of(shard0, shard1));

            List<Object[]> results = collectResults(exchange.scan());

            assertEquals(2, results.size());
            assertEquals(1, results.get(0)[0]);
            assertEquals(2, results.get(1)[0]);
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
        @DisplayName("scan returns empty enumerable when no shard results set")
        void testScanEmptyWithoutShardResults() {
            MergeAggregateExchange exchange =
                    new MergeAggregateExchange(
                            cluster,
                            cluster.traitSetOf(EnumerableConvention.INSTANCE),
                            mockInput,
                            ImmutableBitSet.of(),
                            ImmutableList.of());
            assertFalse(exchange.scan().enumerator().moveNext());
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

        @Test
        @DisplayName("scan merges partial SUM results across shards")
        void testScanMergeSum() {
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

            // Shard 0: group "A" with partial sum 10, group "B" with partial sum 20
            // Shard 1: group "A" with partial sum 15, group "B" with partial sum 25
            // Row layout: [group_key, partial_sum]
            Exchange.ShardResult shard0 =
                    new Exchange.ShardResult(
                            List.of(
                                    new Object[] {"A", 10L},
                                    new Object[] {"B", 20L}),
                            List.of("group", "total"),
                            0);
            Exchange.ShardResult shard1 =
                    new Exchange.ShardResult(
                            List.of(
                                    new Object[] {"A", 15L},
                                    new Object[] {"B", 25L}),
                            List.of("group", "total"),
                            1);
            exchange.setShardResults(List.of(shard0, shard1));

            List<Object[]> results = collectResults(exchange.scan());

            assertEquals(2, results.size());
            assertEquals("A", results.get(0)[0]);
            assertEquals(25L, results.get(0)[1]); // 10 + 15
            assertEquals("B", results.get(1)[0]);
            assertEquals(45L, results.get(1)[1]); // 20 + 25
        }

        @Test
        @DisplayName("scan merges partial COUNT results across shards")
        void testScanMergeCount() {
            ImmutableBitSet groupSet = ImmutableBitSet.of(0);
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

            Exchange.ShardResult shard0 =
                    new Exchange.ShardResult(
                            Arrays.<Object[]>asList(new Object[] {"X", 100L}),
                            List.of("group", "cnt"),
                            0);
            Exchange.ShardResult shard1 =
                    new Exchange.ShardResult(
                            Arrays.<Object[]>asList(new Object[] {"X", 200L}),
                            List.of("group", "cnt"),
                            1);
            exchange.setShardResults(List.of(shard0, shard1));

            List<Object[]> results = collectResults(exchange.scan());

            assertEquals(1, results.size());
            assertEquals("X", results.get(0)[0]);
            assertEquals(300L, results.get(0)[1]); // 100 + 200
        }

        @Test
        @DisplayName("scan merges partial MIN results across shards")
        void testScanMergeMin() {
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
                    new MergeAggregateExchange(
                            cluster,
                            cluster.traitSetOf(EnumerableConvention.INSTANCE),
                            mockInput,
                            groupSet,
                            ImmutableList.of(minCall));

            Exchange.ShardResult shard0 =
                    new Exchange.ShardResult(
                            Arrays.<Object[]>asList(new Object[] {"G1", 5}),
                            List.of("group", "min_val"),
                            0);
            Exchange.ShardResult shard1 =
                    new Exchange.ShardResult(
                            Arrays.<Object[]>asList(new Object[] {"G1", 3}),
                            List.of("group", "min_val"),
                            1);
            Exchange.ShardResult shard2 =
                    new Exchange.ShardResult(
                            Arrays.<Object[]>asList(new Object[] {"G1", 7}),
                            List.of("group", "min_val"),
                            2);
            exchange.setShardResults(List.of(shard0, shard1, shard2));

            List<Object[]> results = collectResults(exchange.scan());

            assertEquals(1, results.size());
            assertEquals("G1", results.get(0)[0]);
            assertEquals(3, results.get(0)[1]); // min(5, 3, 7) = 3
        }

        @Test
        @DisplayName("scan merges partial MAX results across shards")
        void testScanMergeMax() {
            ImmutableBitSet groupSet = ImmutableBitSet.of(0);
            AggregateCall maxCall =
                    AggregateCall.create(
                            SqlStdOperatorTable.MAX,
                            false,
                            false,
                            false,
                            ImmutableList.of(),
                            ImmutableList.of(1),
                            -1,
                            null,
                            RelCollations.EMPTY,
                            TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER),
                            "max_val");

            MergeAggregateExchange exchange =
                    new MergeAggregateExchange(
                            cluster,
                            cluster.traitSetOf(EnumerableConvention.INSTANCE),
                            mockInput,
                            groupSet,
                            ImmutableList.of(maxCall));

            Exchange.ShardResult shard0 =
                    new Exchange.ShardResult(
                            Arrays.<Object[]>asList(new Object[] {"G1", 5}),
                            List.of("group", "max_val"),
                            0);
            Exchange.ShardResult shard1 =
                    new Exchange.ShardResult(
                            Arrays.<Object[]>asList(new Object[] {"G1", 3}),
                            List.of("group", "max_val"),
                            1);
            Exchange.ShardResult shard2 =
                    new Exchange.ShardResult(
                            Arrays.<Object[]>asList(new Object[] {"G1", 7}),
                            List.of("group", "max_val"),
                            2);
            exchange.setShardResults(List.of(shard0, shard1, shard2));

            List<Object[]> results = collectResults(exchange.scan());

            assertEquals(1, results.size());
            assertEquals("G1", results.get(0)[0]);
            assertEquals(7, results.get(0)[1]); // max(5, 3, 7) = 7
        }

        @Test
        @DisplayName("scan merges multiple groups with SUM and COUNT")
        void testScanMergeMultipleGroupsMultipleAggs() {
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
                            ImmutableList.of(sumCall, countCall));

            // Row layout: [group_key, partial_sum, partial_count]
            Exchange.ShardResult shard0 =
                    new Exchange.ShardResult(
                            List.of(
                                    new Object[] {"A", 10L, 2L},
                                    new Object[] {"B", 30L, 3L}),
                            List.of("group", "total", "cnt"),
                            0);
            Exchange.ShardResult shard1 =
                    new Exchange.ShardResult(
                            List.of(
                                    new Object[] {"A", 20L, 4L},
                                    new Object[] {"B", 40L, 5L}),
                            List.of("group", "total", "cnt"),
                            1);
            exchange.setShardResults(List.of(shard0, shard1));

            List<Object[]> results = collectResults(exchange.scan());

            assertEquals(2, results.size());
            // Group "A": SUM=30, COUNT=6
            assertEquals("A", results.get(0)[0]);
            assertEquals(30L, results.get(0)[1]);
            assertEquals(6L, results.get(0)[2]);
            // Group "B": SUM=70, COUNT=8
            assertEquals("B", results.get(1)[0]);
            assertEquals(70L, results.get(1)[1]);
            assertEquals(8L, results.get(1)[2]);
        }

        @Test
        @DisplayName("scan without group keys merges global aggregates")
        void testScanMergeNoGroupKeys() {
            ImmutableBitSet groupSet = ImmutableBitSet.of();
            AggregateCall sumCall =
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
                            TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER),
                            "total");

            MergeAggregateExchange exchange =
                    new MergeAggregateExchange(
                            cluster,
                            cluster.traitSetOf(EnumerableConvention.INSTANCE),
                            mockInput,
                            groupSet,
                            ImmutableList.of(sumCall));

            // Row layout: [partial_sum] (no group key columns)
            Exchange.ShardResult shard0 =
                    new Exchange.ShardResult(
                            Arrays.<Object[]>asList(new Object[] {100L}),
                            List.of("total"),
                            0);
            Exchange.ShardResult shard1 =
                    new Exchange.ShardResult(
                            Arrays.<Object[]>asList(new Object[] {200L}),
                            List.of("total"),
                            1);
            exchange.setShardResults(List.of(shard0, shard1));

            List<Object[]> results = collectResults(exchange.scan());

            assertEquals(1, results.size());
            assertEquals(300L, results.get(0)[0]); // 100 + 200
        }
    }

    /** Helper to collect all rows from an Enumerable scan result. */
    private static List<Object[]> collectResults(Enumerable<?> enumerable) {
        List<Object[]> results = new ArrayList<>();
        try (Enumerator<?> enumerator = enumerable.enumerator()) {
            while (enumerator.moveNext()) {
                Object current = enumerator.current();
                if (current instanceof Object[]) {
                    results.add((Object[]) current);
                } else {
                    results.add(new Object[] {current});
                }
            }
        }
        return results;
    }
}
