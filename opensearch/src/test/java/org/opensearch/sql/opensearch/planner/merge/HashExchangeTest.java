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

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;

@ExtendWith(MockitoExtension.class)
class HashExchangeTest {

    private RexBuilder rexBuilder;
    private RelOptCluster cluster;

    @Mock private RelNode mockInput;

    @BeforeEach
    void setUp() {
        rexBuilder = new RexBuilder(OpenSearchTypeFactory.TYPE_FACTORY);
        cluster = RelOptCluster.create(new VolcanoPlanner(), rexBuilder);
        lenient().when(mockInput.getCluster()).thenReturn(cluster);
    }

    @Test
    @DisplayName("create HashExchange with distribution keys and partitions")
    void testCreateHashExchange() {
        HashExchange exchange =
                new HashExchange(
                        cluster,
                        cluster.traitSetOf(EnumerableConvention.INSTANCE),
                        mockInput,
                        ImmutableList.of(0, 1),
                        8);
        assertEquals("HASH", exchange.getExchangeType());
        assertEquals(ImmutableList.of(0, 1), exchange.getDistributionKeys());
        assertEquals(8, exchange.getNumPartitions());
    }

    @Test
    @DisplayName("factory method creates HashExchange")
    void testFactoryMethod() {
        HashExchange exchange = HashExchange.create(mockInput, List.of(0, 2), 16);
        assertEquals("HASH", exchange.getExchangeType());
        assertEquals(ImmutableList.of(0, 2), exchange.getDistributionKeys());
        assertEquals(16, exchange.getNumPartitions());
    }

    @Test
    @DisplayName("copy preserves distribution keys and partition count")
    void testCopyPreservesFields() {
        HashExchange exchange =
                new HashExchange(
                        cluster,
                        cluster.traitSetOf(EnumerableConvention.INSTANCE),
                        mockInput,
                        ImmutableList.of(1, 3),
                        4);
        RelNode copied =
                exchange.copy(
                        cluster.traitSetOf(EnumerableConvention.INSTANCE),
                        List.of(mockInput));
        assertNotSame(exchange, copied);
        assertInstanceOf(HashExchange.class, copied);
        HashExchange copiedExchange = (HashExchange) copied;
        assertEquals(ImmutableList.of(1, 3), copiedExchange.getDistributionKeys());
        assertEquals(4, copiedExchange.getNumPartitions());
    }

    @Test
    @DisplayName("scan throws UnsupportedOperationException")
    void testScanNotYetImplemented() {
        HashExchange exchange =
                new HashExchange(
                        cluster,
                        cluster.traitSetOf(EnumerableConvention.INSTANCE),
                        mockInput,
                        ImmutableList.of(0),
                        8);
        assertThrows(UnsupportedOperationException.class, exchange::scan);
    }

    @Test
    @DisplayName("single distribution key")
    void testSingleDistributionKey() {
        HashExchange exchange = HashExchange.create(mockInput, List.of(0), 1);
        assertEquals(1, exchange.getDistributionKeys().size());
        assertEquals(1, exchange.getNumPartitions());
    }
}
