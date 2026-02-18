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

import java.util.List;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class LocalJoinExchangeTest {

    private RexBuilder rexBuilder;
    private RelOptCluster cluster;

    @Mock private RelNode mockInput;

    @BeforeEach
    void setUp() {
        rexBuilder = new RexBuilder(TYPE_FACTORY);
        cluster = RelOptCluster.create(new VolcanoPlanner(), rexBuilder);
        lenient().when(mockInput.getCluster()).thenReturn(cluster);
    }

    @Test
    @DisplayName("create LocalJoinExchange with join type and condition")
    void testCreateLocalJoinExchange() {
        RexNode condition =
                rexBuilder.makeLiteral(true, TYPE_FACTORY.createSqlType(SqlTypeName.BOOLEAN));
        LocalJoinExchange exchange =
                new LocalJoinExchange(
                        cluster,
                        cluster.traitSetOf(EnumerableConvention.INSTANCE),
                        mockInput,
                        JoinRelType.INNER,
                        condition);
        assertEquals("LOCAL_JOIN", exchange.getExchangeType());
        assertEquals(JoinRelType.INNER, exchange.getJoinType());
        assertEquals(condition, exchange.getCondition());
    }

    @Test
    @DisplayName("factory method creates LocalJoinExchange")
    void testFactoryMethod() {
        RexNode condition =
                rexBuilder.makeLiteral(true, TYPE_FACTORY.createSqlType(SqlTypeName.BOOLEAN));
        LocalJoinExchange exchange =
                LocalJoinExchange.create(mockInput, JoinRelType.LEFT, condition);
        assertEquals("LOCAL_JOIN", exchange.getExchangeType());
        assertEquals(JoinRelType.LEFT, exchange.getJoinType());
    }

    @Test
    @DisplayName("copy preserves join type and condition")
    void testCopyPreservesFields() {
        RexNode condition =
                rexBuilder.makeLiteral(true, TYPE_FACTORY.createSqlType(SqlTypeName.BOOLEAN));
        LocalJoinExchange exchange =
                new LocalJoinExchange(
                        cluster,
                        cluster.traitSetOf(EnumerableConvention.INSTANCE),
                        mockInput,
                        JoinRelType.FULL,
                        condition);
        RelNode copied =
                exchange.copy(
                        cluster.traitSetOf(EnumerableConvention.INSTANCE),
                        List.of(mockInput));
        assertNotSame(exchange, copied);
        assertInstanceOf(LocalJoinExchange.class, copied);
        LocalJoinExchange copiedExchange = (LocalJoinExchange) copied;
        assertEquals(JoinRelType.FULL, copiedExchange.getJoinType());
        assertEquals(condition, copiedExchange.getCondition());
    }

    @Test
    @DisplayName("scan throws UnsupportedOperationException")
    void testScanNotYetImplemented() {
        RexNode condition =
                rexBuilder.makeLiteral(true, TYPE_FACTORY.createSqlType(SqlTypeName.BOOLEAN));
        LocalJoinExchange exchange =
                new LocalJoinExchange(
                        cluster,
                        cluster.traitSetOf(EnumerableConvention.INSTANCE),
                        mockInput,
                        JoinRelType.INNER,
                        condition);
        assertThrows(UnsupportedOperationException.class, exchange::scan);
    }
}
