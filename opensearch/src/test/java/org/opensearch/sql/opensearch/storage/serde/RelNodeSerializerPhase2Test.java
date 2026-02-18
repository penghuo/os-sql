/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.serde;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.TYPE_FACTORY;

import com.google.common.collect.ImmutableList;
import java.io.IOException;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.opensearch.planner.merge.HashExchange;

/**
 * Tests for Phase 2 RelNodeSerializer extensions: round-trip serialization of HashExchange and
 * other new node types.
 */
class RelNodeSerializerPhase2Test {

    private RexBuilder rexBuilder;
    private RelOptCluster cluster;
    private RelTraitSet enumerableTraits;
    private RelNodeSerializer serializer;

    @BeforeEach
    void setUp() {
        rexBuilder = new RexBuilder(TYPE_FACTORY);
        VolcanoPlanner planner = new VolcanoPlanner();
        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
        planner.addRelTraitDef(RelCollationTraitDef.INSTANCE);
        cluster = RelOptCluster.create(planner, rexBuilder);
        enumerableTraits = cluster.traitSetOf(EnumerableConvention.INSTANCE);
        serializer = new RelNodeSerializer();
    }

    private RelDataType createRowType(String... namesAndTypes) {
        ImmutableList.Builder<RelDataType> types = ImmutableList.builder();
        ImmutableList.Builder<String> names = ImmutableList.builder();
        for (int i = 0; i < namesAndTypes.length; i += 2) {
            names.add(namesAndTypes[i]);
            types.add(TYPE_FACTORY.createSqlType(SqlTypeName.valueOf(namesAndTypes[i + 1])));
        }
        return TYPE_FACTORY.createStructType(types.build(), names.build());
    }

    private RelNodeSerializer.ShardScanPlaceholder createScan(RelDataType rowType) {
        return new RelNodeSerializer.ShardScanPlaceholder(cluster, enumerableTraits, rowType);
    }

    @Test
    @DisplayName("round-trip HashExchange preserves distribution keys and partitions")
    void testRoundTripHashExchange() throws IOException {
        RelDataType rowType = createRowType("id", "INTEGER", "name", "VARCHAR");
        RelNode scan = createScan(rowType);

        HashExchange exchange =
                new HashExchange(
                        cluster,
                        enumerableTraits,
                        scan,
                        ImmutableList.of(0),
                        8);

        String json = serializer.serialize(exchange);
        assertNotNull(json);
        assertTrue(json.contains("HashExchange"));
        assertTrue(json.contains("distributionKeys"));
        assertTrue(json.contains("numPartitions"));

        RelNode result = serializer.deserialize(json, cluster);
        assertInstanceOf(HashExchange.class, result);
        HashExchange resultExchange = (HashExchange) result;
        assertEquals(ImmutableList.of(0), resultExchange.getDistributionKeys());
        assertEquals(8, resultExchange.getNumPartitions());

        // Verify the input was preserved
        assertInstanceOf(
                RelNodeSerializer.ShardScanPlaceholder.class,
                resultExchange.getInput());
    }

    @Test
    @DisplayName("round-trip HashExchange with multiple distribution keys")
    void testRoundTripHashExchangeMultipleKeys() throws IOException {
        RelDataType rowType =
                createRowType("col_a", "INTEGER", "col_b", "VARCHAR", "col_c", "INTEGER");
        RelNode scan = createScan(rowType);

        HashExchange exchange =
                new HashExchange(
                        cluster,
                        enumerableTraits,
                        scan,
                        ImmutableList.of(0, 2),
                        16);

        String json = serializer.serialize(exchange);
        RelNode result = serializer.deserialize(json, cluster);
        assertInstanceOf(HashExchange.class, result);
        HashExchange resultExchange = (HashExchange) result;
        assertEquals(ImmutableList.of(0, 2), resultExchange.getDistributionKeys());
        assertEquals(16, resultExchange.getNumPartitions());
    }

    @Test
    @DisplayName("serialized HashExchange JSON contains expected structure")
    void testHashExchangeSerializationFormat() {
        RelDataType rowType = createRowType("id", "INTEGER");
        RelNode scan = createScan(rowType);
        HashExchange exchange =
                new HashExchange(
                        cluster, enumerableTraits, scan, ImmutableList.of(0), 4);

        String json = serializer.serialize(exchange);
        assertTrue(json.contains("\"rels\""));
        assertTrue(json.contains("\"relOp\""));
        assertTrue(json.contains("ShardScanPlaceholder"));
        // HashExchange serialized by Calcite's explain mechanism will include its class name
        assertTrue(json.contains("HashExchange"));
    }
}
