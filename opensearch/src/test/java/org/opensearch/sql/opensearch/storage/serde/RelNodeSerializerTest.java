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
import org.apache.calcite.adapter.enumerable.EnumerableFilter;
import org.apache.calcite.adapter.enumerable.EnumerableProject;
import org.apache.calcite.adapter.enumerable.EnumerableSort;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class RelNodeSerializerTest {

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
            String typeName = namesAndTypes[i + 1];
            switch (typeName) {
                case "INTEGER":
                    types.add(TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER));
                    break;
                case "VARCHAR":
                    types.add(TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR));
                    break;
                case "BOOLEAN":
                    types.add(TYPE_FACTORY.createSqlType(SqlTypeName.BOOLEAN));
                    break;
                default:
                    throw new IllegalArgumentException("Unknown type: " + typeName);
            }
        }
        return TYPE_FACTORY.createStructType(types.build(), names.build());
    }

    private RelNodeSerializer.ShardScanPlaceholder createScan(RelDataType rowType) {
        return new RelNodeSerializer.ShardScanPlaceholder(cluster, enumerableTraits, rowType);
    }

    @Test
    @DisplayName("round-trip ShardScanPlaceholder preserves row type")
    void testRoundTripScanPlaceholder() throws IOException {
        RelDataType rowType = createRowType("id", "INTEGER", "name", "VARCHAR");
        RelNode scan = createScan(rowType);

        String json = serializer.serialize(scan);
        assertNotNull(json);
        assertTrue(json.contains("ShardScanPlaceholder"));

        RelNode result = serializer.deserialize(json, cluster);
        assertInstanceOf(RelNodeSerializer.ShardScanPlaceholder.class, result);
        assertEquals(2, result.getRowType().getFieldCount());
        assertEquals("id", result.getRowType().getFieldNames().get(0));
        assertEquals("name", result.getRowType().getFieldNames().get(1));
    }

    @Test
    @DisplayName("round-trip EnumerableFilter preserves condition")
    void testRoundTripFilter() throws IOException {
        RelDataType rowType = createRowType("id", "INTEGER", "name", "VARCHAR");
        RelNode scan = createScan(rowType);

        // Create filter: id > 10
        RexInputRef idRef = rexBuilder.makeInputRef(
                TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER), 0);
        RexLiteral ten = rexBuilder.makeLiteral(
                10, TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER));
        RexNode condition = rexBuilder.makeCall(
                SqlStdOperatorTable.GREATER_THAN, idRef, ten);

        EnumerableFilter filter = new EnumerableFilter(
                cluster, enumerableTraits, scan, condition);

        String json = serializer.serialize(filter);
        assertNotNull(json);

        RelNode result = serializer.deserialize(json, cluster);
        assertInstanceOf(EnumerableFilter.class, result);
        EnumerableFilter resultFilter = (EnumerableFilter) result;
        // Filter should have the scan as input
        assertInstanceOf(
                RelNodeSerializer.ShardScanPlaceholder.class,
                resultFilter.getInput());
        // Condition should be preserved (GREATER_THAN)
        assertTrue(resultFilter.getCondition().toString().contains(">"));
    }

    @Test
    @DisplayName("round-trip EnumerableProject preserves expressions and field names")
    void testRoundTripProject() throws IOException {
        RelDataType rowType = createRowType("id", "INTEGER", "name", "VARCHAR");
        RelNode scan = createScan(rowType);

        // Project: select name (index 1)
        RexInputRef nameRef = rexBuilder.makeInputRef(
                TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR), 1);
        RelDataType projRowType = TYPE_FACTORY.createStructType(
                ImmutableList.of(TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR)),
                ImmutableList.of("name"));

        EnumerableProject project = new EnumerableProject(
                cluster, enumerableTraits, scan, ImmutableList.of(nameRef), projRowType);

        String json = serializer.serialize(project);
        assertNotNull(json);

        RelNode result = serializer.deserialize(json, cluster);
        assertInstanceOf(EnumerableProject.class, result);
        EnumerableProject resultProject = (EnumerableProject) result;
        assertEquals(1, resultProject.getProjects().size());
        assertEquals(1, resultProject.getRowType().getFieldCount());
        assertEquals("name", resultProject.getRowType().getFieldNames().get(0));
    }

    @Test
    @DisplayName("round-trip EnumerableSort preserves collation")
    void testRoundTripSort() throws IOException {
        RelDataType rowType = createRowType("id", "INTEGER", "name", "VARCHAR");
        RelNode scan = createScan(rowType);

        RelCollation collation = RelCollations.of(
                new RelFieldCollation(0, RelFieldCollation.Direction.DESCENDING));

        EnumerableSort sort = new EnumerableSort(
                cluster,
                enumerableTraits.replace(collation),
                scan,
                collation,
                null,
                null);

        String json = serializer.serialize(sort);
        assertNotNull(json);

        RelNode result = serializer.deserialize(json, cluster);
        assertInstanceOf(EnumerableSort.class, result);
        EnumerableSort resultSort = (EnumerableSort) result;
        assertEquals(1, resultSort.getCollation().getFieldCollations().size());
        assertEquals(
                RelFieldCollation.Direction.DESCENDING,
                resultSort.getCollation().getFieldCollations().get(0).direction);
    }

    @Test
    @DisplayName("round-trip Filter+Project chain preserves structure")
    void testRoundTripFilterProject() throws IOException {
        RelDataType rowType = createRowType("id", "INTEGER", "name", "VARCHAR");
        RelNode scan = createScan(rowType);

        // Filter: id > 5
        RexInputRef idRef = rexBuilder.makeInputRef(
                TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER), 0);
        RexLiteral five = rexBuilder.makeLiteral(
                5, TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER));
        RexNode condition = rexBuilder.makeCall(
                SqlStdOperatorTable.GREATER_THAN, idRef, five);
        EnumerableFilter filter = new EnumerableFilter(
                cluster, enumerableTraits, scan, condition);

        // Project: select name
        RexInputRef nameRef = rexBuilder.makeInputRef(
                TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR), 1);
        RelDataType projRowType = TYPE_FACTORY.createStructType(
                ImmutableList.of(TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR)),
                ImmutableList.of("name"));
        EnumerableProject project = new EnumerableProject(
                cluster, enumerableTraits, filter, ImmutableList.of(nameRef), projRowType);

        String json = serializer.serialize(project);
        assertNotNull(json);

        RelNode result = serializer.deserialize(json, cluster);
        assertInstanceOf(EnumerableProject.class, result);

        // Verify chain: Project -> Filter -> ShardScanPlaceholder
        RelNode resultFilter = ((EnumerableProject) result).getInput();
        assertInstanceOf(EnumerableFilter.class, resultFilter);

        RelNode resultScan = ((EnumerableFilter) resultFilter).getInput();
        assertInstanceOf(RelNodeSerializer.ShardScanPlaceholder.class, resultScan);
    }

    @Test
    @DisplayName("serialized JSON contains expected structure")
    void testSerializationFormat() {
        RelDataType rowType = createRowType("id", "INTEGER");
        RelNode scan = createScan(rowType);

        String json = serializer.serialize(scan);
        // Should contain rels array and ShardScanPlaceholder marker
        assertTrue(json.contains("\"rels\""));
        assertTrue(json.contains("\"relOp\""));
        assertTrue(json.contains("ShardScanPlaceholder"));
        assertTrue(json.contains("\"rowType\""));
    }
}
