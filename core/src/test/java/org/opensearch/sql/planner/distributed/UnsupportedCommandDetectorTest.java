/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.TYPE_FACTORY;

import com.google.common.collect.ImmutableList;
import java.util.Collections;
import java.util.List;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.opensearch.sql.calcite.utils.CalciteToolsHelper.OpenSearchRelBuilder;
import org.opensearch.sql.planner.distributed.UnsupportedCommandDetector.DetectionResult;
import org.opensearch.sql.planner.distributed.UnsupportedCommandDetector.Support;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class UnsupportedCommandDetectorTest {

    @Mock RelNode mockInput;
    @Mock TableScan mockTableScan;
    @Mock RelOptCluster cluster;
    @Mock RelOptPlanner planner;
    @Mock RelMetadataQuery mq;

    RelDataType bigintType = TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT);
    RelDataType rowType =
            TYPE_FACTORY.createStructType(List.of(bigintType, bigintType), List.of("a", "b"));
    RexBuilder rexBuilder = new RexBuilder(TYPE_FACTORY);
    RelBuilder relBuilder;

    UnsupportedCommandDetector detector;

    @BeforeEach
    void setUp() {
        when(cluster.getTypeFactory()).thenReturn(TYPE_FACTORY);
        when(cluster.getRexBuilder()).thenReturn(rexBuilder);
        when(mq.isVisibleInExplain(any(), any())).thenReturn(true);
        when(cluster.getMetadataQuery()).thenReturn(mq);
        when(cluster.traitSet()).thenReturn(RelTraitSet.createEmpty());
        when(cluster.traitSetOf(Convention.NONE))
                .thenReturn(RelTraitSet.createEmpty().replace(Convention.NONE));
        when(cluster.getPlanner()).thenReturn(planner);
        when(planner.getExecutor()).thenReturn(null);

        // mockInput is a plain RelNode mock – NOT a supported type, usable with RelBuilder
        when(mockInput.getCluster()).thenReturn(cluster);
        when(mockInput.getRowType()).thenReturn(rowType);
        when(mockInput.getTraitSet())
                .thenReturn(RelTraitSet.createEmpty().replace(Convention.NONE));
        when(mockInput.getInputs()).thenReturn(Collections.emptyList());

        // mockTableScan is a TableScan mock – a supported type, for direct detect() calls only
        when(mockTableScan.getCluster()).thenReturn(cluster);
        when(mockTableScan.getRowType()).thenReturn(rowType);
        when(mockTableScan.getTraitSet())
                .thenReturn(RelTraitSet.createEmpty().replace(Convention.NONE));
        when(mockTableScan.getInputs()).thenReturn(Collections.emptyList());

        relBuilder = new OpenSearchRelBuilder(null, cluster, null);

        detector = new UnsupportedCommandDetector();
    }

    @Test
    @DisplayName("TableScan should be supported")
    void tableScanIsSupported() {
        DetectionResult result = detector.detect(mockTableScan);

        assertTrue(result.isSupported());
        assertEquals(Support.SUPPORTED, result.getSupport());
        assertNull(result.getReason());
    }

    @Test
    @DisplayName("Aggregate over scan should be supported")
    void aggregateOverScanIsSupported() {
        // Use mockInput with RelBuilder, then replace input with mockTableScan for detection
        relBuilder.push(mockInput);
        RelBuilder.AggCall aggCall = relBuilder.aggregateCall(SqlStdOperatorTable.COUNT);
        LogicalAggregate aggregate =
                (LogicalAggregate)
                        relBuilder
                                .aggregate(relBuilder.groupKey(0), ImmutableList.of(aggCall))
                                .build();

        // Replace the child with a supported TableScan mock
        LogicalAggregate aggregateWithScan =
                aggregate.copy(aggregate.getTraitSet(), mockTableScan,
                        aggregate.getGroupSet(), aggregate.getGroupSets(),
                        aggregate.getAggCallList());

        DetectionResult result = detector.detect(aggregateWithScan);

        assertTrue(result.isSupported());
        assertEquals(Support.SUPPORTED, result.getSupport());
    }

    @Test
    @DisplayName("Sort over scan should be supported")
    void sortOverScanIsSupported() {
        relBuilder.push(mockInput);
        LogicalSort sort = (LogicalSort) relBuilder.sort(relBuilder.field("a")).build();

        // Replace the child with a supported TableScan mock
        LogicalSort sortWithScan =
                (LogicalSort) sort.copy(sort.getTraitSet(), mockTableScan,
                        sort.getCollation(), sort.offset, sort.fetch);

        DetectionResult result = detector.detect(sortWithScan);

        assertTrue(result.isSupported());
        assertEquals(Support.SUPPORTED, result.getSupport());
    }

    @Test
    @DisplayName("Join should be supported")
    void joinIsSupported() {
        relBuilder.push(mockInput);
        relBuilder.push(mockInput);
        RelNode join =
                relBuilder
                        .join(
                                org.apache.calcite.rel.core.JoinRelType.INNER,
                                relBuilder.equals(
                                        relBuilder.field(2, 0, "a"),
                                        relBuilder.field(2, 1, "a")))
                        .build();

        // Replace both children with supported TableScan mocks
        org.apache.calcite.rel.logical.LogicalJoin logicalJoin =
                (org.apache.calcite.rel.logical.LogicalJoin) join;
        RelNode joinWithScans =
                logicalJoin.copy(logicalJoin.getTraitSet(),
                        logicalJoin.getCondition(),
                        mockTableScan, mockTableScan,
                        logicalJoin.getJoinType(),
                        logicalJoin.isSemiJoinDone());

        DetectionResult result = detector.detect(joinWithScans);

        assertTrue(result.isSupported());
        assertEquals(Support.SUPPORTED, result.getSupport());
    }

    @Test
    @DisplayName("Filter over scan should be supported")
    void filterOverScanIsSupported() {
        relBuilder.push(mockInput);
        RexNode condition =
                relBuilder.equals(
                        relBuilder.field("a"),
                        rexBuilder.makeBigintLiteral(new java.math.BigDecimal(42)));
        LogicalFilter filter = (LogicalFilter) relBuilder.filter(condition).build();

        // Replace the child with a supported TableScan mock
        LogicalFilter filterWithScan =
                filter.copy(filter.getTraitSet(), mockTableScan, filter.getCondition());

        DetectionResult result = detector.detect(filterWithScan);

        assertTrue(result.isSupported());
        assertEquals(Support.SUPPORTED, result.getSupport());
    }

    @Test
    @DisplayName("Custom/unrecognized RelNode should be unsupported")
    void customRelNodeIsUnsupported() {
        // A plain mock RelNode is not an instance of any supported type
        DetectionResult result = detector.detect(mockInput);

        assertFalse(result.isSupported());
        assertEquals(Support.UNSUPPORTED, result.getSupport());
        assertNotNull(result.getReason());
        assertTrue(result.getReason().contains("Unsupported operator"));
    }

    @Test
    @DisplayName("Supported parent with unsupported child should be unsupported")
    void unsupportedChildCausesUnsupported() {
        // mockInput is an unsupported type; placing it as a child of a supported node
        when(mockTableScan.getInputs()).thenReturn(List.of(mockInput));

        DetectionResult result = detector.detect(mockTableScan);

        assertFalse(result.isSupported());
        assertEquals(Support.UNSUPPORTED, result.getSupport());
        assertNotNull(result.getReason());
    }

    @Test
    @DisplayName("DetectionResult.supported() creates a supported result")
    void supportedFactoryMethod() {
        DetectionResult result = DetectionResult.supported();

        assertTrue(result.isSupported());
        assertEquals(Support.SUPPORTED, result.getSupport());
        assertNull(result.getReason());
    }

    @Test
    @DisplayName("DetectionResult.unsupported() creates an unsupported result with reason")
    void unsupportedFactoryMethod() {
        DetectionResult result = DetectionResult.unsupported("test reason");

        assertFalse(result.isSupported());
        assertEquals(Support.UNSUPPORTED, result.getSupport());
        assertEquals("test reason", result.getReason());
    }
}
