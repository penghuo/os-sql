/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.dqe;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.common.setting.Settings.Key;
import org.opensearch.sql.opensearch.storage.OpenSearchIndex;
import org.opensearch.sql.opensearch.storage.scan.CalciteLogicalIndexScan;

@ExtendWith(MockitoExtension.class)
class DSLScanRuleTest {

    static final RelDataTypeFactory TYPE_FACTORY =
            new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);

    @Mock private RelOptCluster cluster;
    @Mock private RelOptTable table;
    @Mock private OpenSearchIndex osIndex;
    @Mock private Settings settings;

    private RelTraitSet logicalTraitSet;
    private RelDataType schema;

    @BeforeEach
    void setUp() {
        logicalTraitSet = mock(RelTraitSet.class);
        RelTraitSet enumerableTraitSet = mock(RelTraitSet.class);
        lenient().when(cluster.traitSetOf(Convention.NONE)).thenReturn(logicalTraitSet);
        lenient().when(logicalTraitSet.plus(EnumerableConvention.INSTANCE))
                .thenReturn(enumerableTraitSet);
        lenient().when(cluster.getTypeFactory()).thenReturn(TYPE_FACTORY);
        lenient().when(osIndex.getMaxResultWindow()).thenReturn(10000);
        lenient().when(osIndex.getSettings()).thenReturn(settings);
        lenient()
                .when(settings.getSettingValue(Key.CALCITE_PUSHDOWN_ROWCOUNT_ESTIMATION_FACTOR))
                .thenReturn(0.9);

        schema =
                TYPE_FACTORY
                        .builder()
                        .add("name", SqlTypeName.VARCHAR)
                        .add("age", SqlTypeName.INTEGER)
                        .build();
        lenient().when(table.getRowType()).thenReturn(schema);
        lenient().when(table.unwrap(any())).thenReturn(null);
    }

    @Test
    @DisplayName("DSLScanRule matches CalciteLogicalIndexScan and produces DSLScan")
    void testRuleMatchesAndConverts() {
        DSLScanRule rule = (DSLScanRule) DSLScanRule.DEFAULT_CONFIG.toRule();
        assertNotNull(rule);

        CalciteLogicalIndexScan logicalScan =
                new CalciteLogicalIndexScan(cluster, table, osIndex);

        // Verify convert produces DSLScan
        RelNode result = rule.convert(logicalScan);
        assertInstanceOf(DSLScan.class, result);

        DSLScan dslScan = (DSLScan) result;
        assertEquals(osIndex, dslScan.getOsIndex());
        assertEquals(logicalScan.getSchema(), dslScan.getSchema());
        assertEquals(logicalScan.getHints(), dslScan.getHints());
    }

    @Test
    @DisplayName("DSLScanRule matches when variables set is empty")
    void testRuleMatchesEmptyVariablesSet() {
        DSLScanRule rule = (DSLScanRule) DSLScanRule.DEFAULT_CONFIG.toRule();

        CalciteLogicalIndexScan logicalScan =
                new CalciteLogicalIndexScan(cluster, table, osIndex);

        // Create a mock RelOptRuleCall
        RelOptRuleCall call = mock(RelOptRuleCall.class);
        when(call.rel(0)).thenReturn(logicalScan);

        // The scan has empty variables set, so rule should match
        assertTrue(rule.matches(call));
    }

    @Test
    @DisplayName("DSLScanRule preserves PushDownContext from source scan")
    void testRulePreservesPushDownContext() {
        DSLScanRule rule = (DSLScanRule) DSLScanRule.DEFAULT_CONFIG.toRule();

        CalciteLogicalIndexScan logicalScan =
                new CalciteLogicalIndexScan(cluster, table, osIndex);

        RelNode result = rule.convert(logicalScan);
        DSLScan dslScan = (DSLScan) result;

        // PushDownContext should be passed through from the logical scan
        assertNotNull(dslScan.getPushDownContext());
        assertEquals(
                logicalScan.getPushDownContext().size(),
                dslScan.getPushDownContext().size());
    }
}
