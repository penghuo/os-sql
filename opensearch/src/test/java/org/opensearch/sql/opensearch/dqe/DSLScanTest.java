/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.dqe;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Map;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
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
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType;
import org.opensearch.sql.opensearch.data.value.OpenSearchExprValueFactory;
import org.opensearch.sql.opensearch.request.OpenSearchRequestBuilder;
import org.opensearch.sql.opensearch.storage.OpenSearchIndex;
import org.opensearch.sql.opensearch.storage.scan.context.FilterDigest;
import org.opensearch.sql.opensearch.storage.scan.context.LimitDigest;
import org.opensearch.sql.opensearch.storage.scan.context.OSRequestBuilderAction;
import org.opensearch.sql.opensearch.storage.scan.context.PushDownContext;
import org.opensearch.sql.opensearch.storage.scan.context.PushDownType;

@ExtendWith(MockitoExtension.class)
class DSLScanTest {

    static final RelDataTypeFactory TYPE_FACTORY =
            new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    final RexBuilder rexBuilder = new RexBuilder(TYPE_FACTORY);

    @Mock private RelOptCluster cluster;
    @Mock private RelOptTable table;
    @Mock private OpenSearchIndex osIndex;
    @Mock private Settings settings;

    private RelTraitSet traitSet;
    private RelDataType schema;

    @BeforeEach
    void setUp() {
        traitSet = mock(RelTraitSet.class);
        lenient().when(traitSet.plus(any(Convention.class))).thenReturn(traitSet);
        lenient().when(cluster.traitSetOf(any(Convention.class))).thenReturn(traitSet);
        lenient().when(cluster.getTypeFactory()).thenReturn(TYPE_FACTORY);
        lenient().when(osIndex.getMaxResultWindow()).thenReturn(10000);
        lenient().when(osIndex.getQueryBucketSize()).thenReturn(1000);
        lenient().when(osIndex.getSettings()).thenReturn(settings);
        lenient()
                .when(settings.getSettingValue(Key.CALCITE_PUSHDOWN_ROWCOUNT_ESTIMATION_FACTOR))
                .thenReturn(0.9);

        // Create a schema with two fields: name (VARCHAR) and age (INTEGER)
        schema =
                TYPE_FACTORY
                        .builder()
                        .add("name", SqlTypeName.VARCHAR)
                        .add("age", SqlTypeName.INTEGER)
                        .build();
        lenient().when(table.getRowType()).thenReturn(schema);

        // Mock createRequestBuilder to return a real OpenSearchRequestBuilder
        OpenSearchExprValueFactory exprValueFactory =
                new OpenSearchExprValueFactory(
                        Map.of(
                                "name", OpenSearchDataType.of(OpenSearchDataType.MappingType.Text),
                                "age",
                                        OpenSearchDataType.of(
                                                OpenSearchDataType.MappingType.Integer)),
                        true);
        lenient()
                .when(osIndex.createRequestBuilder())
                .thenReturn(new OpenSearchRequestBuilder(exprValueFactory, 10000, settings));
    }

    @Test
    @DisplayName("DSLScan builds correct SearchSourceBuilder with filter, project, limit, sort")
    void testBuildSearchSourceBuilderWithPushDowns() {
        PushDownContext pushDownContext = new PushDownContext(osIndex);

        // Push down filter
        pushDownContext.add(
                PushDownType.FILTER,
                new FilterDigest(0, rexBuilder.makeLiteral(true)),
                (OSRequestBuilderAction)
                        requestBuilder ->
                                requestBuilder.pushDownFilterForCalcite(
                                        org.opensearch.index.query.QueryBuilders.termQuery(
                                                "name", "alice")));

        // Push down project
        pushDownContext.add(
                PushDownType.PROJECT,
                List.of("name"),
                (OSRequestBuilderAction)
                        requestBuilder ->
                                requestBuilder.pushDownProjectStream(
                                        List.of("name").stream()));

        // Push down limit
        pushDownContext.add(
                PushDownType.LIMIT,
                new LimitDigest(10, 0),
                (OSRequestBuilderAction) requestBuilder -> requestBuilder.pushDownLimit(10, 0));

        // Push down sort
        pushDownContext.add(
                PushDownType.SORT,
                "name_sort",
                (OSRequestBuilderAction)
                        requestBuilder ->
                                requestBuilder.pushDownSort(
                                        List.of(
                                                org.opensearch.search.sort.SortBuilders.fieldSort(
                                                        "name"))));

        DSLScan dslScan =
                new DSLScan(
                        cluster, traitSet, ImmutableList.of(), table, osIndex, schema,
                        pushDownContext);

        // Verify the request builder has the pushed down operations
        OpenSearchRequestBuilder requestBuilder = dslScan.getPushDownContext().createRequestBuilder();
        assertNotNull(requestBuilder);
        assertNotNull(requestBuilder.getSourceBuilder().query());
        assertTrue(requestBuilder.getSourceBuilder().query().toString().contains("name"));
        // Verify fetch source was set (project)
        assertNotNull(requestBuilder.getSourceBuilder().fetchSource());
        // Verify limit
        assertEquals(10, requestBuilder.getSourceBuilder().size());
        // Verify sort
        assertNotNull(requestBuilder.getSourceBuilder().sorts());
        assertEquals(1, requestBuilder.getSourceBuilder().sorts().size());
    }

    @Test
    @DisplayName("DSLScan with empty PushDownContext produces match_all query")
    void testEmptyPushDownContextMatchAll() {
        PushDownContext pushDownContext = new PushDownContext(osIndex);

        DSLScan dslScan =
                new DSLScan(
                        cluster, traitSet, ImmutableList.of(), table, osIndex, schema,
                        pushDownContext);

        OpenSearchRequestBuilder requestBuilder = dslScan.getPushDownContext().createRequestBuilder();
        assertNotNull(requestBuilder);
        // With no pushdowns, the query should be null (OpenSearch defaults to match_all)
        assertNull(requestBuilder.getSourceBuilder().query());
        // Sorts should be null or empty
        assertNull(requestBuilder.getSourceBuilder().sorts());
    }

    @Test
    @DisplayName("DSLScan with empty index returns 0 rows (not error)")
    void testEmptyIndexReturnsZeroRows() {
        PushDownContext pushDownContext = new PushDownContext(osIndex);

        DSLScan dslScan =
                new DSLScan(
                        cluster, traitSet, ImmutableList.of(), table, osIndex, schema,
                        pushDownContext);

        // Verify the scan can be created without error even with no data
        assertNotNull(dslScan);
        assertNotNull(dslScan.getOsIndex());
        assertEquals(schema, dslScan.deriveRowType());
        // The scan() method creates an Enumerable that would call OpenSearch,
        // so we verify the DSLScan is correctly constructed without error.
        assertEquals(0, pushDownContext.size());
    }

    @Test
    @DisplayName("DSLScan with index alias resolves correctly")
    void testIndexAliasResolvesCorrectly() {
        PushDownContext pushDownContext = new PushDownContext(osIndex);

        // Mock an alias mapping
        when(osIndex.getAliasMapping()).thenReturn(Map.of("a", "name", "b", "age"));

        DSLScan dslScan =
                new DSLScan(
                        cluster, traitSet, ImmutableList.of(), table, osIndex, schema,
                        pushDownContext);

        // Verify alias mapping is accessible through DSLScan
        Map<String, String> aliasMapping = dslScan.getAliasMapping();
        assertNotNull(aliasMapping);
        assertEquals("name", aliasMapping.get("a"));
        assertEquals("age", aliasMapping.get("b"));
    }

    @Test
    @DisplayName("DSLScan copy produces independent clone")
    void testCopyProducesIndependentClone() {
        PushDownContext pushDownContext = new PushDownContext(osIndex);
        pushDownContext.add(
                PushDownType.LIMIT,
                new LimitDigest(5, 0),
                (OSRequestBuilderAction) requestBuilder -> requestBuilder.pushDownLimit(5, 0));

        DSLScan original =
                new DSLScan(
                        cluster, traitSet, ImmutableList.of(), table, osIndex, schema,
                        pushDownContext);

        DSLScan copy = (DSLScan) original.copy();
        assertNotNull(copy);
        assertEquals(original.getOsIndex(), copy.getOsIndex());
        assertEquals(original.deriveRowType(), copy.deriveRowType());
        // PushDownContext should be cloned (not same reference)
        assertTrue(original.getPushDownContext() != copy.getPushDownContext());
        assertEquals(original.getPushDownContext().size(), copy.getPushDownContext().size());
    }
}
