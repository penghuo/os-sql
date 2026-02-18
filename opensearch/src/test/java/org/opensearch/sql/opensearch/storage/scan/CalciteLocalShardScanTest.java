/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.scan;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
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
import org.opensearch.action.search.SearchRequest;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.SearchHit;
import org.opensearch.transport.client.node.NodeClient;

@ExtendWith(MockitoExtension.class)
class CalciteLocalShardScanTest {

    private static final RelDataTypeFactory TYPE_FACTORY =
        new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);

    @Mock private RelOptCluster cluster;
    @Mock private NodeClient client;

    private RelTraitSet traitSet;
    private RelDataType rowType;

    @BeforeEach
    void setUp() {
        traitSet = mock(RelTraitSet.class);
        rowType =
            TYPE_FACTORY.builder()
                .add("name", SqlTypeName.VARCHAR)
                .add("age", SqlTypeName.INTEGER)
                .build();
    }

    @Test
    @DisplayName("Construction with proper parameters stores shard ID, index, and row type")
    void testConstruction() {
        CalciteLocalShardScan scan =
            new CalciteLocalShardScan(
                cluster, traitSet, rowType, 0, "test_index", null, client);

        assertEquals(0, scan.getShardId());
        assertEquals("test_index", scan.getIndexName());
        assertNull(scan.getLuceneQuery());
        assertEquals(rowType, scan.getRowType());
    }

    @Test
    @DisplayName("Construction with Lucene query preserves the query builder")
    void testConstructionWithLuceneQuery() {
        var query = QueryBuilders.termQuery("name", "test");
        CalciteLocalShardScan scan =
            new CalciteLocalShardScan(
                cluster, traitSet, rowType, 2, "my_index", query, client);

        assertEquals(2, scan.getShardId());
        assertEquals("my_index", scan.getIndexName());
        assertNotNull(scan.getLuceneQuery());
        assertEquals(query, scan.getLuceneQuery());
    }

    @Test
    @DisplayName("copy() produces equivalent node with same properties")
    void testCopyProducesEquivalentNode() {
        var query = QueryBuilders.matchAllQuery();
        CalciteLocalShardScan original =
            new CalciteLocalShardScan(
                cluster, traitSet, rowType, 1, "test_index", query, client);

        RelNode copied = original.copy(traitSet, List.of());

        assertInstanceOf(CalciteLocalShardScan.class, copied);
        CalciteLocalShardScan copiedScan = (CalciteLocalShardScan) copied;
        assertEquals(original.getShardId(), copiedScan.getShardId());
        assertEquals(original.getIndexName(), copiedScan.getIndexName());
        assertEquals(original.getLuceneQuery(), copiedScan.getLuceneQuery());
        assertEquals(original.getRowType(), copiedScan.getRowType());
    }

    @Test
    @DisplayName("explainTerms includes shard ID and index name")
    void testExplainTermsIncludesShardAndIndex() {
        CalciteLocalShardScan scan =
            new CalciteLocalShardScan(
                cluster, traitSet, rowType, 3, "explain_index", null, client);

        RelWriter writer = mock(RelWriter.class);
        when(writer.item(anyString(), any())).thenReturn(writer);
        when(writer.itemIf(anyString(), any(), anyBoolean())).thenReturn(writer);

        scan.explainTerms(writer);

        verify(writer).item("shardId", 3);
        verify(writer).item("index", "explain_index");
        verify(writer).itemIf(eq("query"), any(), eq(false));
    }

    @Test
    @DisplayName("explainTerms includes query when Lucene query is present")
    void testExplainTermsWithQuery() {
        var query = QueryBuilders.matchAllQuery();
        CalciteLocalShardScan scan =
            new CalciteLocalShardScan(
                cluster, traitSet, rowType, 1, "query_index", query, client);

        RelWriter writer = mock(RelWriter.class);
        when(writer.item(anyString(), any())).thenReturn(writer);
        when(writer.itemIf(anyString(), any(), anyBoolean())).thenReturn(writer);

        scan.explainTerms(writer);

        verify(writer).item("shardId", 1);
        verify(writer).item("index", "query_index");
        verify(writer).itemIf("query", query, true);
    }

    @Test
    @DisplayName("buildSearchRequest sets preference to target specific shard")
    void testBuildSearchRequestSetsPreference() {
        CalciteLocalShardScan scan =
            new CalciteLocalShardScan(
                cluster, traitSet, rowType, 5, "pref_index", null, client);

        SearchRequest request = scan.buildSearchRequest();

        assertEquals("_shards:5|_local", request.preference());
        assertArrayEquals(new String[] {"pref_index"}, request.indices());
        assertEquals(
            CalciteLocalShardScan.DEFAULT_FETCH_SIZE,
            request.source().size());
    }

    @Test
    @DisplayName("buildSearchRequest includes Lucene query when present")
    void testBuildSearchRequestWithLuceneQuery() {
        var query = QueryBuilders.termQuery("name", "alice");
        CalciteLocalShardScan scan =
            new CalciteLocalShardScan(
                cluster, traitSet, rowType, 0, "query_index", query, client);

        SearchRequest request = scan.buildSearchRequest();

        assertEquals(query, request.source().query());
    }

    @Test
    @DisplayName("buildSearchRequest has no query when luceneQuery is null")
    void testBuildSearchRequestWithoutLuceneQuery() {
        CalciteLocalShardScan scan =
            new CalciteLocalShardScan(
                cluster, traitSet, rowType, 0, "no_query_index", null, client);

        SearchRequest request = scan.buildSearchRequest();

        assertNull(request.source().query());
    }

    @Test
    @DisplayName("LocalShardEnumerator extracts flat fields from source map")
    void testEnumeratorExtractsValues() {
        SearchHit hit = mock(SearchHit.class);
        when(hit.getSourceAsMap()).thenReturn(Map.of("name", "alice", "age", 30));

        CalciteLocalShardScan.LocalShardEnumerator enumerator =
            new CalciteLocalShardScan.LocalShardEnumerator(
                new SearchHit[] {hit}, List.of("name", "age"));

        assertTrue(enumerator.moveNext());
        Object[] row = (Object[]) enumerator.current();
        assertEquals("alice", row[0]);
        assertEquals(30, row[1]);
        assertFalse(enumerator.moveNext());
    }

    @Test
    @DisplayName("LocalShardEnumerator returns scalar for single-column rows")
    void testEnumeratorSingleColumn() {
        SearchHit hit = mock(SearchHit.class);
        when(hit.getSourceAsMap()).thenReturn(Map.of("name", "bob"));

        CalciteLocalShardScan.LocalShardEnumerator enumerator =
            new CalciteLocalShardScan.LocalShardEnumerator(
                new SearchHit[] {hit}, List.of("name"));

        assertTrue(enumerator.moveNext());
        assertEquals("bob", enumerator.current());
    }

    @Test
    @DisplayName("LocalShardEnumerator supports dot-notation nested fields")
    void testEnumeratorNestedFields() {
        SearchHit hit = mock(SearchHit.class);
        when(hit.getSourceAsMap())
            .thenReturn(Map.of("address", Map.of("city", "Seattle")));

        CalciteLocalShardScan.LocalShardEnumerator enumerator =
            new CalciteLocalShardScan.LocalShardEnumerator(
                new SearchHit[] {hit}, List.of("address.city"));

        assertTrue(enumerator.moveNext());
        assertEquals("Seattle", enumerator.current());
    }

    @Test
    @DisplayName("LocalShardEnumerator returns null for missing fields")
    void testEnumeratorMissingField() {
        SearchHit hit = mock(SearchHit.class);
        when(hit.getSourceAsMap()).thenReturn(Map.of("name", "alice"));

        CalciteLocalShardScan.LocalShardEnumerator enumerator =
            new CalciteLocalShardScan.LocalShardEnumerator(
                new SearchHit[] {hit}, List.of("missing_field"));

        assertTrue(enumerator.moveNext());
        assertNull(enumerator.current());
    }

    @Test
    @DisplayName("LocalShardEnumerator reset rewinds to beginning")
    void testEnumeratorReset() {
        SearchHit hit = mock(SearchHit.class);
        when(hit.getSourceAsMap()).thenReturn(Map.of("name", "alice"));

        CalciteLocalShardScan.LocalShardEnumerator enumerator =
            new CalciteLocalShardScan.LocalShardEnumerator(
                new SearchHit[] {hit}, List.of("name"));

        assertTrue(enumerator.moveNext());
        assertEquals("alice", enumerator.current());
        assertFalse(enumerator.moveNext());

        enumerator.reset();
        assertTrue(enumerator.moveNext());
        assertEquals("alice", enumerator.current());
    }

    private static void assertTrue(boolean condition) {
        org.junit.jupiter.api.Assertions.assertTrue(condition);
    }

    private static void assertFalse(boolean condition) {
        org.junit.jupiter.api.Assertions.assertFalse(condition);
    }
}
