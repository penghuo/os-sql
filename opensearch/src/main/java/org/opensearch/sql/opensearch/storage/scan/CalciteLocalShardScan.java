/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.scan;

import java.util.List;
import java.util.Map;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.adapter.enumerable.PhysTypeImpl;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.tree.Blocks;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.type.RelDataType;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.search.SearchHit;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.sql.calcite.plan.Scannable;
import org.opensearch.transport.client.node.NodeClient;

/**
 * Shard-local scan operator for distributed query execution.
 *
 * <p>This operator runs on data nodes and targets a specific shard using the {@code
 * _shards:N|_local} preference parameter. It is reconstructed from a deserialized plan fragment by
 * ShardCalciteRuntime, not constructed via optimizer rules.
 *
 * <p>Unlike {@link CalciteEnumerableIndexScan}, this operator does not require a {@link
 * org.apache.calcite.plan.RelOptTable} or {@link
 * org.opensearch.sql.opensearch.storage.scan.context.PushDownContext}, as Calcite handles operators
 * above the scan in the shard fragment.
 */
public class CalciteLocalShardScan extends AbstractRelNode
    implements Scannable, EnumerableRel {

    /** Default max number of hits to fetch per search request. */
    static final int DEFAULT_FETCH_SIZE = 10_000;

    private final RelDataType schema;
    private final int shardId;
    private final String indexName;
    private final @Nullable QueryBuilder luceneQuery;
    private final NodeClient client;

    /**
     * Creates a CalciteLocalShardScan.
     *
     * @param cluster Calcite cluster
     * @param traitSet trait set (should include {@link
     *     org.apache.calcite.adapter.enumerable.EnumerableConvention})
     * @param rowType the row type describing the output schema
     * @param shardId the target shard ordinal
     * @param indexName the OpenSearch index to scan
     * @param luceneQuery optional Lucene query from PredicateAnalyzer (may be null)
     * @param client the node-local client for executing search requests
     */
    public CalciteLocalShardScan(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelDataType rowType,
        int shardId,
        String indexName,
        @Nullable QueryBuilder luceneQuery,
        NodeClient client) {
        super(cluster, traitSet);
        this.schema = rowType;
        this.shardId = shardId;
        this.indexName = indexName;
        this.luceneQuery = luceneQuery;
        this.client = client;
    }

    @Override
    protected RelDataType deriveRowType() {
        return schema;
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw)
            .item("shardId", shardId)
            .item("index", indexName)
            .itemIf("query", luceneQuery, luceneQuery != null);
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        assert inputs.isEmpty();
        return new CalciteLocalShardScan(
            getCluster(), traitSet, schema, shardId, indexName, luceneQuery, client);
    }

    @Override
    public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
        PhysType physType =
            PhysTypeImpl.of(
                implementor.getTypeFactory(), getRowType(), pref.preferArray());
        Expression scanOperator =
            implementor.stash(this, CalciteLocalShardScan.class);
        return implementor.result(
            physType, Blocks.toBlock(Expressions.call(scanOperator, "scan")));
    }

    @Override
    public Enumerable<@Nullable Object> scan() {
        return new AbstractEnumerable<>() {
            @Override
            public Enumerator<Object> enumerator() {
                SearchRequest searchRequest = buildSearchRequest();
                SearchResponse response = client.search(searchRequest).actionGet();
                return new LocalShardEnumerator(
                    response.getHits().getHits(), getRowType().getFieldNames());
            }
        };
    }

    SearchRequest buildSearchRequest() {
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.size(DEFAULT_FETCH_SIZE);
        if (luceneQuery != null) {
            sourceBuilder.query(luceneQuery);
        }
        SearchRequest request = new SearchRequest(indexName);
        request.source(sourceBuilder);
        request.preference("_shards:" + shardId + "|_local");
        return request;
    }

    public int getShardId() {
        return shardId;
    }

    public String getIndexName() {
        return indexName;
    }

    public @Nullable QueryBuilder getLuceneQuery() {
        return luceneQuery;
    }

    /** Enumerator that iterates over SearchHit results from a shard-local search. */
    static class LocalShardEnumerator implements Enumerator<Object> {
        private final SearchHit[] hits;
        private final List<String> fieldNames;
        private int position = -1;

        LocalShardEnumerator(SearchHit[] hits, List<String> fieldNames) {
            this.hits = hits;
            this.fieldNames = fieldNames;
        }

        @Override
        public Object current() {
            SearchHit hit = hits[position];
            Map<String, Object> source = hit.getSourceAsMap();
            if (fieldNames.size() == 1) {
                return extractValue(source, fieldNames.get(0));
            }
            return fieldNames.stream().map(field -> extractValue(source, field)).toArray();
        }

        /**
         * Extracts a value from the source map, supporting dot-notation for nested fields.
         */
        @SuppressWarnings("unchecked")
        static @Nullable Object extractValue(Map<String, Object> source, String fieldName) {
            // Try direct lookup first
            if (source.containsKey(fieldName)) {
                return source.get(fieldName);
            }
            // Support dot-separated nested field paths
            String[] parts = fieldName.split("\\.");
            Object current = source;
            for (String part : parts) {
                if (current instanceof Map) {
                    current = ((Map<String, Object>) current).get(part);
                } else {
                    return null;
                }
            }
            return current;
        }

        @Override
        public boolean moveNext() {
            position++;
            return position < hits.length;
        }

        @Override
        public void reset() {
            position = -1;
        }

        @Override
        public void close() {
            // No resources to clean up for a simple search response
        }
    }
}
