/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.omni.connector.opensearch;

import io.trino.spi.connector.*;
import org.apache.lucene.search.Query;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.index.IndexService;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;
import org.opensearch.plugin.omni.connector.opensearch.decoder.FieldDecoder;

import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class OpenSearchPageSourceProvider
        implements ConnectorPageSourceProvider
{
    private final Supplier<IndicesService> indicesServiceSupplier;
    private final ClusterService clusterService;

    public OpenSearchPageSourceProvider(Supplier<IndicesService> indicesServiceSupplier, ClusterService clusterService)
    {
        this.indicesServiceSupplier = requireNonNull(indicesServiceSupplier);
        this.clusterService = requireNonNull(clusterService);
    }

    @Override
    public ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableHandle table,
            List<ColumnHandle> columns,
            DynamicFilter dynamicFilter)
    {
        OpenSearchSplit osSplit = (OpenSearchSplit) split;
        OpenSearchTableHandle osTable = (OpenSearchTableHandle) table;

        // P1-5: Null guards before dereference chain
        IndicesService indicesService = indicesServiceSupplier.get();
        if (indicesService == null) {
            throw new RuntimeException("OpenSearch connector not yet initialized - IndicesService unavailable");
        }

        IndexMetadata indexMeta = clusterService.state().metadata().index(osSplit.getIndex());
        if (indexMeta == null) {
            throw new RuntimeException("Index [" + osSplit.getIndex() + "] not found in cluster state");
        }

        IndexService indexService = indicesService.indexService(indexMeta.getIndex());
        if (indexService == null) {
            throw new RuntimeException("IndexService not available for index [" + osSplit.getIndex() + "] on this node");
        }

        IndexShard shard = indexService.getShard(osSplit.getShard());

        // P0-3: Acquire searcher with try-finally to prevent leak on exception
        Engine.Searcher searcher = shard.acquireSearcher("omni");
        try {
            // Reconstruct column handles and decoders from ClusterState
            List<OpenSearchColumnHandle> osColumns = columns.stream()
                    .map(c -> (OpenSearchColumnHandle) c)
                    .collect(Collectors.toList());

            Map<String, Object> meta = getMeta(osSplit.getIndex());
            List<FieldDecoder> decoders = OpenSearchTypeMapper.createDecoders(osColumns, meta);

            // Build Lucene query from pushed-down predicates
            Query query = LuceneQueryBuilder.build(osTable.getConstraint(), osTable.getLikePatterns(), osTable.getQueryStringExpression());

            OpenSearchPageSource pageSource = new OpenSearchPageSource(
                    searcher, osColumns, decoders, query, osSplit, osTable.getLimit());
            searcher = null; // ownership transferred to pageSource
            return pageSource;
        } finally {
            if (searcher != null) {
                searcher.close(); // cleanup on exception
            }
        }
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> getMeta(String indexName)
    {
        MappingMetadata mapping = clusterService.state().metadata().index(indexName).mapping();
        if (mapping == null) return Map.of();
        Map<String, Object> source = mapping.sourceAsMap();
        return (Map<String, Object>) source.getOrDefault("_meta", Map.of());
    }
}
