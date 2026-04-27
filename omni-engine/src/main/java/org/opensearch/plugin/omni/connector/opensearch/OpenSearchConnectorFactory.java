/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.omni.connector.opensearch;

import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.indices.IndicesService;

import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

public class OpenSearchConnectorFactory
{
    private final Supplier<IndicesService> indicesServiceSupplier;
    private final ClusterService clusterService;

    public OpenSearchConnectorFactory(
            Supplier<IndicesService> indicesServiceSupplier,
            ClusterService clusterService)
    {
        this.indicesServiceSupplier = requireNonNull(indicesServiceSupplier);
        this.clusterService = requireNonNull(clusterService);
    }

    public Connector create(String catalogName, ConnectorContext context)
    {
        OpenSearchMetadata metadata = new OpenSearchMetadata(clusterService);
        OpenSearchSplitManager splitManager = new OpenSearchSplitManager(clusterService, indicesServiceSupplier);
        OpenSearchPageSourceProvider pageSourceProvider = new OpenSearchPageSourceProvider(
                indicesServiceSupplier, clusterService);
        return new OpenSearchConnector(metadata, splitManager, pageSourceProvider);
    }
}
