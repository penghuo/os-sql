/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.omni.connector.opensearch;

import io.trino.spi.connector.*;
import io.trino.spi.transaction.IsolationLevel;

import static java.util.Objects.requireNonNull;

public class OpenSearchConnector
        implements Connector
{
    private final OpenSearchMetadata metadata;
    private final OpenSearchSplitManager splitManager;
    private final OpenSearchPageSourceProvider pageSourceProvider;

    public OpenSearchConnector(
            OpenSearchMetadata metadata,
            OpenSearchSplitManager splitManager,
            OpenSearchPageSourceProvider pageSourceProvider)
    {
        this.metadata = requireNonNull(metadata);
        this.splitManager = requireNonNull(splitManager);
        this.pageSourceProvider = requireNonNull(pageSourceProvider);
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(
            IsolationLevel isolationLevel, boolean readOnly, boolean autoCommit)
    {
        return OpenSearchTransactionHandle.INSTANCE;
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorSession session, ConnectorTransactionHandle transactionHandle)
    {
        return metadata;
    }

    @Override
    public ConnectorSplitManager getSplitManager()
    {
        return splitManager;
    }

    @Override
    public ConnectorPageSourceProvider getPageSourceProvider()
    {
        return pageSourceProvider;
    }
}
