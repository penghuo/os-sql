/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.omni.connector.opensearch;

import com.fasterxml.jackson.annotation.JsonCreator;
import io.trino.spi.connector.ConnectorTransactionHandle;

public enum OpenSearchTransactionHandle
        implements ConnectorTransactionHandle
{
    INSTANCE;

    @JsonCreator
    public static OpenSearchTransactionHandle getInstance()
    {
        return INSTANCE;
    }
}
