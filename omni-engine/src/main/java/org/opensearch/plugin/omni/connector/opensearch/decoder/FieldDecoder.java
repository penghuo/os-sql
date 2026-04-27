/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.omni.connector.opensearch.decoder;

import io.trino.spi.block.BlockBuilder;

/**
 * Decodes a Java object value (from parsed _source JSON) into a Trino Block entry.
 * Implementations are stateless and thread-safe.
 */
public interface FieldDecoder
{
    /**
     * Decode a value and append it to the block builder.
     * If value is null, appends null to the builder.
     */
    void decode(Object value, BlockBuilder output);
}
