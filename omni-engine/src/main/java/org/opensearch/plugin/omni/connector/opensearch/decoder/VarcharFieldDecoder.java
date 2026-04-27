/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.omni.connector.opensearch.decoder;

import io.airlift.slice.Slices;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.VarcharType;

public final class VarcharFieldDecoder implements FieldDecoder
{
    public static final VarcharFieldDecoder INSTANCE = new VarcharFieldDecoder();

    @Override
    public void decode(Object value, BlockBuilder output)
    {
        if (value == null) {
            output.appendNull();
            return;
        }
        VarcharType.VARCHAR.writeSlice(output, Slices.utf8Slice(value.toString()));
    }
}
