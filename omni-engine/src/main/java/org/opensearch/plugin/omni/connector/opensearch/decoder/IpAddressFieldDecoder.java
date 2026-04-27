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

public final class IpAddressFieldDecoder implements FieldDecoder
{
    public static final IpAddressFieldDecoder INSTANCE = new IpAddressFieldDecoder();

    @Override
    public void decode(Object value, BlockBuilder output)
    {
        if (value == null) {
            output.appendNull();
            return;
        }
        // Store IP as VARCHAR (same as Trino ES connector for non-pushdown types)
        VarcharType.VARCHAR.writeSlice(output, Slices.utf8Slice(value.toString()));
    }
}
