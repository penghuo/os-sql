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
import io.trino.spi.type.VarbinaryType;

import java.util.Base64;

public final class VarbinaryFieldDecoder implements FieldDecoder
{
    public static final VarbinaryFieldDecoder INSTANCE = new VarbinaryFieldDecoder();

    @Override
    public void decode(Object value, BlockBuilder output)
    {
        if (value == null) {
            output.appendNull();
            return;
        }
        byte[] bytes;
        if (value instanceof byte[] byteArray) {
            bytes = byteArray;
        } else {
            // Assume Base64 encoded string
            bytes = Base64.getDecoder().decode(value.toString());
        }
        VarbinaryType.VARBINARY.writeSlice(output, Slices.wrappedBuffer(bytes));
    }
}
