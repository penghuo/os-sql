/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.omni.connector.opensearch.decoder;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.airlift.slice.Slices;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.VarcharType;

public final class RawJsonFieldDecoder implements FieldDecoder
{
    public static final RawJsonFieldDecoder INSTANCE = new RawJsonFieldDecoder();
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public void decode(Object value, BlockBuilder output)
    {
        if (value == null) {
            output.appendNull();
            return;
        }
        try {
            String json = MAPPER.writeValueAsString(value);
            VarcharType.VARCHAR.writeSlice(output, Slices.utf8Slice(json));
        } catch (JsonProcessingException e) {
            output.appendNull();
        }
    }
}
