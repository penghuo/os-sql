/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.omni.connector.opensearch.decoder;

import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.RowBlockBuilder;

import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public final class RowFieldDecoder implements FieldDecoder
{
    private final List<String> fieldNames;
    private final List<FieldDecoder> fieldDecoders;

    public RowFieldDecoder(List<String> fieldNames, List<FieldDecoder> fieldDecoders)
    {
        this.fieldNames = requireNonNull(fieldNames);
        this.fieldDecoders = requireNonNull(fieldDecoders);
    }

    @Override
    public void decode(Object value, BlockBuilder output)
    {
        if (value == null) {
            output.appendNull();
            return;
        }
        @SuppressWarnings("unchecked")
        Map<String, Object> map = (Map<String, Object>) value;
        ((RowBlockBuilder) output).buildEntry(fieldBuilders -> {
            for (int i = 0; i < fieldNames.size(); i++) {
                Object fieldValue = map.get(fieldNames.get(i));
                fieldDecoders.get(i).decode(fieldValue, fieldBuilders.get(i));
            }
        });
    }
}
