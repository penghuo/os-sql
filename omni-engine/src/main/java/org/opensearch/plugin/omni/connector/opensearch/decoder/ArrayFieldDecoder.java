/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.omni.connector.opensearch.decoder;

import io.trino.spi.block.ArrayBlockBuilder;
import io.trino.spi.block.BlockBuilder;

import java.util.List;

import static java.util.Objects.requireNonNull;

public final class ArrayFieldDecoder implements FieldDecoder
{
    private final FieldDecoder elementDecoder;

    public ArrayFieldDecoder(FieldDecoder elementDecoder)
    {
        this.elementDecoder = requireNonNull(elementDecoder);
    }

    @Override
    public void decode(Object value, BlockBuilder output)
    {
        if (value == null) {
            output.appendNull();
            return;
        }
        ((ArrayBlockBuilder) output).buildEntry(elementBuilder -> {
            if (value instanceof List<?> list) {
                for (Object element : list) {
                    elementDecoder.decode(element, elementBuilder);
                }
            } else {
                // Single value — wrap as single-element array
                elementDecoder.decode(value, elementBuilder);
            }
        });
    }
}
