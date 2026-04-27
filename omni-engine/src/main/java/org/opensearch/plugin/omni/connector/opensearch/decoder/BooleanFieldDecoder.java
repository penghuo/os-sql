/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.omni.connector.opensearch.decoder;

import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.BooleanType;

public final class BooleanFieldDecoder implements FieldDecoder
{
    public static final BooleanFieldDecoder INSTANCE = new BooleanFieldDecoder();

    @Override
    public void decode(Object value, BlockBuilder output)
    {
        if (value == null) {
            output.appendNull();
            return;
        }
        BooleanType.BOOLEAN.writeBoolean(output, value instanceof Boolean b ? b : Boolean.parseBoolean(value.toString()));
    }
}
