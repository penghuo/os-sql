/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.omni.connector.opensearch.decoder;

import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.TinyintType;

public final class TinyintFieldDecoder implements FieldDecoder
{
    public static final TinyintFieldDecoder INSTANCE = new TinyintFieldDecoder();

    @Override
    public void decode(Object value, BlockBuilder output)
    {
        if (value == null) {
            output.appendNull();
            return;
        }
        if (value instanceof Number n) {
            TinyintType.TINYINT.writeLong(output, n.byteValue());
        } else {
            TinyintType.TINYINT.writeLong(output, Byte.parseByte(value.toString()));
        }
    }
}
