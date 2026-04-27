/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.omni.connector.opensearch.decoder;

import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.SmallintType;

public final class SmallintFieldDecoder implements FieldDecoder
{
    public static final SmallintFieldDecoder INSTANCE = new SmallintFieldDecoder();

    @Override
    public void decode(Object value, BlockBuilder output)
    {
        if (value == null) {
            output.appendNull();
            return;
        }
        if (value instanceof Number n) {
            SmallintType.SMALLINT.writeLong(output, n.shortValue());
        } else {
            SmallintType.SMALLINT.writeLong(output, Short.parseShort(value.toString()));
        }
    }
}
