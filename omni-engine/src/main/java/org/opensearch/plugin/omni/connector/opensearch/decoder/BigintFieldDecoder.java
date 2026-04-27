/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.omni.connector.opensearch.decoder;

import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.BigintType;

public final class BigintFieldDecoder implements FieldDecoder
{
    public static final BigintFieldDecoder INSTANCE = new BigintFieldDecoder();

    @Override
    public void decode(Object value, BlockBuilder output)
    {
        if (value == null) {
            output.appendNull();
            return;
        }
        if (value instanceof Number n) {
            BigintType.BIGINT.writeLong(output, n.longValue());
        } else {
            BigintType.BIGINT.writeLong(output, Long.parseLong(value.toString()));
        }
    }
}
