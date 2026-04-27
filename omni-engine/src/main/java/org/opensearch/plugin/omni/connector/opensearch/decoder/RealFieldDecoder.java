/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.omni.connector.opensearch.decoder;

import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.RealType;

public final class RealFieldDecoder implements FieldDecoder
{
    public static final RealFieldDecoder INSTANCE = new RealFieldDecoder();

    @Override
    public void decode(Object value, BlockBuilder output)
    {
        if (value == null) {
            output.appendNull();
            return;
        }
        if (value instanceof Number n) {
            RealType.REAL.writeLong(output, Float.floatToIntBits(n.floatValue()));
        } else {
            RealType.REAL.writeLong(output, Float.floatToIntBits(Float.parseFloat(value.toString())));
        }
    }
}
