/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.omni.connector.opensearch.decoder;

import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.DoubleType;

public final class DoubleFieldDecoder implements FieldDecoder
{
    public static final DoubleFieldDecoder INSTANCE = new DoubleFieldDecoder();

    @Override
    public void decode(Object value, BlockBuilder output)
    {
        if (value == null) {
            output.appendNull();
            return;
        }
        if (value instanceof Number n) {
            DoubleType.DOUBLE.writeDouble(output, n.doubleValue());
        } else {
            DoubleType.DOUBLE.writeDouble(output, Double.parseDouble(value.toString()));
        }
    }
}
