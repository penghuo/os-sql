/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.omni.connector.opensearch.decoder;

import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.TimestampType;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

public final class TimestampFieldDecoder implements FieldDecoder
{
    public static final TimestampFieldDecoder INSTANCE = new TimestampFieldDecoder();

    @Override
    public void decode(Object value, BlockBuilder output)
    {
        if (value == null) {
            output.appendNull();
            return;
        }
        long epochMicros;
        if (value instanceof Number number) {
            // Epoch millis from OpenSearch
            epochMicros = number.longValue() * 1000;
        } else {
            // ISO date string
            try {
                Instant instant = DateTimeFormatter.ISO_DATE_TIME.parse(value.toString(), Instant::from);
                epochMicros = instant.toEpochMilli() * 1000;
            } catch (DateTimeParseException e) {
                output.appendNull();
                return;
            }
        }
        TimestampType.TIMESTAMP_MILLIS.writeLong(output, epochMicros);
    }
}
