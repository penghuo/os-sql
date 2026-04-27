/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.omni.ppl.udf;

import io.airlift.slice.Slice;
import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlNullable;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

/**
 * PPL UNIX_TIMESTAMP function — converts a datetime to Unix epoch seconds.
 * Transpiled SQL: UNIX_TIMESTAMP(datetime_col)
 */
public final class UnixTimestampFunction
{
    private static final DateTimeFormatter DATETIME_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    private UnixTimestampFunction() {}

    @ScalarFunction("unix_timestamp")
    @Description("Converts a datetime string to Unix epoch seconds")
    @SqlNullable
    @SqlType(StandardTypes.DOUBLE)
    public static Double unixTimestamp(@SqlNullable @SqlType(StandardTypes.VARCHAR) Slice value)
    {
        if (value == null) {
            return null;
        }
        String s = value.toStringUtf8();
        try {
            LocalDateTime dt = LocalDateTime.parse(s, DATETIME_FORMAT);
            return (double) dt.toEpochSecond(ZoneOffset.UTC);
        }
        catch (DateTimeParseException e) {
            try {
                LocalDate d = LocalDate.parse(s, DATE_FORMAT);
                return (double) d.atStartOfDay().toEpochSecond(ZoneOffset.UTC);
            }
            catch (DateTimeParseException e2) {
                return null;
            }
        }
    }

    @ScalarFunction("unix_timestamp")
    @Description("Returns a numeric value as-is (already epoch seconds)")
    @SqlType(StandardTypes.DOUBLE)
    public static double unixTimestampFromDouble(@SqlType(StandardTypes.DOUBLE) double value)
    {
        return value;
    }

    @ScalarFunction("unix_timestamp")
    @Description("Returns a bigint value as epoch seconds")
    @SqlType(StandardTypes.DOUBLE)
    public static double unixTimestampFromLong(@SqlType(StandardTypes.BIGINT) long value)
    {
        return (double) value;
    }

    @ScalarFunction("unix_timestamp")
    @Description("Converts a timestamp to Unix epoch seconds")
    @SqlType(StandardTypes.DOUBLE)
    public static double unixTimestampFromTimestamp(@SqlType("timestamp(3)") long epochMicros)
    {
        // Trino timestamp(3) is micros since 2000-01-01T00:00:00Z
        // Convert to Unix epoch seconds
        long EPOCH_2000_MICROS = 946684800000000L;
        return (double) ((epochMicros + EPOCH_2000_MICROS) / 1_000_000L);
    }
}
