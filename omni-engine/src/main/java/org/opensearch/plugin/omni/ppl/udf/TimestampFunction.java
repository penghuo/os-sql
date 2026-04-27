/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.omni.ppl.udf;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlNullable;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

/**
 * PPL TIMESTAMP function — constructs a timestamp from a datetime value.
 * Transpiled SQL: TIMESTAMP(datetime_expr) or TIMESTAMP(datetime_expr, time_expr)
 */
public final class TimestampFunction
{
    private static final DateTimeFormatter DATETIME_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    private TimestampFunction() {}

    @ScalarFunction("timestamp")
    @Description("Constructs a timestamp from a datetime string")
    @SqlNullable
    @SqlType(StandardTypes.VARCHAR)
    public static Slice timestamp(@SqlNullable @SqlType(StandardTypes.VARCHAR) Slice value)
    {
        if (value == null) {
            return null;
        }
        String s = value.toStringUtf8();
        try {
            LocalDateTime dt = LocalDateTime.parse(s, DATETIME_FORMAT);
            return Slices.utf8Slice(dt.format(DATETIME_FORMAT));
        }
        catch (DateTimeParseException e) {
            try {
                LocalDate d = LocalDate.parse(s, DATE_FORMAT);
                return Slices.utf8Slice(d.atStartOfDay().format(DATETIME_FORMAT));
            }
            catch (DateTimeParseException e2) {
                return null;
            }
        }
    }
}
