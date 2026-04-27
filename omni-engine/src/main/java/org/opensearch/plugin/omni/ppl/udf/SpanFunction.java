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

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;

import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;

/**
 * PPL SPAN function — buckets a timestamp into time intervals.
 * Transpiled SQL: SPAN(timestamp_col, interval, unit)
 * Units: 's' (second), 'm' (minute), 'h' (hour), 'd' (day), 'w' (week), 'M' (month), 'y' (year)
 */
public final class SpanFunction
{
    // Trino timestamp(3) epoch is 2000-01-01T00:00:00Z, stored as millis
    private static final long EPOCH_2000_MILLIS = 946684800000L;

    private SpanFunction() {}

    @ScalarFunction("span")
    @Description("Buckets a varchar timestamp into time intervals")
    @SqlNullable
    @SqlType(StandardTypes.VARCHAR)
    public static Slice spanVarchar(
            @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice value,
            @SqlType(StandardTypes.BIGINT) long interval,
            @SqlType(StandardTypes.VARCHAR) Slice unit)
    {
        if (value == null) {
            return null;
        }
        String ts = value.toStringUtf8();
        String u = unit.toStringUtf8();
        LocalDateTime dt = LocalDateTime.parse(ts, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        LocalDateTime truncated = truncate(dt, interval, u);
        return Slices.utf8Slice(truncated.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
    }

    @ScalarFunction("span")
    @Description("Buckets a timestamp into time intervals")
    @SqlType("timestamp(3)")
    public static long spanTimestamp(
            @SqlType("timestamp(3)") long epochMillis,
            @SqlType(StandardTypes.INTEGER) long interval,
            @SqlType(StandardTypes.VARCHAR) Slice unit)
    {
        // Convert Trino timestamp(3) (millis since 2000-01-01) to Java epoch millis
        long javaEpochMillis = epochMillis + EPOCH_2000_MILLIS;
        LocalDateTime dt = LocalDateTime.ofInstant(Instant.ofEpochMilli(javaEpochMillis), ZoneOffset.UTC);
        String u = unit.toStringUtf8();
        LocalDateTime truncated = truncate(dt, interval, u);
        long resultMillis = truncated.toInstant(ZoneOffset.UTC).toEpochMilli();
        return resultMillis - EPOCH_2000_MILLIS;
    }

    @ScalarFunction("span")
    @Description("Buckets a numeric value into intervals")
    @SqlNullable
    @SqlType(StandardTypes.DOUBLE)
    public static Double spanNumeric(
            @SqlNullable @SqlType(StandardTypes.DOUBLE) Double value,
            @SqlType(StandardTypes.DOUBLE) double interval)
    {
        if (value == null) {
            return null;
        }
        return Math.floor(value / interval) * interval;
    }

    private static LocalDateTime truncate(LocalDateTime dt, long interval, String unit)
    {
        return switch (unit.toLowerCase()) {
            case "s" -> dt.truncatedTo(ChronoUnit.SECONDS)
                    .withSecond((int) (dt.getSecond() / interval * interval));
            case "m" -> dt.truncatedTo(ChronoUnit.MINUTES)
                    .withMinute((int) (dt.getMinute() / interval * interval));
            case "h" -> dt.truncatedTo(ChronoUnit.HOURS)
                    .withHour((int) (dt.getHour() / interval * interval));
            case "d" -> {
                long epochDay = dt.toLocalDate().toEpochDay();
                long truncatedDay = (epochDay / interval) * interval;
                yield LocalDateTime.of(java.time.LocalDate.ofEpochDay(truncatedDay), java.time.LocalTime.MIDNIGHT);
            }
            case "w" -> {
                long epochDay = dt.toLocalDate().toEpochDay();
                long truncatedDay = (epochDay / (interval * 7)) * (interval * 7);
                yield LocalDateTime.of(java.time.LocalDate.ofEpochDay(truncatedDay), java.time.LocalTime.MIDNIGHT);
            }
            case "M" -> dt.withDayOfMonth(1).truncatedTo(ChronoUnit.DAYS)
                    .withMonth(((dt.getMonthValue() - 1) / (int) interval * (int) interval) + 1);
            case "y" -> dt.withDayOfYear(1).truncatedTo(ChronoUnit.DAYS)
                    .withYear((dt.getYear() / (int) interval) * (int) interval);
            default -> throw new IllegalArgumentException("Unsupported span unit: " + unit);
        };
    }
}
