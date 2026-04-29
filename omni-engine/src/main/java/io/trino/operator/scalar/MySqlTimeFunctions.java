/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.operator.scalar;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.TrinoException;
import io.trino.spi.function.Description;
import io.trino.spi.function.LiteralParameters;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;

import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;

/**
 * MySQL-compatible time manipulation functions.
 */
public final class MySqlTimeFunctions
{
    private MySqlTimeFunctions() {}

    // ========== Helper methods for parsing VARCHAR to TIMESTAMP ==========

    /**
     * Parse a timestamp string in MySQL-compatible format.
     * Returns epoch microseconds.
     */
    private static long parseTimestampMicros(Slice s)
    {
        String str = s.toStringUtf8().trim();
        // Try "YYYY-MM-DD HH:MM:SS(.fff)" / "YYYY-MM-DDTHH:MM:SS"
        String normalized = str.length() > 10 && str.charAt(10) == 'T'
                ? str : str.replace(' ', 'T');
        if (normalized.length() == 10) {
            normalized = normalized + "T00:00:00";
        }
        try {
            LocalDateTime ldt = LocalDateTime.parse(normalized);
            return ldt.toEpochSecond(ZoneOffset.UTC) * 1_000_000L + ldt.getNano() / 1000;
        }
        catch (Exception e) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Cannot parse timestamp: " + str);
        }
    }

    // ========== addtime: add time duration to a timestamp ==========

    @Description("Add time to timestamp")
    @ScalarFunction("addtime")
    @LiteralParameters("p")
    @SqlType("timestamp(p)")
    public static long addtime(@SqlType("timestamp(p)") long timestamp, @SqlType(StandardTypes.VARCHAR) Slice timeStr)
    {
        // Parse time string (HH:MM:SS or HH:MM:SS.ffffff)
        long microsToAdd = parseTimeDuration(timeStr);
        return timestamp + microsToAdd;
    }

    // ========== subtime: subtract time duration from a timestamp ==========

    @Description("Subtract time from timestamp")
    @ScalarFunction("subtime")
    @LiteralParameters("p")
    @SqlType("timestamp(p)")
    public static long subtime(@SqlType("timestamp(p)") long timestamp, @SqlType(StandardTypes.VARCHAR) Slice timeStr)
    {
        long microsToSubtract = parseTimeDuration(timeStr);
        return timestamp - microsToSubtract;
    }

    private static long parseTimeDuration(Slice timeStr)
    {
        String str = timeStr.toStringUtf8().trim();
        String[] parts = str.split(":");
        if (parts.length < 2) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Invalid time format: " + str);
        }

        long hours = Long.parseLong(parts[0]);
        long minutes = Long.parseLong(parts[1]);
        double seconds = parts.length > 2 ? Double.parseDouble(parts[2]) : 0.0;

        return (hours * 3600 + minutes * 60) * 1_000_000L + (long) (seconds * 1_000_000);
    }

    // ========== timestampdiff: difference between two timestamps in specified unit ==========

    @Description("Difference between two timestamps")
    @ScalarFunction("timestampdiff")
    @LiteralParameters("p")
    @SqlType(StandardTypes.BIGINT)
    public static long timestampdiff(
            @SqlType(StandardTypes.VARCHAR) Slice unit,
            @SqlType("timestamp(p)") long timestamp1,
            @SqlType("timestamp(p)") long timestamp2)
    {
        String unitStr = unit.toStringUtf8().toLowerCase();
        long micros1 = timestamp1;
        long micros2 = timestamp2;
        long diffMicros = micros2 - micros1;

        return switch (unitStr) {
            case "microsecond" -> diffMicros;
            case "second" -> diffMicros / 1_000_000;
            case "minute" -> diffMicros / (60L * 1_000_000);
            case "hour" -> diffMicros / (3600L * 1_000_000);
            case "day" -> diffMicros / (86400L * 1_000_000);
            case "week" -> diffMicros / (7L * 86400L * 1_000_000);
            case "month" -> calculateMonthDiff(micros1, micros2);
            case "quarter" -> calculateMonthDiff(micros1, micros2) / 3;
            case "year" -> calculateMonthDiff(micros1, micros2) / 12;
            default -> throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Unknown unit: " + unitStr);
        };
    }

    @Description("Difference between two timestamps from VARCHAR")
    @ScalarFunction("timestampdiff")
    @SqlType(StandardTypes.BIGINT)
    public static long timestampdiffFromVarchar(
            @SqlType(StandardTypes.VARCHAR) Slice unit,
            @SqlType(StandardTypes.VARCHAR) Slice timestamp1Str,
            @SqlType(StandardTypes.VARCHAR) Slice timestamp2Str)
    {
        long timestamp1 = parseTimestampMicros(timestamp1Str);
        long timestamp2 = parseTimestampMicros(timestamp2Str);
        return timestampdiff(unit, timestamp1, timestamp2);
    }

    private static long calculateMonthDiff(long micros1, long micros2)
    {
        LocalDateTime dt1 = LocalDateTime.ofInstant(Instant.ofEpochMilli(micros1 / 1000), ZoneId.of("UTC"));
        LocalDateTime dt2 = LocalDateTime.ofInstant(Instant.ofEpochMilli(micros2 / 1000), ZoneId.of("UTC"));
        return ChronoUnit.MONTHS.between(dt1, dt2);
    }

    // ========== convert_tz: convert timestamp from one timezone to another ==========

    @Description("Convert timestamp from one timezone to another")
    @ScalarFunction("convert_tz")
    @LiteralParameters("p")
    @SqlType("timestamp(p)")
    public static long convertTz(
            @SqlType("timestamp(p)") long timestamp,
            @SqlType(StandardTypes.VARCHAR) Slice fromTz,
            @SqlType(StandardTypes.VARCHAR) Slice toTz)
    {
        try {
            String fromZone = fromTz.toStringUtf8();
            String toZone = toTz.toStringUtf8();

            // Convert micros to instant
            Instant instant = Instant.ofEpochMilli(timestamp / 1000);

            // Interpret as being in fromZone
            ZonedDateTime fromZdt = instant.atZone(ZoneId.of(fromZone));

            // Convert to toZone
            ZonedDateTime toZdt = fromZdt.withZoneSameLocal(ZoneId.of(toZone));

            // Return as micros
            return toZdt.toInstant().toEpochMilli() * 1000;
        }
        catch (Exception e) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Invalid timezone conversion: " + e.getMessage());
        }
    }

    @Description("Convert timestamp from VARCHAR from one timezone to another")
    @ScalarFunction("convert_tz")
    @SqlType(StandardTypes.TIMESTAMP)
    public static long convertTzFromVarchar(
            @SqlType(StandardTypes.VARCHAR) Slice timestampStr,
            @SqlType(StandardTypes.VARCHAR) Slice fromTz,
            @SqlType(StandardTypes.VARCHAR) Slice toTz)
    {
        long timestamp = parseTimestampMicros(timestampStr);
        return convertTz(timestamp, fromTz, toTz);
    }

    // ========== str_to_date: parse string to date/timestamp ==========

    @Description("Parse string to date using format")
    @ScalarFunction("str_to_date")
    @SqlType(StandardTypes.TIMESTAMP)
    public static long strToDate(@SqlType(StandardTypes.VARCHAR) Slice dateStr, @SqlType(StandardTypes.VARCHAR) Slice formatStr)
    {
        try {
            String str = dateStr.toStringUtf8();
            String format = formatStr.toStringUtf8();

            // Convert MySQL format to Java DateTimeFormatter
            String javaPattern = convertMySqlFormatToJava(format);
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern(javaPattern);

            // Try to parse as LocalDateTime first, fall back to LocalDate
            try {
                LocalDateTime dt = LocalDateTime.parse(str, formatter);
                return dt.atZone(ZoneId.of("UTC")).toInstant().toEpochMilli() * 1000;
            }
            catch (DateTimeParseException e) {
                // Try as date only
                java.time.LocalDate date = java.time.LocalDate.parse(str, formatter);
                return date.atStartOfDay(ZoneId.of("UTC")).toInstant().toEpochMilli() * 1000;
            }
        }
        catch (Exception e) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Invalid date format: " + e.getMessage());
        }
    }

    private static String convertMySqlFormatToJava(String mysqlFormat)
    {
        String result = mysqlFormat;
        result = result.replace("%Y", "yyyy");
        result = result.replace("%y", "yy");
        result = result.replace("%m", "MM");
        result = result.replace("%d", "dd");
        result = result.replace("%H", "HH");
        result = result.replace("%i", "mm");  // %i is minutes in MySQL
        result = result.replace("%s", "ss");  // %s is seconds
        result = result.replace("%f", "SSSSSS");  // %f is microseconds (6 digits)
        return result;
    }

    // ========== time: extract time part as varchar ==========

    @Description("Extract time part from timestamp")
    @ScalarFunction("time")
    @LiteralParameters("p")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice time(@SqlType("timestamp(p)") long timestamp)
    {
        LocalDateTime dt = LocalDateTime.ofInstant(
                Instant.ofEpochMilli(timestamp / 1000),
                ZoneId.of("UTC"));
        String timeStr = String.format("%02d:%02d:%02d",
                dt.getHour(), dt.getMinute(), dt.getSecond());
        return Slices.utf8Slice(timeStr);
    }

    @Description("Parse time from string")
    @ScalarFunction("time")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice timeFromString(@SqlType(StandardTypes.VARCHAR) Slice timeStr)
    {
        // For strings, just return as-is if valid format
        String str = timeStr.toStringUtf8().trim();
        if (str.matches("\\d{2}:\\d{2}:\\d{2}.*")) {
            return timeStr;
        }
        throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Invalid time format: " + str);
    }
}
