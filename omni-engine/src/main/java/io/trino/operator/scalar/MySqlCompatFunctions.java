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
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.StandardTypes;
import org.joda.time.chrono.ISOChronology;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.type.DateTimes.scaleEpochMicrosToMillis;
import static java.util.concurrent.TimeUnit.DAYS;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.LocalDate;
import java.time.LocalTime;

import io.trino.spi.connector.ConnectorSession;

/**
 * MySQL-compatible wrappers over existing Trino datetime extraction functions.
 *
 * <p>These differ from Trino canonical functions in return type (INTEGER vs BIGINT)
 * and — for {@code dayofweek} — in numbering convention (Sunday=1..Saturday=7 vs
 * ISO Monday=1..Sunday=7).
 */
public final class MySqlCompatFunctions
{
    private static final ISOChronology UTC_CHRONOLOGY = ISOChronology.getInstanceUTC();

    private MySqlCompatFunctions() {}

    // ========== Helper methods for parsing VARCHAR to TIMESTAMP/DATE/TIME ==========

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

    // ========== dayofweek: MySQL Sunday=1..Saturday=7 ==========

    @Description("MySQL day of week (Sunday=1..Saturday=7)")
    @ScalarFunction("dayofweek")
    public static final class DayOfWeek
    {
        private DayOfWeek() {}

        @SqlType(StandardTypes.INTEGER)
        public static long dayOfWeekFromDate(@SqlType(StandardTypes.DATE) long date)
        {
            // ISO: Monday=1..Sunday=7. Convert to MySQL: Sunday=1..Saturday=7.
            long iso = UTC_CHRONOLOGY.dayOfWeek().get(DAYS.toMillis(date));
            return (iso % 7) + 1;
        }

        @LiteralParameters("p")
        @SqlType(StandardTypes.INTEGER)
        public static long dayOfWeekFromTimestamp(@SqlType("timestamp(p)") long timestamp)
        {
            long iso = UTC_CHRONOLOGY.dayOfWeek().get(scaleEpochMicrosToMillis(timestamp));
            return (iso % 7) + 1;
        }

        @LiteralParameters("p")
        @SqlType(StandardTypes.INTEGER)
        public static long dayOfWeekFromLongTimestamp(@SqlType("timestamp(p)") LongTimestamp timestamp)
        {
            return dayOfWeekFromTimestamp(timestamp.getEpochMicros());
        }

        @SqlType(StandardTypes.INTEGER)
        public static long dayOfWeekFromVarchar(@SqlType(StandardTypes.VARCHAR) Slice dateStr)
        {
            long timestamp = parseTimestampMicros(dateStr);
            return dayOfWeekFromTimestamp(timestamp);
        }
    }

    // ========== dayofmonth: just cast to INTEGER ==========

    @Description("MySQL day of month (1..31)")
    @ScalarFunction("dayofmonth")
    public static final class DayOfMonth
    {
        private DayOfMonth() {}

        @SqlType(StandardTypes.INTEGER)
        public static long dayOfMonthFromDate(@SqlType(StandardTypes.DATE) long date)
        {
            return UTC_CHRONOLOGY.dayOfMonth().get(DAYS.toMillis(date));
        }

        @LiteralParameters("p")
        @SqlType(StandardTypes.INTEGER)
        public static long dayOfMonthFromTimestamp(@SqlType("timestamp(p)") long timestamp)
        {
            return UTC_CHRONOLOGY.dayOfMonth().get(scaleEpochMicrosToMillis(timestamp));
        }

        @LiteralParameters("p")
        @SqlType(StandardTypes.INTEGER)
        public static long dayOfMonthFromLongTimestamp(@SqlType("timestamp(p)") LongTimestamp timestamp)
        {
            return dayOfMonthFromTimestamp(timestamp.getEpochMicros());
        }

        @SqlType(StandardTypes.INTEGER)
        public static long dayOfMonthFromVarchar(@SqlType(StandardTypes.VARCHAR) Slice dateStr)
        {
            long timestamp = parseTimestampMicros(dateStr);
            return dayOfMonthFromTimestamp(timestamp);
        }
    }

    // ========== dayofyear: just cast to INTEGER ==========

    @Description("MySQL day of year (1..366)")
    @ScalarFunction("dayofyear")
    public static final class DayOfYear
    {
        private DayOfYear() {}

        @SqlType(StandardTypes.INTEGER)
        public static long dayOfYearFromDate(@SqlType(StandardTypes.DATE) long date)
        {
            return UTC_CHRONOLOGY.dayOfYear().get(DAYS.toMillis(date));
        }

        @LiteralParameters("p")
        @SqlType(StandardTypes.INTEGER)
        public static long dayOfYearFromTimestamp(@SqlType("timestamp(p)") long timestamp)
        {
            return UTC_CHRONOLOGY.dayOfYear().get(scaleEpochMicrosToMillis(timestamp));
        }

        @LiteralParameters("p")
        @SqlType(StandardTypes.INTEGER)
        public static long dayOfYearFromLongTimestamp(@SqlType("timestamp(p)") LongTimestamp timestamp)
        {
            return dayOfYearFromTimestamp(timestamp.getEpochMicros());
        }

        @SqlType(StandardTypes.INTEGER)
        public static long dayOfYearFromVarchar(@SqlType(StandardTypes.VARCHAR) Slice dateStr)
        {
            long timestamp = parseTimestampMicros(dateStr);
            return dayOfYearFromTimestamp(timestamp);
        }
    }

    // ========== weekday: MySQL Monday=0..Sunday=6 ==========

    @Description("MySQL weekday (Monday=0..Sunday=6)")
    @ScalarFunction("weekday")
    public static final class Weekday
    {
        private Weekday() {}

        @SqlType(StandardTypes.INTEGER)
        public static long weekdayFromDate(@SqlType(StandardTypes.DATE) long date)
        {
            // ISO: Monday=1..Sunday=7. Convert to MySQL weekday: Monday=0..Sunday=6.
            long iso = UTC_CHRONOLOGY.dayOfWeek().get(DAYS.toMillis(date));
            return iso - 1;
        }

        @LiteralParameters("p")
        @SqlType(StandardTypes.INTEGER)
        public static long weekdayFromTimestamp(@SqlType("timestamp(p)") long timestamp)
        {
            long iso = UTC_CHRONOLOGY.dayOfWeek().get(scaleEpochMicrosToMillis(timestamp));
            return iso - 1;
        }

        @LiteralParameters("p")
        @SqlType(StandardTypes.INTEGER)
        public static long weekdayFromLongTimestamp(@SqlType("timestamp(p)") LongTimestamp timestamp)
        {
            return weekdayFromTimestamp(timestamp.getEpochMicros());
        }

        @SqlType(StandardTypes.INTEGER)
        public static long weekdayFromVarchar(@SqlType(StandardTypes.VARCHAR) Slice dateStr)
        {
            long timestamp = parseTimestampMicros(dateStr);
            return weekdayFromTimestamp(timestamp);
        }
    }

    // ========== minute_of_day: minute within the day (0..1439) ==========

    @Description("MySQL minute of day (0..1439)")
    @ScalarFunction("minute_of_day")
    public static final class MinuteOfDay
    {
        private MinuteOfDay() {}

        @LiteralParameters("p")
        @SqlType(StandardTypes.INTEGER)
        public static long minuteOfDayFromTimestamp(@SqlType("timestamp(p)") long timestamp)
        {
            long millis = scaleEpochMicrosToMillis(timestamp);
            long hour = UTC_CHRONOLOGY.hourOfDay().get(millis);
            long minute = UTC_CHRONOLOGY.minuteOfHour().get(millis);
            return hour * 60 + minute;
        }

        @LiteralParameters("p")
        @SqlType(StandardTypes.INTEGER)
        public static long minuteOfDayFromLongTimestamp(@SqlType("timestamp(p)") LongTimestamp timestamp)
        {
            return minuteOfDayFromTimestamp(timestamp.getEpochMicros());
        }

        @SqlType(StandardTypes.INTEGER)
        public static long minuteOfDayFromVarchar(@SqlType(StandardTypes.VARCHAR) Slice timestampStr)
        {
            long timestamp = parseTimestampMicros(timestampStr);
            return minuteOfDayFromTimestamp(timestamp);
        }
    }

    // ========== microsecond: microsecond part (0..999999) ==========

    @Description("MySQL microsecond (0..999999)")
    @ScalarFunction("microsecond")
    public static final class Microsecond
    {
        private Microsecond() {}

        @LiteralParameters("p")
        @SqlType(StandardTypes.INTEGER)
        public static long microsecondFromTimestamp(@SqlType("timestamp(p)") long timestamp)
        {
            // timestamp is in microseconds, extract the fractional part
            return timestamp % 1_000_000;
        }

        @LiteralParameters("p")
        @SqlType(StandardTypes.INTEGER)
        public static long microsecondFromLongTimestamp(@SqlType("timestamp(p)") LongTimestamp timestamp)
        {
            long micros = timestamp.getEpochMicros();
            int picosOfMicro = timestamp.getPicosOfMicro();
            // Convert picos to additional micros (though typically this is sub-microsecond)
            return (micros % 1_000_000 + picosOfMicro / 1_000_000) % 1_000_000;
        }

        @SqlType(StandardTypes.INTEGER)
        public static long microsecondFromVarchar(@SqlType(StandardTypes.VARCHAR) Slice timestampStr)
        {
            long timestamp = parseTimestampMicros(timestampStr);
            return microsecondFromTimestamp(timestamp);
        }
    }

    // ========== period_diff: difference between two periods (YYYYMM format) ==========

    @Description("MySQL period_diff: difference in months between two periods")
    @ScalarFunction("period_diff")
    @SqlType(StandardTypes.INTEGER)
    public static long periodDiff(@SqlType(StandardTypes.BIGINT) long period1, @SqlType(StandardTypes.BIGINT) long period2)
    {
        // Convert YYYYMM or YYMM to total months
        long months1 = periodToMonths(period1);
        long months2 = periodToMonths(period2);
        return months1 - months2;
    }

    private static long periodToMonths(long period)
    {
        if (period >= 100_0000) {
            // Format: YYYYMM (e.g., 202301)
            long year = period / 100;
            long month = period % 100;
            return year * 12 + month;
        }
        else {
            // Format: YYMM (e.g., 2301)
            long year = period / 100;
            long month = period % 100;
            // Assume 70-99 are 1970-1999, 00-69 are 2000-2069
            if (year >= 70) {
                year += 1900;
            }
            else {
                year += 2000;
            }
            return year * 12 + month;
        }
    }

    // ========== strftime: format unix timestamp ==========

    @Description("Format unix timestamp using strftime format string")
    @ScalarFunction("strftime")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice strftime(@SqlType(StandardTypes.BIGINT) long unixTimestamp, @SqlType(StandardTypes.VARCHAR) Slice formatSlice)
    {
        return formatTimestamp(unixTimestamp, 0, formatSlice);
    }

    @Description("Format unix timestamp (double) using strftime format string")
    @ScalarFunction("strftime")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice strftimeDouble(@SqlType(StandardTypes.DOUBLE) double unixTimestamp, @SqlType(StandardTypes.VARCHAR) Slice formatSlice)
    {
        long seconds = (long) unixTimestamp;
        int nanos = (int) ((unixTimestamp - seconds) * 1_000_000_000);
        return formatTimestamp(seconds, nanos, formatSlice);
    }

    @Description("Format timestamp using strftime format string")
    @ScalarFunction("strftime")
    @LiteralParameters("p")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice strftimeTimestamp(@SqlType("timestamp(p)") long timestamp, @SqlType(StandardTypes.VARCHAR) Slice formatSlice)
    {
        // timestamp is in microseconds since epoch, use proper scaling
        long millis = scaleEpochMicrosToMillis(timestamp);
        long seconds = millis / 1000;
        int nanos = (int) ((millis % 1000) * 1_000_000);
        return formatTimestamp(seconds, nanos, formatSlice);
    }

    @Description("Format timestamp with time zone using strftime format string")
    @ScalarFunction("strftime")
    @LiteralParameters("p")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice strftimeTimestampWithTz(@SqlType("timestamp(p) with time zone") long timestampWithTz, @SqlType(StandardTypes.VARCHAR) Slice formatSlice)
    {
        // timestamp with time zone is also in microseconds since epoch
        long millis = scaleEpochMicrosToMillis(timestampWithTz);
        long seconds = millis / 1000;
        int nanos = (int) ((millis % 1000) * 1_000_000);
        return formatTimestamp(seconds, nanos, formatSlice);
    }

    private static Slice formatTimestamp(long seconds, int nanos, Slice formatSlice)
    {
        String format = formatSlice.toStringUtf8();
        Instant instant = Instant.ofEpochSecond(seconds, nanos);
        ZonedDateTime zdt = ZonedDateTime.ofInstant(instant, ZoneId.of("UTC"));

        // Convert strftime format specifiers to Java DateTimeFormatter patterns
        String javaPattern = convertStrftimeToJavaPattern(format);

        try {
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern(javaPattern);
            String result = zdt.format(formatter);
            return Slices.utf8Slice(result);
        }
        catch (Exception e) {
            // Fallback to simple format
            return Slices.utf8Slice(zdt.toString());
        }
    }

    private static String convertStrftimeToJavaPattern(String strftimeFormat)
    {
        // Map common strftime format specifiers to Java DateTimeFormatter patterns
        // Process longer patterns first to avoid partial matches
        String result = strftimeFormat;

        // Handle multi-character patterns first
        result = result.replace("%3Q", "SSS");        // Milliseconds (3 digits)
        result = result.replace("%F", "yyyy-MM-dd");  // Date (YYYY-MM-DD)
        result = result.replace("%T", "HH:mm:ss");    // Time (HH:MM:SS)

        // Then single-character patterns
        result = result.replace("%Y", "yyyy");   // 4-digit year
        result = result.replace("%y", "yy");     // 2-digit year
        result = result.replace("%m", "MM");     // Month (01..12)
        result = result.replace("%d", "dd");     // Day of month (01..31)
        result = result.replace("%H", "HH");     // Hour (00..23)
        result = result.replace("%I", "hh");     // Hour (01..12)
        result = result.replace("%M", "mm");     // Minute (00..59)
        result = result.replace("%S", "ss");     // Second (00..59) - lowercase!
        result = result.replace("%p", "a");      // AM/PM
        result = result.replace("%a", "EEE");    // Abbreviated weekday name
        result = result.replace("%A", "EEEE");   // Full weekday name
        result = result.replace("%b", "MMM");    // Abbreviated month name
        result = result.replace("%B", "MMMM");   // Full month name
        result = result.replace("%Z", "z");      // Time zone

        return result;
    }

    // ========== curdate, curtime, current_time: MySQL current-time functions ==========

    @Description("MySQL curdate() — current date in session timezone")
    @ScalarFunction("curdate")
    @SqlType(StandardTypes.DATE)
    public static long curdate(ConnectorSession session)
    {
        // Return current date in session timezone as days since epoch
        return LocalDate.ofInstant(
                Instant.ofEpochMilli(session.getStart().toEpochMilli()),
                session.getTimeZoneKey().getZoneId()
        ).toEpochDay();
    }

    @Description("MySQL curtime() — current time in session timezone")
    @ScalarFunction("curtime")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice curtime(ConnectorSession session)
    {
        // Return current time as HH:MM:SS string
        LocalTime time = LocalTime.ofInstant(
                Instant.ofEpochMilli(session.getStart().toEpochMilli()),
                session.getTimeZoneKey().getZoneId()
        );
        return Slices.utf8Slice(String.format("%02d:%02d:%02d",
                time.getHour(), time.getMinute(), time.getSecond()));
    }

    @Description("MySQL current_time() — alias for curtime()")
    @ScalarFunction("current_time")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice currentTime(ConnectorSession session)
    {
        return curtime(session);
    }

    // ========== utc_date, utc_time, utc_timestamp: MySQL UTC functions ==========

    @Description("MySQL utc_date() — current date in UTC")
    @ScalarFunction("utc_date")
    @SqlType(StandardTypes.DATE)
    public static long utcDate()
    {
        return LocalDate.now(ZoneOffset.UTC).toEpochDay();
    }

    @Description("MySQL utc_time() — current time in UTC")
    @ScalarFunction("utc_time")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice utcTime()
    {
        LocalTime time = LocalTime.now(ZoneOffset.UTC);
        return Slices.utf8Slice(String.format("%02d:%02d:%02d",
                time.getHour(), time.getMinute(), time.getSecond()));
    }

    @Description("MySQL utc_timestamp() — current timestamp in UTC")
    @ScalarFunction("utc_timestamp")
    @SqlType("timestamp(6)")
    public static long utcTimestamp()
    {
        // Return microseconds since epoch
        return Instant.now().toEpochMilli() * 1000L;
    }

    // ========== date_sub: subtract days from date ==========

    @Description("MySQL date_sub(date, N) — subtract N days")
    @ScalarFunction("date_sub")
    @SqlType(StandardTypes.DATE)
    public static long dateSub(@SqlType(StandardTypes.DATE) long date, @SqlType(StandardTypes.BIGINT) long days)
    {
        return date - days;
    }

    @Description("MySQL date_sub(date_str, N) — subtract N days from string date")
    @ScalarFunction("date_sub")
    @SqlType(StandardTypes.DATE)
    public static long dateSub(@SqlType(StandardTypes.VARCHAR) Slice dateStr, @SqlType(StandardTypes.BIGINT) long days)
    {
        // Parse date string to epoch day, then subtract
        String str = dateStr.toStringUtf8().trim();
        try {
            LocalDate ld = LocalDate.parse(str.substring(0, Math.min(10, str.length())));
            return ld.toEpochDay() - days;
        }
        catch (Exception e) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Cannot parse date: " + str);
        }
    }

    // ========== yearweek: return YYYYww format ==========

    @Description("MySQL yearweek(date) — return year and week as YYYYww")
    @ScalarFunction("yearweek")
    @SqlType(StandardTypes.INTEGER)
    public static long yearweek(@SqlType(StandardTypes.DATE) long date)
    {
        long millis = DAYS.toMillis(date);
        long year = UTC_CHRONOLOGY.year().get(millis);
        long week = UTC_CHRONOLOGY.weekOfWeekyear().get(millis);
        return year * 100 + week;
    }

    @Description("MySQL yearweek(timestamp) — return year and week as YYYYww")
    @ScalarFunction("yearweek")
    @LiteralParameters("p")
    @SqlType(StandardTypes.INTEGER)
    public static long yearweekFromTimestamp(@SqlType("timestamp(p)") long timestamp)
    {
        long millis = scaleEpochMicrosToMillis(timestamp);
        long year = UTC_CHRONOLOGY.year().get(millis);
        long week = UTC_CHRONOLOGY.weekOfWeekyear().get(millis);
        return year * 100 + week;
    }

    @Description("MySQL yearweek(date_str) — return year and week as YYYYww from string")
    @ScalarFunction("yearweek")
    @SqlType(StandardTypes.INTEGER)
    public static long yearweekFromVarchar(@SqlType(StandardTypes.VARCHAR) Slice dateStr)
    {
        long timestamp = parseTimestampMicros(dateStr);
        return yearweekFromTimestamp(timestamp);
    }

    // ========== to_seconds: seconds since year 0 ==========

    @Description("MySQL to_seconds(date) — seconds since year 0")
    @ScalarFunction("to_seconds")
    @SqlType(StandardTypes.BIGINT)
    public static long toSeconds(@SqlType(StandardTypes.DATE) long date)
    {
        // MySQL to_seconds counts from year 0 (proleptic Gregorian).
        // Unix epoch is 1970-01-01, which is 719528 days from year 0 in proleptic Gregorian.
        // So: to_seconds = (days_since_year0) * 86400
        //               = (date + 719528) * 86400
        return (date + 719528L) * 86400L;
    }

    @Description("MySQL to_seconds(timestamp) — seconds since year 0")
    @ScalarFunction("to_seconds")
    @LiteralParameters("p")
    @SqlType(StandardTypes.BIGINT)
    public static long toSecondsFromTimestamp(@SqlType("timestamp(p)") long timestamp)
    {
        // Timestamp is in microseconds; convert to date first
        long days = timestamp / (1_000_000L * 86400L);
        return toSeconds(days);
    }

    @Description("MySQL to_seconds(date_str) — seconds since year 0 from string")
    @ScalarFunction("to_seconds")
    @SqlType(StandardTypes.BIGINT)
    public static long toSecondsFromVarchar(@SqlType(StandardTypes.VARCHAR) Slice dateStr)
    {
        String str = dateStr.toStringUtf8().trim();
        try {
            LocalDate ld = LocalDate.parse(str.substring(0, Math.min(10, str.length())));
            return toSeconds(ld.toEpochDay());
        }
        catch (Exception e) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Cannot parse date: " + str);
        }
    }

    // ========== datetime: parse string to timestamp ==========

    @Description("MySQL datetime(str) — parse datetime string")
    @ScalarFunction("datetime")
    @SqlType("timestamp(6)")
    public static long datetime(@SqlType(StandardTypes.VARCHAR) Slice dateStr)
    {
        return parseTimestampMicros(dateStr);
    }
}
