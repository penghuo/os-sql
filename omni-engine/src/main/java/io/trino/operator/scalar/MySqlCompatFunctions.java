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

import static io.trino.type.DateTimes.scaleEpochMicrosToMillis;
import static java.util.concurrent.TimeUnit.DAYS;

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
}
