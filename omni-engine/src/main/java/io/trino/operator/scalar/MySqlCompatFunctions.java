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

import io.trino.spi.function.Description;
import io.trino.spi.function.LiteralParameters;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.StandardTypes;
import org.joda.time.chrono.ISOChronology;

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
}
